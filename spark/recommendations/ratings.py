from pyspark import SparkContext, SparkConf, StorageLevel
import numpy as np
import math
from numpy.linalg import svd
from elasticsearch_interface \
    import get_es_rdd, save_es_rdd, get_currency_pair_dict
from utils import parse_range, parse_dates, modify_record

def count_pairs(rdd):
    return rdd \
        .map(lambda item:
            ((item[1]['provider_id'], item[1]['currency_pair']), 1)) \
        .reduceByKey(lambda a, b: a + b)

def net_amount(rdd):
    return rdd \
        .map(lambda item: ((item[1]['provider_id'], item[1]['currency_pair']),
            item[1]['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def net_pnl(rdd):
    return rdd \
        .map(lambda item: ((item[1]['provider_id'], item[1]['currency_pair']),
            item[1]['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def pnl_per_amount(pnl_rdd, amount_rdd):
    per_amount = amount_rdd.map(lambda item: (item[0], 1.0 / item[1]))
    union = pnl_rdd.union(per_amount)
    return union.reduceByKey(lambda a, b: a * b)

def net_provider_pnl(rdd):
    return rdd \
        .map(lambda item: (item[1]['provider_id'], item[1]['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def net_provider_amount(rdd):
    return rdd \
        .map(lambda item: (item[1]['provider_id'], item[1]['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def total_pair_count(rdd):
    return rdd \
        .map(lambda item: (item[1]['currency_pair'], 1)) \
        .reduceByKey(lambda a, b: a + b)

def net_pair_amount(rdd):
    return rdd \
        .map(lambda item: (item[1]['currency_pair'], item[1]['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def net_pair_pnl(rdd):
    return rdd \
        .map(lambda item: (item[1]['currency_pair'], item[1]['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def normalize_feature(rdd):
    max_value = rdd.map(lambda item: item[1]).max()
    count = rdd.count()
    average = rdd \
        .map(lambda item: item[1]) \
        .reduce(lambda a, b: a + b) / float(count)
    return rdd.map(lambda item: (item[0], float(item[1] - average) / max_value))

def generate_features(rdd):
    count_rdd = count_pairs(rdd)
    amount_rdd = net_amount(rdd)
    pnl_rdd = net_pnl(rdd)
    keys = get_keys(pnl_rdd).cache()
    pnl_per_amount_rdd = pnl_per_amount(pnl_rdd, amount_rdd)
    provider_pnl_rdd = canonicalize_keys(
        net_provider_pnl(rdd), keys, provider=True)
    provider_amount_rdd = canonicalize_keys(
        net_provider_amount(rdd), keys, provider=True)
    provider_pnl_per_amount_rdd = \
        pnl_per_amount(provider_pnl_rdd, provider_amount_rdd)
    total_pair_count_rdd = canonicalize_keys(
        total_pair_count(rdd), keys, currency_pair=True)
    total_pair_amount_rdd = canonicalize_keys(
        net_pair_amount(rdd), keys, currency_pair=True)
    total_pair_pnl_rdd = canonicalize_keys(
        net_pair_pnl(rdd), keys, currency_pair=True)
    total_pair_pnl_per_amount_rdd = \
        pnl_per_amount(total_pair_pnl_rdd, total_pair_amount_rdd)
    return {
        'pair_counts': normalize_feature(count_rdd),
        'net_amount': normalize_feature(amount_rdd),
        'net_pair_pnl': normalize_feature(pnl_rdd),
        'pnl_per_amount': normalize_feature(pnl_per_amount_rdd),
        'provider_pnl': normalize_feature(provider_pnl_rdd),
        'provider_amount': normalize_feature(provider_amount_rdd),
        'provider_pnl_per_amount': normalize_feature(
            provider_pnl_per_amount_rdd),
        'net_pair_count': normalize_feature(total_pair_count_rdd),
        'pair_amount': normalize_feature(total_pair_amount_rdd),
        'pair_pnl': normalize_feature(total_pair_pnl_rdd),
        'pair_pnl_per_amount': normalize_feature(total_pair_pnl_per_amount_rdd)
    }

def get_keys(rdd):
    return rdd.sortByKey().map(lambda item: (item[0]))

def canonicalize_keys(feature, keys, provider=False, currency_pair=False):
    if provider:
        return feature.join(keys).map(lambda x: ((x[0], x[1][1]), x[1][0]))
    if currency_pair:
        return feature \
            .join(keys.map(lambda x: (x[1], x[0]))) \
            .map(lambda x: ((x[1][1], x[0]), x[1][0]))

def generate_weights(features):
    feature_matrix = generate_feature_matrix(features)
    m = len(feature_matrix[0])
    sigma = (1.0 / m) * np.dot(feature_matrix, np.transpose(feature_matrix))
    U, _, _ = svd(sigma)
    U = U[0].tolist()
    U = map(lambda x: x - min(U) + 1, U)
    return {
        'pair_counts': U[2],
        'net_amount': U[9],
        'net_pair_pnl': U[10],
        'pnl_per_amount': U[5],
        'provider_pnl': U[6],
        'provider_amount': U[3],
        'provider_pnl_per_amount': U[0],
        'net_pair_count': U[1],
        'pair_amount': U[4],
        'pair_pnl': U[7],
        'pair_pnl_per_amount': U[8]
    }

def apply_weights(features, weights):
    for feature, _ in features.iteritems():
        features[feature] \
            .foreach(lambda item: (item[0], weights[feature] * item[1]))
    return features

def generate_feature_matrix(features):
    return [feature_value.sortByKey().map(lambda item: item[1]).collect()
         for _, feature_value in features.iteritems()]

def format_pca(ratings, keys):
    ratings = ratings.zipWithIndex().map(lambda x: (x[1], x[0]))
    keys = keys.zipWithIndex().map(lambda x: (x[1], x[0]))
    return keys.join(ratings) \
        .map(lambda x: x[1]) \
        .map(lambda item: (item[0][0], {
            'provider_id': item[0][0],
            'currency_pair': item[0][1],
            'rating': item[1]
        }))

def pca(sc, features):
    feature_matrix = generate_feature_matrix(features)
    m = len(feature_matrix[0])
    sigma = (1.0 / m) * np.dot(feature_matrix, np.transpose(feature_matrix))
    U, _, _ = svd(sigma)
    ratings = sc.parallelize(
        np.dot(np.transpose(U[0]), feature_matrix).tolist())
    keys = get_keys(features['pair_counts'])
    return format_pca(ratings, keys).cache()

def linear_combination(sc, features):
    weights = generate_weights(features)
    weighted_features = apply_weights(features, weights)
    return sc \
        .union([feature_values
            for _,feature_values in features.iteritems()]) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda item: (item[0][0], {
            'provider_id': item[0][0],
            'currency_pair': item[0][1],
            'rating': item[1]
        })).cache()

def format_ratings(ratings, end_date, daterange):
    max_rating = ratings.map(lambda item: item[1]['rating']).cache().max()
    min_rating = ratings.map(lambda item: item[1]['rating']).min()
    start, end = parse_dates(end_date, daterange)
    return ratings.map(lambda item: (str(item[1]['provider_id']) + ':' +
        item[1]['currency_pair'] + ':' + end_date + ':' + daterange,
        modify_record(item[1], append={
            'rating_id': str(item[1]['provider_id']) + ':' +
                item[1]['currency_pair'] + ':' + end_date + ':' + daterange,
            'start_date': start,
            'end_date': end,
            'rating': 10 * float(item[1]['rating'] - min_rating)
                / (max_rating - min_rating) if max_rating != min_rating else 10
        })))

def format_ratings_no_normalization(ratings, end_date, daterange):
    start, end = parse_dates(end_date, daterange)
    return ratings.map(lambda item: (str(item[1]['provider_id']) + ':' +
        item[1]['currency_pair'] + ':' + end_date + ':' + daterange,
        modify_record(item[1], append={
            'rating_id': str(item[1]['provider_id']) + ':' +
                item[1]['currency_pair'] + ':' + end_date + ':' + daterange,
            'start_date': start,
            'end_date': end,
            'rating': item[1]['rating']
        })))

def get_ratings(sc, rdd, rating_function, end_date, daterange):
    features = generate_features(rdd)
    ratings = rating_function(sc, features)
    return format_ratings(ratings, end_date, daterange)

def save_ratings(ratings, index, key=None):
    save_es_rdd(ratings, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    start_date, end_date = parse_dates('2015-05-01', '7d')
    es_rdd = get_es_rdd(sc, index='forex/transaction', date_field='date_closed',
        start_date=start_date, end_date=end_date) \
            .persist(StorageLevel.MEMORY_AND_DISK)
    ratings = get_ratings(sc, es_rdd, pca, '2015-05-01', '7d')
    save_ratings(ratings, 'forex/rating', 'rating_id')
