from pyspark import SparkContext, SparkConf, StorageLevel
import numpy as np
from numpy.linalg import svd
from elasticsearch_interface \
    import get_es_rdd, save_es_rdd, get_currency_pair_dict
from utils import parse_range, parse_dates, modify_record

def count_pairs(rdd):
    return rdd \
        .map(lambda item:
            ((item[1]['provider_id'], item[1]['currency_pair']), 1)) \
        .reduceByKey(lambda a, b: a + b)

def total_amount(rdd):
    return rdd \
        .map(lambda item: ((item[1]['provider_id'], item[1]['currency_pair']),
            item[1]['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def total_pair_pnl(rdd):
    return rdd \
        .map(lambda item: ((item[1]['provider_id'], item[1]['currency_pair']),
            item[1]['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def total_provider_pnl(rdd):
    return rdd \
        .map(lambda item: (item[1]['provider_id'], item[1]['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def pnl_per_amount(pnl_rdd, amount_rdd):
    per_amount = amount_rdd.map(lambda item: (item[0], 1.0 / item[1]))
    union = pnl_rdd.union(per_amount)
    return union.reduceByKey(lambda a, b: a * b)

def normalize_feature(rdd):
    max_value = rdd.map(lambda item: item[1]).max()
    count = rdd.count()
    average = rdd \
        .map(lambda item: item[1]) \
        .reduce(lambda a, b: a + b) / float(count)
    return rdd.map(lambda item: (item[0], float(item[1] - average) / max_value))

def generate_features(rdd):
    count_rdd = count_pairs(rdd)
    pnl_rdd = total_pair_pnl(rdd)
    amount_rdd = total_amount(rdd)
    pnl_per_amount_rdd = pnl_per_amount(pnl_rdd, amount_rdd)
    provider_pnl_rdd = canonicalize_keys(
        total_provider_pnl(rdd), get_keys(pnl_rdd))
    return {
        'pair_counts': normalize_feature(count_rdd),
        'total_amount': normalize_feature(amount_rdd),
        'total_pair_pnl': normalize_feature(pnl_rdd),
        'pnl_per_amount': normalize_feature(pnl_per_amount_rdd),
        'total_provider_pnl': normalize_feature(provider_pnl_rdd)
    }

def get_keys(rdd):
    return rdd.sortByKey().map(lambda item: (item[0]))

def canonicalize_keys(feature, keys):
    return feature.join(keys).map(lambda x: ((x[0], x[1][1]), x[1][0]))

def generate_weights():
    return {
        'pair_counts': 1.0,
        'total_amount': 1.0,
        'total_pair_pnl': 1.0,
        'pnl_per_amount': 1.0,
        'total_provider_pnl': 1.0
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
    weights = generate_weights()
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

def calc_ratings(sc, features, method):
    return method(sc, features)

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

def get_ratings(sc, rdd, end_date, daterange):
    features = generate_features(rdd)
    ratings = calc_ratings(sc, features, method=pca)
    return format_ratings_no_normalization(ratings, end_date, daterange)

def save_ratings(ratings, index, key=None):
    save_es_rdd(ratings, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    start_date, end_date = parse_dates('2015-05-01', '1d')
    es_rdd = get_es_rdd(sc, index='forex/transaction', date_field='date_closed',
        start_date=start_date, end_date=end_date) \
            .persist(StorageLevel.MEMORY_AND_DISK)
    ratings = get_ratings(sc, es_rdd, '2015-05-01', '1d')
    save_ratings(ratings, 'forex/rating', 'rating_id')
