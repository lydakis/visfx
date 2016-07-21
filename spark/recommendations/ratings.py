from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.mllib.linalg import Vectors
import numpy as np
import json
from numpy.linalg import svd
from elasticsearch_interface \
    import get_es_rdd, save_es_rdd, get_currency_pair_dict
from utils import parse_range, parse_dates, modify_record

def get_keys(rdd):
    return rdd.sortByKey().map(lambda item: (item[0]))

def generate_weights():
    return {
        'pair_counts': 1.0,
        'total_amount': 1.0,
        'total_pair_pnl': 1.0,
        'pnl_per_amount': 1.0,
        'provider_pnl': 1.0,
        'provider_amount': 1.0,
        'provider_pnl_per_amount': 1.0,
        'total_pair_count': 1.0,
        'pair_amount': 1.0,
        'pair_pnl': 1.0,
        'pair_pnl_per_amount': 1.0
    }

def apply_weights(features, weights):
    for feature, _ in features.iteritems():
        features[feature] \
            .foreach(lambda item: (item[0], weights[feature] * item[1]))
    return features

def generate_feature_matrix(features):
    return [feature_value.sortByKey().map(lambda item: item[1]).collect()
         for _, feature_value in features.iteritems()], \
            [feature for feature, _ in features.iteritems()]

def format_pca(ratings, keys):
    ratings = ratings.zipWithIndex().map(lambda x: (x[1], x[0]))
    keys = keys.zipWithIndex().map(lambda x: (x[1], x[0]))
    return keys.join(ratings) \
        .map(lambda x: x[1])

def pca(sc, features):
    feature_matrix, feature_names = generate_feature_matrix(features)
    m = len(feature_matrix[0])
    sigma = (1.0 / m) * np.dot(feature_matrix, np.transpose(feature_matrix))
    U, s, _ = svd(sigma)
    ratings = sc.parallelize(
        np.dot(U.T[0], feature_matrix).tolist())
    coordinates = np.dot(U.T[0:2], feature_matrix).tolist()
    keys = get_keys(features.values()[0])
    return format_pca(ratings, keys), zip(coordinates[0], coordinates[1]), \
        zip(feature_names, (U.T[0]).tolist()), compute_variance(s)

def compute_variance(s):
    return float(s[0]) / s.sum()

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

def format_ratings(rdd, ratings, weights, variance):
    max_rating = ratings.map(lambda (_, value): value).max()
    min_rating = ratings.map(lambda (_, value): value).min()
    return ratings.join(rdd).map(lambda (key, (rating, body)): (key, {
        'rating_id': key,
        'provider_id': body['provider_id'],
        'currency_pair': body['currency_pair'],
        'transaction_type': body['transaction_type'],
        'country': body['country'],
        'start_date': body['start_date'],
        'end_date': body['end_date'],
        'weights': {
            weights['a'][0][0]: weights['a'][0][1],
            weights['a'][1][0]: weights['a'][1][1],
            weights['a'][2][0]: weights['a'][2][1],
            weights['a'][3][0]: weights['a'][3][1],
            weights['pc'][0][0]: weights['pc'][0][1],
            weights['pc'][1][0]: weights['pc'][1][1],
            weights['pc'][2][0]: weights['pc'][2][1],
            weights['p'][0][0]: weights['p'][0][1],
            weights['p'][1][0]: weights['p'][1][1],
            weights['p'][2][0]: weights['p'][2][1],
            weights['c'][0][0]: weights['c'][0][1],
            weights['c'][1][0]: weights['c'][1][1],
            weights['c'][2][0]: weights['c'][2][1],
            weights['l'][0][0]: weights['l'][0][1],
            weights['l'][1][0]: weights['l'][1][1],
            weights['l'][2][0]: weights['l'][2][1],
        },
        'rating': (1 +
            (4 * float(rating - min_rating) / (max_rating - min_rating)))
                if max_rating != min_rating else 2.5
    }))

def get_ratings(sc, rdd, rating_function):
    features = extract_features(rdd)
    trade_ratings, trade_coordinates, trade_weights, trade_variance = \
        rating_function(sc, features['pc'])
    provider_ratings, provider_coordinates, provider_weights, provider_variance = \
        rating_function(sc, features['p'])
    currency_ratings, currency_coordinates, currency_weights, currency_variance = \
        rating_function(sc, features['c'])
    country_ratings, country_coordinates, country_weights, country_variance = \
        rating_function(sc, features['l'])
    overall_ratings, overall_coordinates, overall_weights, \
        overall_variance = rating_function(sc, {
            'trade_ratings': trade_ratings,
            'provider_ratings': provider_ratings,
            'currency_ratings': currency_ratings,
            'country_ratings': country_ratings})
    weights = {
        'pc': trade_weights,
        'p': provider_weights,
        'c': currency_weights,
        'l': country_weights,
        'a': overall_weights
    }
    variance = {
        'pc': trade_variance,
        'p': provider_variance,
        'c': currency_variance,
        'l': country_variance,
        'a': overall_variance
    }
    coordinates = {
        'pc': trade_coordinates,
        'p': provider_coordinates,
        'c': currency_coordinates,
        'l': country_coordinates,
        'a': overall_coordinates
    }
    return format_ratings(rdd, overall_ratings, weights, variance)

def rating_meta_data(coordinates, weights, variance):
    return sc.parallelize((make_key(('', '', '', '', start_date, end_date)), {
        'start_date': start_date,
        'end_date': end_date,
        'coordinates': {
            'overall': coordinates['a'],
            'trade': coordinates['pc'],
            'provider': coordinates['p'],
            'currency_pair': coordinates['c'],
            'country': coordinates['l']
        },
        'variance': {
            'overall': variance['a'],
            'trade': variance['pc'],
            'provider': variance['p'],
            'currency_pair': variance['c'],
            'country': variance['l']
        }
    }))

def extract_features(rdd):
    pc_trade_count = rdd.map(lambda (key, body): (key, body['pc_trade_count']))
    pc_trade_duration = rdd.map(lambda (key, body): (key, body['pc_trade_duration']))
    pc_net_pnl = rdd.map(lambda (key, body): (key, body['pc_net_pnl']))
    p_trade_count = rdd.map(lambda (key, body): (key, body['p_trade_count']))
    p_trade_duration = rdd.map(lambda (key, body): (key, body['p_trade_duration']))
    p_net_pnl = rdd.map(lambda (key, body): (key, body['p_net_pnl']))
    c_trade_count = rdd.map(lambda (key, body): (key, body['c_trade_count']))
    c_trade_duration = rdd.map(lambda (key, body): (key, body['c_trade_duration']))
    c_net_pnl = rdd.map(lambda (key, body): (key, body['c_net_pnl']))
    l_trade_count = rdd.map(lambda (key, body): (key, body['l_trade_count']))
    l_trade_duration = rdd.map(lambda (key, body): (key, body['l_trade_duration']))
    l_net_pnl = rdd.map(lambda (key, body): (key, body['l_net_pnl']))
    return {
        'pc': {
            'pc_trade_count': pc_trade_count,
            'pc_trade_duration': pc_trade_duration,
            'pc_net_pnl': pc_net_pnl,
        },
        'p': {
            'p_trade_count': p_trade_count,
            'p_trade_duration': p_trade_duration,
            'p_net_pnl': p_net_pnl,
        },
        'c': {
            'c_trade_count': c_trade_count,
            'c_trade_duration': c_trade_duration,
            'c_net_pnl': c_net_pnl,
        },
        'l': {
            'l_trade_count': l_trade_count,
            'l_trade_duration': l_trade_duration,
            'l_net_pnl': l_net_pnl,
        }
    }

start_date = '2015-05-01'
end_date = '2015-05-07'

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    query = json.dumps({
        'query': {
            'bool': {
                'must': [
                    { 'term': { 'start_date': start_date }},
                    { 'term': { 'end_date': end_date }}
                ]
            }
        }
    })

    rdd = get_es_rdd(sc, query=query, index='forex/feature') \
            .persist(StorageLevel.MEMORY_AND_DISK)
    ratings = get_ratings(sc, rdd, pca)
    save_es_rdd(ratings, 'forex/rating', key='rating_id', upsert=False)
