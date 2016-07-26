from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.mllib.linalg import Vectors
import numpy as np
import json
from numpy.linalg import svd
from elasticsearch_interface \
    import get_es_rdd, save_es_rdd, get_currency_pair_dict
from utils import parse_range, parse_dates, modify_record

def create_proximity_matrix(rdd, w=[
        [1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0]]):
    keys = []
    feature_matrix = []
    features = rdd.map(lambda (key, body): (key, [
        w[0][0]*w[0][1]*body['pc_trade_count'],
        w[0][0]*w[0][2]*body['pc_trade_duration'],
        w[0][0]*w[0][3]*body['pc_net_pnl'],
        w[1][0]*w[1][1]*body['p_trade_count'],
        w[1][0]*w[1][2]*body['p_trade_duration'],
        w[1][0]*w[1][3]*body['p_net_pnl'],
        w[2][0]*w[2][1]*body['c_trade_count'],
        w[2][0]*w[2][2]*body['c_trade_duration'],
        w[2][0]*w[2][3]*body['c_net_pnl'],
        w[3][0]*w[3][1]*body['l_trade_count'],
        w[3][0]*w[3][2]*body['l_trade_duration'],
        w[3][0]*w[3][3]*body['l_net_pnl']])).collect()
    for line in features:
        keys.append(line[0])
        feature_matrix.append(line[1])
    return keys, np.cov(np.array(feature_matrix))

def mds(p):
    n = len(p)
    P = np.square(p)
    J = np.eye(n) - (1.0 / n) * np.ones((n, n))
    B = (-1/2) * J.dot(P).dot(J)
    U, s, _ = svd(B)
    return np.dot(U.T[0:2].T, np.diag(s[0:2]))

def zip_keys(sc, mds, keys):
    mds = sc.parallelize(mds.tolist()).zipWithIndex().map(lambda x: (x[1], x[0]))
    keys = sc.parallelize(keys).zipWithIndex().map(lambda x: (x[1], x[0]))
    return keys.join(mds) \
        .map(lambda x: x[1])

start_date = '2015-05-01'
end_date = '2015-05-07'

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute MDS')
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

    rdd, _ = get_es_rdd(sc, query=query, index='forex/feature') \
            .randomSplit([1.0, 9.0], seed=0L)

    ratings = get_es_rdd(sc, query=query, index='forex/rating')

    weights = ratings.map(lambda (_, body): [
            [body['weights']['trade_ratings'],
            body['weights']['pc_trade_count'],
            body['weights']['pc_trade_duration'],
            body['weights']['pc_net_pnl']],
            [body['weights']['provider_ratings'],
            body['weights']['p_trade_count'],
            body['weights']['p_trade_duration'],
            body['weights']['p_net_pnl']],
            [body['weights']['currency_ratings'],
            body['weights']['c_trade_count'],
            body['weights']['c_trade_duration'],
            body['weights']['c_net_pnl']],
            [body['weights']['country_ratings'],
            body['weights']['l_trade_count'],
            body['weights']['l_trade_duration'],
            body['weights']['l_net_pnl']]]).top(1)[0]

    k, p = create_proximity_matrix(rdd)
    m = mds(p)
    points = zip_keys(sc, m, k)

    k, p = create_proximity_matrix(rdd, weights)
    wm = mds(p)
    weighted_points = zip_keys(sc, wm, k)

    pp = sc.parallelize(points.join(weighted_points).collect())
    pr = pp.join(ratings) \
        .map(lambda (key, ((p, wp), body)):
            (key, {
                'point_id': key,
                'provider_id': body['provider_id'],
                'currency_pair': body['currency_pair'],
                'transaction_type': body['transaction_type'],
                'country': body['country'],
                'start_date': body['start_date'],
                'end_date': body['end_date'],
                'rating': body['rating'],
                'x': p[0],
                'y': p[1],
                'wx': wp[0],
                'wy': wp[1]
            }))

    save_es_rdd(pr, 'forex/point', key='point_id')
