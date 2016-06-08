from pyspark import SparkContext, SparkConf, StorageLevel
from elasticsearch_interface import es_read_conf, es_write_conf, \
    get_es_rdd, save_es_rdd

def parse_range(daterange):
    import datetime as dt
    if 'd' in daterange:
        return dt.timedelta(days=int(daterange.replace('d', '')))
    elif 'w' in daterange:
        return dt.timedelta(weeks=int(daterange.replace('w', '')))
    elif 'm' in daterange:
        return dt.timedelta(weeks=4*int(daterange.replace('m', '')))
    elif 'y' in daterange:
        return dt.timedelta(weeks=48*int(daterange.replace('y', '')))
    else:
        raise

def parse_dates(end_date, daterange):
    import datetime as dt
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')
    start_date = end_date - parse_range(daterange)
    return start_date.isoformat(), end_date.isoformat()

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
    provider_pnl_rdd = total_provider_pnl(rdd)
    return {
        'per_currency': {
            'pair_counts': normalize_feature(count_rdd),
            'total_amount': normalize_feature(amount_rdd),
            'total_pair_pnl': normalize_feature(pnl_rdd),
            'pnl_per_amount': normalize_feature(pnl_per_amount_rdd)
        },
        'per_provider': {
            'total_provider_pnl': normalize_feature(provider_pnl_rdd)
        }
    }

def generate_weights():
    return {
        'per_currency': {
            'pair_counts': 1.0,
            'total_amount': 1.0,
            'total_pair_pnl': 1.0,
            'pnl_per_amount': 1.0
        },
        'per_provider': {
            'total_provider_pnl': 1.0
        }
    }

def apply_weights(features, weights):
    for feature_type, feature_dict in features.iteritems():
        for feature, _ in feature_dict.iteritems():
            features[feature_type][feature] = features[feature_type][feature] \
                .map(lambda item:
                    (item[0], weights[feature_type][feature] * item[1])).cache()
    return features

def calc_ratings(sc, features):
    per_currency = sc \
        .union([feature_value
            for _,feature_value in features['per_currency'].iteritems()]) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda item: (item[0][0], {
            'provider_id': item[0][0],
            'currency_pair': item[0][1],
            'rating': item[1]
        })).cache()
    per_provider = sc \
        .union([feature_value
            for _,feature_value in features['per_provider'].iteritems()]) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda item: (item[0], {
            'provider_id': item[0],
            'rating': item[1]
        })).cache()
    def add_ratings(r1, r2):
        return {
            'provider_id': r1['provider_id'],
            'currency_pair': r1['currency_pair'] \
                if 'currency_pair' in r1 else r2['currency_pair'],
            'rating': r1['rating'] + r2['rating']
        }
    return sc \
        .union([per_currency, per_provider]) \
        .reduceByKey(lambda a, b: add_ratings(a, b))

def format_ratings(ratings, end_date, daterange):
    def modify_record(record, append):
        record.update(append)
        return record
    max_rating = ratings.map(lambda item: item[1]['rating']).cache().max()
    min_rating = ratings.map(lambda item: item[1]['rating']).min()
    return ratings.map(lambda item: (str(item[1]['provider_id']) + ':' +
        item[1]['currency_pair'] + ':' + end_date + ':' + daterange,
        modify_record(item[1], append={
            'rating_id': str(item[1]['provider_id']) + ':' +
                item[1]['currency_pair'] + ':' + end_date + ':' + daterange,
            'end_date': end_date,
            'range': daterange,
            'rating': 10 * float(item[1]['rating'] - min_rating)
                / (max_rating - min_rating) if max_rating != min_rating else 10
        })))

def get_ratings(sc, rdd, end_date, daterange):
    features = generate_features(rdd)
    weights = generate_weights()
    weighted_features = apply_weights(features, weights)
    ratings = calc_ratings(sc, weighted_features).cache()
    return format_ratings(ratings, end_date, daterange)

def save_ratings(ratings, index, key=None):
    save_es_rdd(ratings, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    start_date, end_date = parse_dates('2015-05-01', '1y')
    es_rdd = get_es_rdd(sc, 'forex/transaction', start_date, end_date) \
        .persist(StorageLevel.MEMORY_AND_DISK)
    ratings = get_ratings(sc, es_rdd, '2015-05-01', '1y')
    save_ratings(ratings, 'forex/rating', 'rating_id')
