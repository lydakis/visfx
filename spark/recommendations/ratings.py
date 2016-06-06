from pyspark import SparkContext, SparkConf
from elasticsearch_interface import es_read_conf, es_write_conf, \
    get_es_rdd, save_es_rdd

def get_providers(rdd):
    return rdd.map(lambda item: item[1]['provider_id']).distinct()

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

def append_record(item, record):
    item[1].update(record)
    return item

def filter_daterange(rdd, end_date, daterange):
    import datetime as dt
    end = dt.datetime.strptime(end_date, '%Y-%m-%d')
    start = end - parse_range(daterange)
    return rdd.filter(lambda item: dt.datetime.strptime(
            item[1]['date_closed'], '%Y-%m-%dT%H:%M:%S.%fZ') < end
            if 'date_closed' in item[1] else False) \
        .filter(lambda item: dt.datetime.strptime(
            item[1]['date_closed'], '%Y-%m-%dT%H:%M:%S.%fZ') > start
            if 'date_closed' in item[1] else False) \
        .map(lambda item: append_record(item, {
            'end_date': end_date,
            'range': daterange
        }))

def filter_provider(rdd, provider):
    return rdd.filter(lambda item: provider == item[1]['provider_id'])

def count_pairs(rdd, provider):
    doc = rdd.first()[1]
    end_date = doc['end_date']
    daterange = doc['range']
    pair_counts = rdd \
        .filter(lambda item: provider == item[1]['provider_id']) \
        .map(lambda item: item[1]['currency_pair']) \
        .map(lambda pair: (pair, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda item: (
            str(provider) + ':' + item[0] + ':' + end_date + ':' + daterange, {
                'provider_id': provider,
                'end_date': end_date,
                'range': daterange,
                'currency_pair': item[0],
                'count': item[1]
            }
        ))
    return pair_counts

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
    average = rdd\
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
            'total_amount' normalize_feature(amount_rdd),
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
            'pair_counts': 0.5,
            'total_amount' 0.5,
            'total_pair_pnl': 0.5,
            'pnl_per_amount': 0.5
        },
        'per_provider': {
            'total_provider_pnl': 0.5
        }
    }

def calc_rating(features, weights):
    pass

def calc_rating(rdd):
    max_count = rdd.takeOrdered(
        1, key = lambda item: -item[1]['count'])[0][1]['count']
    min_count = rdd.takeOrdered(
        1, key = lambda item: item[1]['count'])[0][1]['count']
    return rdd.map(lambda item: append_record(item, {
        'rating_id': item[0],
        'rating': 1+(9.0*(item[1]['count']-min_count)/(max_count-min_count))
            if max_count != min_count else 10
    }))

def get_ratings(sc, rdd, end_date, daterange):
    rdd = filter_daterange(rdd, end_date, daterange).cache()
    providers = sc.broadcast(get_providers(rdd).collect())
    return [calc_rating(count_pairs(rdd, provider))
        for provider in providers.value]

def save_ratings(ratings, index, key=None):
    for rating in ratings:
        save_es_rdd(rating, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    es_rdd = get_es_rdd(sc, 'forex/transaction')
    ratings = get_ratings(sc, es_rdd, '2015-05-01', '1y')
    save_ratings(ratings, 'forex/rating', 'rating_id')
