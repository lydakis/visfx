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
            item[1]['date_closed'], '%Y-%m-%dT%H:%M:%S.%fZ') < end) \
        .filter(lambda item: dt.datetime.strptime(
            item[1]['date_closed'], '%Y-%m-%dT%H:%M:%S.%fZ') > start) \
        .map(lambda item: append_record(item, {
            'end_date': end_date,
            'range': daterange
        }))

def count_pairs(rdd, provider):
    doc = rdd.first()[1]
    end_date = doc['end_date']
    daterange = doc['range']
    pair_counts = rdd \
        .filter(lambda item: provider == item[1]['provider_id']) \
        .map(lambda item: item[1]['currency_pair']) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
        .map(lambda item: (
            str(provider) + ':' + item[0] + ':' + end_date + ':' + daterange, {
                'provider_id': provider,
                'end_date': end_date,
                'range': daterange,
                'currency_pair': item[0],
                'count': item[1]
            }
        )).cache()
    return pair_counts

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

def get_ratings(rdd, end_date, daterange):
    rdd = filter_daterange(rdd, end_date, daterange)
    providers = get_providers(rdd).collect()
    return [calc_rating(count_pairs(rdd, provider)) for provider in providers]

def save_ratings(ratings, index, key=None):
    for rating in ratings:
        save_es_rdd(rating, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    es_rdd = get_es_rdd('forex/transaction')
    ratings = get_ratings(es_rdd, '2015-05-01', '1y')
    save_ratings(ratings, 'forex/rating', 'rating_id')
