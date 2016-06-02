from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFaxtorizationModel, Rating

def es_read_conf(index):
     return {
        'es.nodes': 'localhost',
        'es.port': '9200',
        'es.resource': index
    }

def es_write_conf(index, key=None):
    conf = {
        'es.nodes': 'localhost',
        'es.port': '9200',
        'es.resource': index
    }
    if key != None:
        conf['es.mapping.id'] = key
    return conf

def get_es_rdd(index):
    return sc.newAPIHadoopRDD(
        inputFormatClass='org.elasticsearch.hadoop.mr.EsInputFormat',
        keyClass='org.apache.hadoop.io.NullWritable',
        valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
        conf=es_read_conf(index))

def save_es_rdd(rdd, index, key=None):
    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf(index, key))

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

def filter_daterange(rdd, end_date, daterange):
    import datetime as dt
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')
    start_date = end_date - parse_range(daterange)
    return rdd.filter(lambda item: dt.datetime.strptime(
            item[1]['date_closed'], '%Y-%m-%dT%H:%M:%S.%fZ') < end_date) \
        .filter(lambda item: dt.datetime.strptime(
            item[1]['date_closed'], '%Y-%m-%dT%H:%M:%S.%fZ') > start_date)

def count_pairs(provider, rdd, end_date, daterange):
    rdd = filter_daterange(rdd, end_date, daterange)
    pair_counts = rdd \
        .filter(lambda item: provider == item[1]['provider_id']) \
        .map(lambda item: item[1]['currency_pair']) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
        .map(lambda item: ('key', {
            'provider_id': provider,
            'end_date': end_date,
            'range': daterange,
            'currency_pair': item[0],
            'count': item[1]
        }))
    return pair_counts

def calc_rating(rdd):
    max_count = rdd.takeOrdered(
        1, key = lambda item: -item[1]['count'])[0][1]['count']
    min_count = rdd.takeOrdered(
        1, key = lambda item: item[1]['count'])[0][1]['count']
    return rdd.map(lambda item: ('key', {
        'rating_id': str(item[1]['provider_id']) + ':' + \
            item[1]['currency_pair'] + ':' + \
            item[1]['end_date'] + ':' + item[1]['range'],
        'provider_id': item[1]['provider_id'],
        'end_date': item[1]['end_date'],
        'range': item[1]['range'],
        'currency_pair': item[1]['currency_pair'],
        'count': item[1]['count'],
        'rating': 1+(9*(item[1]['count']-min_count)/(max_count-min_count))
            if max_count != min_count else 10
    }))

def get_ratings(rdd, end_date, daterange):
    providers = get_providers(rdd).collect()
    return [calc_rating(count_pairs(
        provider, rdd, end_date, daterange)) for provider in providers]

def save_ratings(ratings, index, key=None):
    for rating in ratings:
        save_es_rdd(rating, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Currency Recommendations')
    sc = SparkContext(conf=conf)

    es_rdd = get_es_rdd('forex/transaction')
    ratings = get_ratings(es_rdd, '2015-05-01', '1y')
    save_ratings(ratings, 'forex/rating', 'rating_id')
