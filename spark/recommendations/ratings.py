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

def count_pairs(provider, rdd):
    pair_counts = rdd \
        .filter(lambda item: provider == item[1]['provider_id']) \
        .map(lambda item: item[1]['currency_pair']) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
        .map(lambda item: ('key', {
            'provider_id': provider,
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
        'rating_id': str(item[1]['provider_id']) + ':' + item[1]['currency_pair'],
        'provider_id': item[1]['provider_id'],
        'currency_pair': item[1]['currency_pair'],
        'count': item[1]['count'],
        'rating': 1+(9*(item[1]['count']-min_count)/(max_count-min_count))
            if max_count != min_count else 10
    }))

def get_ratings(rdd):
    providers = get_providers(rdd).collect()
    return [calc_rating(count_pairs(provider, rdd)) for provider in providers]

def save_ratings(ratings, index, key=None):
    for rating in ratings:
        save_es_rdd(rating, index, key)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Currency Recommendations')
    sc = SparkContext(conf=conf)

    es_rdd = get_es_index('forex/transaction')
    ratings = get_ratings(es_rdd)
    save_ratings(ratings, 'forex/rating', 'rating_id')
