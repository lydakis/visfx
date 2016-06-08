def es_read_conf(index, date_field=None, start_date=None, end_date=None):
    conf = {
       'es.nodes': 'search',
       'es.port': '9200',
       'es.resource': index
    }
    if start_date != None and end_date != None:
        conf['es.query'] = '''{
            "query": {
                "match_all": {
                }
            },
            "filter": {
                "range": {
                    "''' + date_field + '''": {
                        "gte": "''' + start_date + '''",
                        "lte": "''' + end_date + '''",
                        "format": "yyyy-MM-dd'T'HH:mm:ss"
                    }
                }
            }
        }'''
    return conf

def es_write_conf(index, key=None):
    conf = {
        'es.nodes': 'search',
        'es.port': '9200',
        'es.resource': index
    }
    if key != None:
        conf['es.mapping.id'] = key
    return conf

def get_es_rdd(sc, index, date_field=None, start_date=None, end_date=None):
    return sc.newAPIHadoopRDD(
        inputFormatClass='org.elasticsearch.hadoop.mr.EsInputFormat',
        keyClass='org.apache.hadoop.io.NullWritable',
        valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
        conf=es_read_conf(index, date_field, start_date, end_date))

def save_es_rdd(rdd, index, key=None):
    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf(index, key))
