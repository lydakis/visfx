from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json

app = Flask(__name__)
CORS(app)

ES_HOST = {
    'host': 'search',
    'port': 9200
}

es = Elasticsearch([ES_HOST])

@app.route('/')
def health():
    return jsonify(es.cluster.health())

@app.route('/transactions')
def get_transactions():
    query = json.dumps({
        'query': {
            'match_all': {}
        },
        'filter': {
            'range': {
                'date_closed': {
                    'gte': '2015-05-01',
                    'lte': '2015-05-07',
                    'format': 'yyyy-MM-dd'
                }
            }
        }
    })
    docs = list(scan(es,
        query=query,
        index='forex',
        doc_type='transaction'))
    res = [drop_keys(
        doc['_source'], ['language', '@timestamp', 'type', '@version'])
            for doc in docs]
    return jsonify(res)

def drop_keys(dictionary, keys):
    for key in keys:
        if key in dictionary:
            del dictionary[key]
    return dictionary

if __name__ == '__main__':
    app.run(host='0.0.0.0')
