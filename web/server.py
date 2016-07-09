from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
from elasticsearch import Elasticsearch
import json

app = Flask(__name__)
CORS(app)

ES_HOST = {
    'host': 'search',
    'port': 9200
}

es = Elasticsearch([ES_HOST])

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
    res = es.search(
        index='forex',
        body=query,
        filter_path=['hits.hits._source'])
    return jsonify(res)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
