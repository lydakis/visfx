from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
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
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    query = format_query(start_date, end_date)
    try:
        docs = scan(es,
            query=query,
            index='forex',
            doc_type='transaction')
        res = [drop_keys(
            doc['_source'], ['language', '@timestamp', 'type', '@version'])
                for doc in docs]
    except:
        res = {} 
    return jsonify(res)

def format_query(start_date, end_date):
    return json.dumps({
        'query': {
            'match_all': {}
        },
        'filter': {
            'range': {
                'date_closed': {
                    'gte': start_date,
                    'lte': end_date,
                    'format': 'yyyy-MM-dd'
                }
            }
        }
    })

def drop_keys(dictionary, keys):
    for key in keys:
        if key in dictionary:
            del dictionary[key]
    return dictionary

if __name__ == '__main__':
    app.run(host='0.0.0.0')
