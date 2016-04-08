import {Injectable} from 'angular2/core';
import {Http, Response, Headers, RequestOptions} from 'angular2/http';
import {Observable} from 'rxjs/Observable';

import {ElasticsearchResponse} from './elasticsearch-response';
import {Transaction} from './transaction';
import {History} from './history';
import {TimeResolution} from './time-resolution'

@Injectable()
export class ElasticsearchService {
    private _esUrl = 'http://83.212.100.48:9200';
    private _index = 'forex';

    constructor(private http: Http) { }

    getHealthString() {
        let healthUrl = '/_cat/health';

        return this.http.get(this._esUrl + healthUrl)
            .map(res => <string>res.text())
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    getDocument(id: number, index: string, type: string) {
        let documentUrl = '/' + index + '/' + type + '/' + id;

        return this.http.get(this._esUrl + documentUrl)
            .map(res => this.processResponse(res.json()))
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    search(query: Object, index: string, type: string) {
        let searchUrl = '/' + index + '/' + type + '/_search'
        let body = JSON.stringify(query);

        return this.http.post(this._esUrl + searchUrl, body)
            .map(res => this.processResponse(res.json()))
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    getHistory(
        currencyPair: string,
        startDate: string, endDate: string,
        resolution: TimeResolution) {
        let type = 'history';
        let query = {
            "query": {
                "match": {
                    "currency_pair": currencyPair
                }
            },
            "filter": {
                "range": {
                    "tick_date": {
                        "gte": startDate,
                        "lte": endDate,
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        }

        if (TimeResolution.T !== resolution) {
            query["aggs"] = {
                "resolution": {
                    "date_histogram": {
                        "field": "tick_date",
                        "interval": "15m"
                    },
                    "aggs": {
                        "avg_bid": {
                            "avg": {
                                "field": "bid_price"
                            }
                        },
                        "avg_ask": {
                            "avg": {
                                "field": "ask_price"
                            }
                        },
                        "median_bid": {
                            "percentiles": {
                                "field": "bid_price",
                                "percents": [50]
                            }
                        },
                        "median_ask": {
                            "percentiles": {
                                "field": "bid_ask",
                                "percents": [50]
                            }
                        }
                    }
                }
            }
        }

        console.log(query);

        return this.search(query, this._index, 'history');
    }

    private processResponse(res: ElasticsearchResponse) {
        let result = [];
        console.log(res);
        for (let i = 0; i < res.hits.hits.length; i++) {
            result.push(res.hits.hits[i]._source);
        }

        return result;
    }

    private handleError(error: Response) {
        console.error(error);
        return Observable.throw(error.json().error || 'Server Error');
    }
}
