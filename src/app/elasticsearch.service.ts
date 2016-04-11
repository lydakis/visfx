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

    search(
        query: Object, index: string, type: string,
        processResponse: (res: ElasticsearchResponse) => Object[]) {
        let searchUrl = '/' + index + '/' + type + '/_search'
        let body = JSON.stringify(query);

        return this.http.post(this._esUrl + searchUrl, body)
            .map(res => processResponse(res.json()))
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    getHistory(
        currencyPair: string,
        startDate: string, endDate: string,
        resolution: string) {
        let type = 'history';
        let query = {
            "size": 10000,
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
            query["size"] = 1;
            query["aggs"] = {
                "resolution": {
                    "date_histogram": {
                        "field": "tick_date",
                        "interval": resolution
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

        return this.search(
            query, this._index, 'history', this.processHistoryResponseForCharts);
    }

    private processGeneralResponse(res: ElasticsearchResponse) {
        let result = [];

        for (let i = 0; i < res.hits.hits.length; i++) {
            result.push(res.hits.hits[i]._source);
        }

        return result;
    }

    private processHistoryResponseForCharts(res: ElasticsearchResponse) {
        let results = res.hits.hits;
        let aggs = res.aggregations.resolution.buckets;
        let series = [
            {
                name: (<History>results[0]._source).currency_pair + ' Ask',
                data: []
            },
            {
                name: (<History>results[0]._source).currency_pair + ' Bid',
                data: []
            }
        ];

        if (undefined === res.aggregations) {
            for (let i = 0; i < results.length; i++) {
                series[0].data[i] = (<History>results[i]._source).ask_price;
                series[1].data[i] = (<History>results[i]._source).bid_price;
            }
        }
        else {
            for (let i = 0; i < aggs.length; i++) {
                series[0].data[i] = aggs[i].avg_ask.value;
                series[1].data[i] = aggs[i].avg_bid.value;
            }
        }

        return series;
    }

    private handleError(error: Response) {
        console.error(error);
        return Observable.throw(error.json().error || 'Server Error');
    }
}
