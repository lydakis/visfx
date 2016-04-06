import {Injectable} from 'angular2/core';
import {Http, Response, Headers, RequestOptions} from 'angular2/http';
import {Observable} from 'rxjs/Observable';

import {ElasticsearchResponse} from './elasticsearch-response';
import {Transaction} from './transaction';
import {History} from './history';

@Injectable()
export class ElasticsearchService {
    private _esUrl = 'http://83.212.100.48:9200';

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
            .map(res => <Object>res.json())
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    search(query: Object, index: string, type: string) {
        let searchUrl = '/' + index + '/' + type + '/_search?pretty'
        let body = JSON.stringify(query);

        return this.http.post(this._esUrl + searchUrl, body)
            .map(res => processResult<History>(res.json()))
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    private handleError(error: Response) {
        console.error(error);
        return Observable.throw(error.json().error || 'Server Error');
    }
}

function processResult<T>(res: ElasticsearchResponse) {
    let result: T[];

    for (let i = 0; i < res.hits.hits.length; i++) {
        result.push(<T>res.hits.hits[i]._source)
    }

    return result;
}
