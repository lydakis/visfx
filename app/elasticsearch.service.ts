import {Injectable} from 'angular2/core';
import {Http, Response} from 'angular2/http';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class ElasticsearchService {
    private _esUrl = 'http://localhost:9200';

    constructor(private http: Http) { }

    getHealthString() {
        let healthUrl = '/_cat/health';
        return this.http.get(this._esUrl + healthUrl)
            .map(res => <string>res.text())
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    private handleError(error: Response) {
        console.error(error);
        return Observable.throw(error.json().error || 'Server Error');
    }
}