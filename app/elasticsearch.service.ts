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

    getDocument(id: number, index: string, type: string) {
        let documentUrl = '/' + index + '/' + type + '/' + id;

        return this.http.get(this._esUrl + documentUrl)
            .map(res => <string>res.text())
            .do(data => console.log(data))
            .catch(this.handleError);
    }

    search(query: Object, index?: string, type?: string) {
        let searchUrl = '/_search'

        if (index && type) {
            searchUrl = '/' + type + searchUrl;
        }
        if(index) {
            searchUrl = '/' + index + searchUrl;
        }

        return this.http.get(this._esUrl + searchUrl, query)
                        .map(res => <any>res.json())
                        .do(data => console.log(data))
                        .catch(this.handleError);
    }

    private handleError(error: Response) {
        console.error(error);
        return Observable.throw(error.json().error || 'Server Error');
    }
}