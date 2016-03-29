import {Component, OnInit} from 'angular2/core';
import {HTTP_PROVIDERS}    from 'angular2/http';

import {ElasticsearchService} from './elasticsearch.service'

@Component({
    selector: 'app',
    templateUrl: 'app/app.component.html',
    providers: [
        HTTP_PROVIDERS,
        ElasticsearchService
    ]
})
export class AppComponent implements OnInit {
    health = 'Loading';
    errorMessage: string;

    constructor(private _es: ElasticsearchService) { }

    ngOnInit() {
        this._es.getHealthString()
            .subscribe(
            health => this.health = health,
            error => this.errorMessage = <any>error);

        let query = {
            "query": {
                "match_all": {}
            }
        }

        this._es.search(query, 'nfl', '2013')
            .subscribe();
    }
}
