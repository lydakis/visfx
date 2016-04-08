import {Component, OnInit} from 'angular2/core';
import {HTTP_PROVIDERS}    from 'angular2/http';

import {ElasticsearchService} from './elasticsearch.service'
import {TimeResolution} from './time-resolution'

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
    res: Object;

    constructor(private _es: ElasticsearchService) { }

    ngOnInit() {
        this._es.getHealthString()
            .subscribe(
            health => this.health = health,
            error => this.errorMessage = <any>error);

        // let query = {
        //     "query": {
        //         "match": {
        //             "currency_pair": "EUR/USD"
        //         }
        //     }
        // }
        //
        // this._es.search(query, 'forex', 'history')
            // .subscribe(
            //     res => this.res = res,
            //     error => this.errorMessage = <any>error
            // );

        this._es.getHistory(
            'EUR/USD',
            '2015-01-01', '2015-03-01',
            TimeResolution.M15)
            .subscribe(
                res => this.res = res,
                error => this.errorMessage = <any>error
            );
    }
}
