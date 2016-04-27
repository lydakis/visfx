import {Component, OnInit} from 'angular2/core';
import {HTTP_PROVIDERS, JSONP_PROVIDERS}    from 'angular2/http';

import {ElasticsearchService} from './elasticsearch.service'
import {TimeResolution} from './time-resolution'
import {LineChartComponent} from './line-chart.component';
import {StockChartComponent} from './stock-chart.component';

@Component({
    selector: 'app',
    templateUrl: 'app/app.component.html',
    styles: [`
        chart {
            display: block;
        }
    `],
    providers: [
        HTTP_PROVIDERS,
        JSONP_PROVIDERS,
        ElasticsearchService,
    ],
    directives: [
        LineChartComponent,
        StockChartComponent
    ]
})
export class AppComponent implements OnInit {
    health = 'Loading';
    errorMessage: string;
    res: Object;
    title = 'EUR/USD';
    data: Object[];

    constructor(private _es: ElasticsearchService) { }

    ngOnInit() {
        this._es.getHealthString()
            .subscribe(
            health => this.health = health,
            error => this.errorMessage = <any>error);

        this._es.getHistory(
            'EUR/USD',
            '2015-01-01', '2015-12-31',
            TimeResolution.W1)
            .subscribe(
                res => this.data = res,
                error => this.errorMessage = <any>error
            );
    }
}
