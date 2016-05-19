import {Component, OnInit} from '@angular/core';
import {HTTP_PROVIDERS}    from '@angular/http';

import {ElasticsearchService} from './elasticsearch.service'
import {TimeResolution} from './time-resolution'
import {LineChartComponent} from './line-chart.component';
import {StockChartComponent} from './stock-chart.component';
import {TreeMapComponent} from './tree-map.component';
import {DatePickerComponent} from './date-picker.component';

@Component({
    selector: 'dashboard',
    templateUrl: 'app/dashboard.component.html',
    styles: [`
        chart {
            display: block;
        }
    `],
    providers: [
        HTTP_PROVIDERS,
        ElasticsearchService,
    ],
    directives: [
        LineChartComponent,
        StockChartComponent,
        TreeMapComponent,
        DatePickerComponent
    ]
})
export class DashboardComponent implements OnInit {
    errorMessage: string;
    res: Object;
    title = 'EUR/USD';
    data: Object[];


    constructor(private _es: ElasticsearchService) { }

    ngOnInit() {
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
