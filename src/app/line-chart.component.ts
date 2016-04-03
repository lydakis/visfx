import {Component} from 'angular2/core';
import {CHART_DIRECTIVES} from 'angular2-highcharts';

@Component({
    selector: 'line-chart',
    directives: [CHART_DIRECTIVES],
    template: `
        <chart [options]="options"
             (load)="saveInstance($event.context)">
        </chart>
    `
})
export class LineChartComponent {
    constructor() {
        this.options = {
          chart: { type: 'spline' },
          title: { text : 'EUR/USD'},
          series: [{ data: [2,3,5,8,13] }]
        };
        setInterval(() => this.chart.series[0].addPoint(Math.random() * 10), 1000);
    }

    chart: HighchartsChartObject;
    options: HighchartsOptions;
    saveInstance(chartInstance) {
        this.chart = chartInstance;
    }
}
