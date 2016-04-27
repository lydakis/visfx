import {Component, OnChanges, Input} from 'angular2/core';
import {CHART_DIRECTIVES, Highcharts} from 'angular2-highcharts';

@Component({
    selector: 'line-chart',
    directives: [CHART_DIRECTIVES],
    template: `
        <chart [options]="options"
             (load)="saveInstance($event.context)">
        </chart>
    `
})
export class LineChartComponent implements OnChanges {
    @Input() series: Object[];
    @Input() title: string;
    @Input() xAxis: Object;
    @Input() yAxis: Object;

    ngOnChanges() {
        this.options = {
            title: { text : this.title },
            xAxis: this.xAxis,
            yAxis: this.yAxis,
            series: this.series
        };
    }

    chart: HighchartsChartObject;
    options: HighchartsOptions;

    saveInstance(chartInstance) {
        this.chart = chartInstance;
    }
}
