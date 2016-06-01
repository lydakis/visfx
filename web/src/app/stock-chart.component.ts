import {Component, OnChanges, Input} from '@angular/core';
import {CHART_DIRECTIVES, Highcharts} from 'angular2-highcharts';

@Component({
  selector: 'stock-chart',
  directives: [CHART_DIRECTIVES],
  template: `<chart type="StockChart" [options]="options"></chart>`
})
export class StockChartComponent implements OnChanges {
  @Input() series: Object[];
  @Input() title: string;

  ngOnChanges() {
    this.options = {
      title : { text : this.title },
      series : this.series,
      credits: false
    };
  }
  options: HighstockOptions;
}
