import {Component, Input} from '@angular/core';
import {
  CORE_DIRECTIVES, NgIf,
  NgSwitch, NgSwitchWhen, NgSwitchDefault} from '@angular/common';
import {DROPDOWN_DIRECTIVES} from 'ng2-bootstrap/ng2-bootstrap';

import {ElasticsearchService} from './elasticsearch.service';
import {LineChartComponent} from './line-chart.component';
import {StockChartComponent} from './stock-chart.component';
import {DatePickerComponent} from './date-picker.component';
import {DatetimeRange} from './datetime-range';
import {TimeResolution} from './time-resolution'

@Component({
  selector: 'showcase',
  templateUrl: 'app/showcase.component.html',
  directives: [
    DROPDOWN_DIRECTIVES,
    CORE_DIRECTIVES,
    NgIf,
    NgSwitch, NgSwitchWhen, NgSwitchDefault,
    LineChartComponent,
    StockChartComponent,
    DatePickerComponent
  ]
})
export class ShowcaseComponent {
  @Input() daterange: DatetimeRange;

  public chartTypeStatus: { isopen: boolean } = { isopen: false };
  public chartTypes: Array<string> = [
    'Line Chart',
    'Stock Chart'];
  public chartType: string;
  public currencyStatus: { isopen: boolean } = { isopen: false };
  public currencyPairs: Array<string> = [
    'EUR/USD',
    'USD/JPY',
    'GBP/USD',
    'AUD/USD',
    'USD/CHF',
    'NZD/USD',
    'USD/CAD'];
  public currencyPair: string;

  public data: Object[];

  constructor(private _es: ElasticsearchService) { }

  public getChartSelection(choice) {
    this.chartType = choice;
    
    if (this.currencyPair) {
      this.getData(this.currencyPair, this.daterange, TimeResolution.M1);
    }
  }

  public getCurrencySelection(choice) {
    this.currencyPair = choice;
  }

  private getData(
    currencyPair: string,
    daterange: DatetimeRange, resolution: string) {
    this._es.getHistory(
      currencyPair,
      daterange.startDate.toISOString(), daterange.endDate.toISOString(),
      resolution
    ).subscribe(
      res => this.data = res,
      error => console.debug(error)
      );

    console.log(daterange.startDate.toISOString());
  }
}
