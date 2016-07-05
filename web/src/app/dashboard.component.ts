import {Component, OnInit} from '@angular/core';
import {HTTP_PROVIDERS}  from '@angular/http';

import {ElasticsearchService} from './elasticsearch.service'
import {TimeResolution} from './time-resolution'
import {LineChartComponent} from './line-chart.component';
import {StockChartComponent} from './stock-chart.component';
import {TreeMapComponent} from './tree-map.component';
import {DatePickerComponent} from './date-picker.component';
import {DatetimeRange} from './datetime-range';
import {ShowcaseComponent} from './showcase.component';
import {AreaChart} from './area-chart.component';
import {AreaChartConfig} from './area-chart-config';

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
    DatePickerComponent,
    ShowcaseComponent,
    AreaChart
  ]
})
export class DashboardComponent implements OnInit {
  dashboardTitle: string = "FX Dashboard";
  errorMessage: string;
  res: Object;
  title = 'EUR/USD';
  data: Object[];
  date: DatetimeRange = new DatetimeRange();

  private areaChartConfig: Array<AreaChartConfig>;

  constructor(private _es: ElasticsearchService) { }

  ngOnInit() {
    this._es.getHistory(
      'EUR/USD',
      '2015-01-01T00:00:00.000Z', '2015-12-31T00:00:00.000Z',
      TimeResolution.W1)
      .subscribe(
      res => this.data = res,
      error => this.errorMessage = <any>error
      );
    this.getStats();
  }

  getStats() {
    let stats = {
      "customerOrderStats": [
        {
          "date": "2016-04-14T21:18:26.838Z",
          "universal": 1460668706838,
          "count": 0
        },
        {
          "date": "2016-04-15T21:18:26.838Z",
          "universal": 1460755106838,
          "count": 0
        },
        {
          "date": "2016-04-16T21:18:26.838Z",
          "universal": 1460841506838,
          "count": 0
        },
        {
          "date": "2016-04-17T21:18:26.838Z",
          "universal": 1460927906838,
          "count": 0
        },
        {
          "date": "2016-04-18T21:18:26.838Z",
          "universal": 1461014306838,
          "count": 0
        },
        {
          "date": "2016-04-19T21:18:26.838Z",
          "universal": 1461100706838,
          "count": 0
        },
        {
          "date": "2016-04-20T21:18:26.838Z",
          "universal": 1461187106838,
          "count": 0
        },
        {
          "date": "2016-04-21T21:18:26.838Z",
          "universal": 1461273506838,
          "count": 1
        }
      ],
      "customerIncomeStats": [
        {
          "date": "2016-04-14T21:18:26.842Z",
          "universal": 1460668706842,
          "count": 0
        },
        {
          "date": "2016-04-15T21:18:26.842Z",
          "universal": 1460755106842,
          "count": 0
        },
        {
          "date": "2016-04-16T21:18:26.842Z",
          "universal": 1460841506842,
          "count": 0
        },
        {
          "date": "2016-04-17T21:18:26.842Z",
          "universal": 1460927906842,
          "count": 0
        },
        {
          "date": "2016-04-18T21:18:26.842Z",
          "universal": 1461014306842,
          "count": 0
        },
        {
          "date": "2016-04-19T21:18:26.842Z",
          "universal": 1461100706842,
          "count": 0
        },
        {
          "date": "2016-04-20T21:18:26.842Z",
          "universal": 1461187106842,
          "count": 0
        },
        {
          "date": "2016-04-21T21:18:26.842Z",
          "universal": 1461273506842,
          "count": 10
        }
      ]
    }

    let customerIncomeArea = new AreaChartConfig();
    customerIncomeArea.settings = {
      fill: 'rgba(1, 67, 163, 1)',
      interpolation: 'monotone'
    };
    customerIncomeArea.dataset = stats.customerIncomeStats.map(data => {
      return { x: new Date(data.date), y: data.count };
    });

    // We create a new AreaChartConfig object to set orders by customer config
    let customerOrderArea = new AreaChartConfig();
    customerOrderArea.settings = {
      fill: 'rgba(195, 0, 47, 1)',
      interpolation: 'monotone'
    };
    customerOrderArea.dataset = stats.customerOrderStats.map(data => {
      return { x: new Date(data.date), y: data.count };
    });

    // to finish we append our AreaChartConfigs into an array of configs
    this.areaChartConfig = new Array<AreaChartConfig>();
    this.areaChartConfig.push(customerIncomeArea);
    this.areaChartConfig.push(customerOrderArea);
  }
}
