import {Component, OnChanges, Input} from '@angular/core';
import {CHART_DIRECTIVES, Highcharts} from 'angular2-highcharts';

@Component({
    selector: 'tree-map',
    directives: [CHART_DIRECTIVES],
    template: `<chart [options]="options"></chart>`
})
export class TreeMapComponent implements OnChanges {
    @Input() treeMapData: Object[];
    @Input() title: string;

    ngOnChanges() {
        this.options = {
            colorAxis: {
                minColor: '#FFFFFF',
                maxColor: Highcharts.getOptions().colors[0]
            },
            title : { text : this.title },
            series : [{
                type: 'treemap',
                data: this.treeMapData
            }],
            credits: false,
        };
    }
    options: Object;
}
