import {Component} from 'angular2/core';
import {HTTP_PROVIDERS, JSONP_PROVIDERS} from 'angular2/http';
import {FORM_PROVIDERS} from 'angular2/common';
import {
    RouteConfig,
    ROUTER_DIRECTIVES,
    ROUTER_PROVIDERS,
} from 'angular2/router';

import {DashboardComponent} from './dashboard.component';

@RouteConfig([
    {path: '/', component: DashboardComponent, name: 'Dashboard'},
])
@Component({
    selector: 'app',
    templateUrl: 'app/app.component.html',
    styleUrls: ['app/app.component.css'],
    providers: [
        ROUTER_PROVIDERS, FORM_PROVIDERS, HTTP_PROVIDERS
    ],
    directives: [ROUTER_DIRECTIVES]
})
export class AppComponent {
    mobileView:number = 992;
    toggle:boolean = false;

    constructor() {
        this.attachEvents();
    }

    attachEvents() {
        window.onresize = ()=> {
            if (this.getWidth() >= this.mobileView) {
                if (localStorage.getItem('toggle')) {
                    this.toggle = !localStorage.getItem('toggle') ? false : true;
                } else {
                    this.toggle = true;
                }
            } else {
                this.toggle = false;
            }
        }
    }

    getWidth() {
        return window.innerWidth;
    }

    toggleSidebar() {
        this.toggle = !this.toggle;
        localStorage.setItem('toggle', this.toggle.toString());
    }

}
