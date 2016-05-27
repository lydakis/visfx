import {Component} from '@angular/core';
import {HTTP_PROVIDERS, JSONP_PROVIDERS} from '@angular/http';
import {FORM_PROVIDERS} from '@angular/common';
import {
    Router,
    Routes,
    ROUTER_DIRECTIVES,
    ROUTER_PROVIDERS,

} from '@angular/router';

import {DashboardComponent} from './dashboard.component';

@Routes([
    { path: '/', component: DashboardComponent },
    { path: '/currency', component: DashboardComponent },
    { path: '/country', component: DashboardComponent },
    { path: '/provider', component: DashboardComponent },
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
    selectedPage: string;

    constructor(private router: Router) {
        this.attachEvents();
        console.log(router.urlTree.contains(router.createUrlTree(['/currency'])));
        router.changes.subscribe(() => {
            if (router.urlTree.contains(router.createUrlTree(['/']))) {
                this.selectedPage = "/";
            }
            if (router.urlTree.contains(router.createUrlTree(['/currency']))) {
                console.log("test");
                this.selectedPage = "/currency";
            }
            if (router.urlTree.contains(router.createUrlTree(['/country']))) {
                this.selectedPage = "/country";
            }
            if (router.urlTree.contains(router.createUrlTree(['/provider']))) {
                this.selectedPage = "/provider";
            }
        })
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
