// import {Component} from 'angular2/core';
// import {CORE_DIRECTIVES, FORM_DIRECTIVES} from 'angular2/common';
// import {TimepickerComponent} from 'ng2-bootstrap/ng2-bootstrap';
//
// @Component({
//     selector: 'date-picker',
//     templateUrl: 'app/date-picker.component.html',
//     directives: [
//         TimepickerComponent,
//         CORE_DIRECTIVES,
//         FORM_DIRECTIVES
//     ]
// })
// export class DatePickerComponent {
//     public hstep: number = 1;
//     public mstep: number = 1;
//     public ismeridian: boolean = true;
//     public month: string = 'May';
//     public day: number = 15;
//     public year: number = 1992;
//     public monthList = ['January', 'February', 'March', 'April', 'May', 'June',
//         'July', 'August', 'September', 'October', 'November', 'December'];
//
//     public mytime: Date = new Date();
//     public options: any = {
//         hstep: [1, 2, 3],
//         mstep: [1, 5, 10, 15, 25, 30],
//         month: ['January', 'February', 'March', 'April', 'May', 'June',
//             'July', 'August', 'September', 'October', 'November', 'December']
//     };
//
//     public toggleMode(): void {
//         this.ismeridian = !this.ismeridian;
//     };
//
//     public update(): void {
//         let d = new Date();
//         d.setHours(14);
//         d.setMinutes(0);
//         this.mytime = d;
//     };
//
//     public changed(): void {
//         console.log('Time changed to: ' + this.mytime);
//     };
//
//     public clear(): void {
//         this.mytime = void 0;
//     };
// }
