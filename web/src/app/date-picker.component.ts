import {Component} from '@angular/core';
import {CORE_DIRECTIVES, FORM_DIRECTIVES} from '@angular/common';
import {TimepickerComponent} from 'ng2-bootstrap/ng2-bootstrap';

@Component({
    selector: 'date-picker',
    templateUrl: 'app/date-picker.component.html',
    directives: [
        TimepickerComponent,
        CORE_DIRECTIVES,
        FORM_DIRECTIVES
    ]
})
export class DatePickerComponent {
    date: Date = new Date();
    years: number = this.date.getFullYear();
    monthsInt: number = this.date.getMonth() - 1;
    months: string;
    days: number = this.date.getDay();
    maxMonths: number = 11;
    minMonths: number = 0;
    _maxDays: number;
    minDays: number = 1;

    constructor() {
        this.months = this.monthsDictionary[this.monthsInt];
    }

    private monthsDictionary = {
        0: "Jan",
        1: "Feb",
        2: "Mar",
        3: "Apr",
        4: "May",
        5: "Jun",
        6: "Jul",
        7: "Aug",
        8: "Sept",
        9: "Oct",
        10: "Nov",
        11: "Dec"
    }

    private get maxDays(): number {
        let m30 = [3, 5, 8, 10];
        let m31 = [0, 2, 4, 6, 7, 9, 11];

        if (m31.includes(this.monthsInt)) {
            this._maxDays = 31;
        }
        else if (m30.includes(this.monthsInt)) {
            this._maxDays = 30;
        }
        else if (this.years % 4 == 0) {
            this._maxDays = 29;
        }
        else {
            this._maxDays = 28;
        }

        return this._maxDays
    }

    changed(): void {
        console.log('Time changed to: ' + this.date);
    }

    protected incrementYears(): void {
        this.years += 1;
        this.date.setFullYear(this.years);
    }

    protected decrementYears(): void {
        this.years -= 1;
        this.date.setFullYear(this.years);
    }

    protected updateYears(): void {

    }

    protected incrementMonths(): void {
        this.monthsInt = (this.monthsInt + 1) % 12;
        this.date.setMonth(this.monthsInt + 1);
        this.months = this.monthsDictionary[this.monthsInt];
    }

    private noIncrementMonths(): boolean {
        let incrementSelected = this.monthsInt + 1;
        console.log(incrementSelected > this.maxMonths);
        return incrementSelected > this.maxMonths;
    }

    protected decrementMonths(): void {
        this.monthsInt =
            this.monthsInt - 1 < this.minMonths ? this.maxMonths : this.monthsInt - 1;
        this.date.setMonth(this.monthsInt + 1);
        this.months = this.monthsDictionary[this.monthsInt];
    }

    private noDecrementMonths(): boolean {
        let decrementSelected = this.monthsInt - 1;
        return decrementSelected < this.minMonths;
    }

    protected incrementDays(): void {
        this.days = this.days + 1 > this.maxDays ? this.minDays : this.days + 1;
        this.date.setDate(this.days);
    }

    private noIncrementDays(): boolean {
        let incrementSelected = this.days + 1;
        return incrementSelected > this.maxDays;
    }

    protected decrementDays(): void {
        this.days =
            this.days - 1 < this.minDays ? this.maxDays + 1 : this.days - 1;
        this.date.setDate(this.days);
    }

    private noDecrementDays(): boolean {
        let decrementSelected = this.days - 1;
        return decrementSelected < this.minDays;
    }

}
