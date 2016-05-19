import {Component, Self} from '@angular/core';
import {
    NgModel, ControlValueAccessor,
    CORE_DIRECTIVES, FORM_DIRECTIVES} from '@angular/common';
import {TimepickerComponent} from 'ng2-bootstrap/ng2-bootstrap';

import {DatetimeRange} from './datetime-range';

@Component({
    selector: 'date-picker[ngModel]',
    templateUrl: 'app/date-picker.component.html',
    directives: [
        TimepickerComponent,
        CORE_DIRECTIVES,
        FORM_DIRECTIVES
    ]
})
export class DatePickerComponent implements ControlValueAccessor {
    dr: NgModel;
    daterange: DatetimeRange = new DatetimeRange();
    private _selected: DatetimeRange = new DatetimeRange();

    public onChange:any = Function.prototype;
    public onTouched:any = Function.prototype;

    private get selected(): DatetimeRange {
        return this._selected;
    }

    private set selected(v: DatetimeRange) {
        if (v) {
            this._selected = v;
            this.dr.viewToModelUpdate(this.selected);
        }
    }

    constructor( @Self() dr: NgModel) {
        this.dr = dr;
        dr.valueAccessor = this;
    }

    public writeValue(v: any): void {
        if (v === this.selected) {
            return;
        }
        if (v && v instanceof DatetimeRange) {
            this.selected = v;
            return;
        }
        this.selected = v ? new DatetimeRange(v) : void 0;
    }

    public registerOnChange(fn: (_: any) => {}): void { this.onChange = fn; }

    public registerOnTouched(fn: () => {}): void { this.onTouched = fn; }

    protected incrementStartYears(): void {
        this.daterange.startYear += 1;
        this.selected = this.daterange;
    }

    protected decrementStartYears(): void {
        this.daterange.startYear -=1;
        this.selected = this.daterange;
    }

    protected incrementEndYears(): void {
        this.daterange.endYear += 1;
        this.selected = this.daterange;
    }

    protected decrementEndYears(): void {
        this.daterange.endYear -=1;
        this.selected = this.daterange;
    }

    protected incrementStartMonths(): void {
        this.daterange.startMonth =
            (this.daterange.startMonth + 1) > this.daterange.maxMonth ?
            this.daterange.minMonth : this.daterange.startMonth + 1;
        this.selected = this.daterange;
    }

    protected decrementStartMonths(): void {
        this.daterange.startMonth =
            (this.daterange.startMonth - 1) < this.daterange.minMonth ?
            this.daterange.maxMonth : this.daterange.startMonth - 1;
        this.selected = this.daterange;
    }

    protected incrementEndMonths(): void {
        this.daterange.endMonth =
            (this.daterange.endMonth + 1) > this.daterange.maxMonth ?
            this.daterange.minMonth : this.daterange.endMonth + 1;
        this.selected = this.daterange;
    }

    protected decrementEndMonths(): void {
        this.daterange.endMonth =
            (this.daterange.endMonth - 1) < this.daterange.minMonth ?
            this.daterange.maxMonth : this.daterange.endMonth - 1;
        this.selected = this.daterange;
    }

    protected incrementStartDays(): void {
        this.daterange.startDay =
            (this.daterange.startDay + 1) > this.daterange.maxDay(
                this.daterange.startMonth, this.daterange.startYear) ?
            this.daterange.minDay : this.daterange.startDay + 1;
        this.selected = this.daterange;
    }

    protected decrementStartDays(): void {
        this.daterange.startDay =
            (this.daterange.startDay - 1) < this.daterange.minDay ?
            this.daterange.maxDay(
                this.daterange.startMonth, this.daterange.startYear) :
            this.daterange.startDay - 1;
        this.selected = this.daterange;
    }

    protected incrementEndDays(): void {
        this.daterange.endDay =
            (this.daterange.endDay + 1) > this.daterange.maxDay(
                this.daterange.endMonth, this.daterange.endYear) ?
            this.daterange.minDay : this.daterange.endDay + 1;
        this.selected = this.daterange;
    }

    protected decrementEndDays(): void {
        this.daterange.endDay =
            (this.daterange.endDay - 1) < this.daterange.minDay ?
            this.daterange.maxDay(
                this.daterange.endMonth, this.daterange.endYear) :
            this.daterange.endDay - 1;
        this.selected = this.daterange;
    }

}
