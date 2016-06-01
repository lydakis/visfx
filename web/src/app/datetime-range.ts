export class DatetimeRange {
  private _startDate: Date;
  private _endDate: Date;
  private _monthsDict = {
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
  };

  constructor(daterange?: DatetimeRange) {
    if(daterange) {
      this._startDate = daterange.startDate;
      this._endDate = daterange.endDate;
    }
    else {
      this._startDate = new Date();
      this._endDate = new Date();
      this._startDate.setMonth(this._startDate.getMonth() - 6);
    }
  }

  maxMonth: number = 11;
  minMonth: number = 0;
  minDay: number = 1;
  maxDay(month: number, year: number): number {
    let m30 = [3, 5, 8, 10];
    let m31 = [0, 2, 4, 6, 7, 9, 11];
    let maxDays = 28;

    if (m31.includes(month)) {
      maxDays = 31;
    }
    else if (m30.includes(month)) {
      maxDays = 30;
    }
    else if (year % 4 == 0) {
      maxDays = 29;
    }

    return maxDays
  }

  get startDate(): Date {
    return this._startDate;
  }

  set startDate(sd: Date) {
    this._startDate = sd;
  }

  get endDate(): Date {
    return this._endDate;
  }

  set endDate(ed: Date) {
    this._endDate = ed;
  }

  get startYear(): number {
    return this._startDate.getFullYear();
  }

  set startYear(sy: number) {
    this._startDate.setFullYear(sy);
  }

  get endYear(): number {
    return this._endDate.getFullYear();
  }

  set endYear(ey: number) {
    this._endDate.setFullYear(ey);
  }

  get startMonth(): number {
    return this._startDate.getMonth();
  }

  get startMonthString(): string {
    return this._monthsDict[this._startDate.getMonth()];
  }

  set startMonth(sm: number) {
    if (sm >= this.minMonth && sm <= this.maxMonth) {
      this._startDate.setMonth(sm);
    }
  }

  get endMonth(): number {
    return this._endDate.getMonth();
  }

  get endMonthString(): string {
    return this._monthsDict[this._endDate.getMonth()];
  }

  set endMonth(em: number) {
    if (em >= this.minMonth && em <= this.maxMonth) {
      this._endDate.setMonth(em);
    }
  }

  get startDay(): number {
    return this._startDate.getDate();
  }

  set startDay(sd: number) {
    if (sd >= this.minDay
      && sd <= this.maxDay(this.startMonth, this.startYear)) {
      this._startDate.setDate(sd);
    }
  }

  get endDay(): number {
    return this._endDate.getDate();
  }

  set endDay(ed: number) {
    if (ed >= this.minDay
      && ed <= this.maxDay(this.endMonth, this.endYear)) {
      this._endDate.setDate(ed);
    }
  }
}
