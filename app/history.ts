export interface History {
    _timestamp: Date;
    _version: number;
    askPrice: number;
    bidPrice: number;
    currencyPair: string;
    historyId: number;
    tickDate: Date;
}