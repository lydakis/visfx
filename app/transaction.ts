export interface Transaction {
    _timestamp: Date;
    _version: number;
    amount: number;
    country: string;
    currencyPair: string;
    dateClosed: Date;
    dateOpen: Date;
    language: string;
    netPnl: number;
    providerId: number;
    transactionId: number;
    transactionType: string;
}