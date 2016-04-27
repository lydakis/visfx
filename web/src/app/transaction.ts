export interface Transaction {
    amount: number;
    country: string;
    currency_pair: string;
    date_closed: Date;
    date_open: Date;
    language: string;
    net_pnl: number;
    provider_id: number;
    transaction_id: number;
    transaction_type: string;
}
