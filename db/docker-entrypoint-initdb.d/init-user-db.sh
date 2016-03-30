#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER visfx;
    CREATE DATABASE visfx;
    GRANT ALL PRIVILEGES ON DATABASE visfx TO visfx;

    CREATE TABLE transaction (
        transaction_id serial PRIMARY KEY,
        provider_id integer NOT NULL,
        currency_pair varchar (10) NOT NULL,
        transaction_type varchar (5) NOT NULL,
        amount real NOT NULL,
        date_open timestamp NOT NULL,
        date_closed timestamp NOT NULL,
        net_pnl real NOT NULL,
        country varchar (25) NOT NULL,
        language varchar (10) NOT NULL
    );

    CREATE TABLE history (
        history_id serial PRIMARY KEY,
        currency_pair varchar (10) NOT NULL,
        tick_date timestamp NOT NULL,
        bid_price real NOT NULL,
        ask_price real NOT NULL
    );

    COPY transaction(provider_id, currency_pair, transaction_type, amount, date_open, date_closed, net_pnl, country, language)
    FROM '/tmp/data/transaction/2015.csv' DELIMITER ',' CSV HEADER;

    COPY history(currency_pair, tick_date, bid_price, ask_price)
    FROM '/tmp/data/history/2015.csv' DELIMITER ',' CSV HEADER;
EOSQL
