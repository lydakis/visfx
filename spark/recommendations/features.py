from pyspark import SparkContext, SparkConf, StorageLevel
import datetime as dt
from elasticsearch_interface \
    import get_es_rdd, save_es_rdd, get_currency_pair_dict
from utils import parse_range, parse_dates, modify_record

def pc_trade_count(rdd):
    return rdd \
        .map(lambda (_, values):
            ((values['provider_id'],
            values['currency_pair'],
            values['transaction_type']), 1)) \
        .reduceByKey(lambda a, b: a + b)

def pc_amount(rdd):
    return rdd \
        .map(lambda (_, values):
            ((values['provider_id'],
            values['currency_pair'],
            values['transaction_type']), values['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def pc_net_pnl(rdd):
    return rdd \
        .map(lambda (_, values):
            ((values['provider_id'],
            values['currency_pair'],
            values['transaction_type']), values['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def pc_trade_duration(rdd):
    return rdd \
        .map(lambda (_, values):
            ((values['provider_id'],
            values['currency_pair'],
            values['transaction_type']),
                total_seconds(dateFormat(values['date_closed']) -
                dateFormat(values['date_open'])))) \
        .combineByKey(lambda value: (value, 1),
             lambda x, value: (x[0] + value, x[1] + 1),
             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (key, (value_sum, count)): (key, value_sum / count))

def pnl_per_amount(pnl_rdd, amount_rdd):
    per_amount = amount_rdd.map(lambda (key, value): (key, 1.0 / value))
    union = pnl_rdd.union(per_amount)
    return union.reduceByKey(lambda a, b: a * b)

def p_trade_count(rdd):
    return rdd \
        .map(lambda (_, values):
            (values['provider_id'], 1)) \
        .reduceByKey(lambda a, b: a + b)

def p_net_pnl(rdd):
    return rdd \
        .map(lambda (_, values): (values['provider_id'], values['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def p_amount(rdd):
    return rdd \
        .map(lambda (_, values): (values['provider_id'], values['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def p_trade_duration(rdd):
    return rdd \
        .map(lambda (_, values):
            (values['provider_id'],
                total_seconds(dateFormat(values['date_closed']) -
                dateFormat(values['date_open'])))) \
        .combineByKey(lambda value: (value, 1),
             lambda x, value: (x[0] + value, x[1] + 1),
             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (key, (value_sum, count)): (key, value_sum / count))

def c_trade_count(rdd):
    return rdd \
        .map(lambda (_, values):
            ((values['currency_pair'], values['transaction_type']), 1)) \
        .reduceByKey(lambda a, b: a + b)

def c_amount(rdd):
    return rdd \
        .map(lambda (_, values):
        ((values['currency_pair'],
        values['transaction_type']), values['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def c_net_pnl(rdd):
    return rdd \
        .map(lambda (_, values):
        ((values['currency_pair'],
        values['transaction_type']), values['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def pc_trade_duration(rdd):
    return rdd \
        .map(lambda (_, values):
            ((values['currency_pair'],
            values['transaction_type']),
                total_seconds(dateFormat(values['date_closed']) -
                dateFormat(values['date_open'])))) \
        .combineByKey(lambda value: (value, 1),
             lambda x, value: (x[0] + value, x[1] + 1),
             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (key, (value_sum, count)): (key, value_sum / count))

def l_trade_count(rdd):
    return rdd \
        .map(lambda (_, values):
            (values['country'], 1)) \
        .reduceByKey(lambda a, b: a + b)

def l_net_pnl(rdd):
    return rdd \
        .map(lambda (_, values): (values['country'], values['net_pnl'])) \
        .reduceByKey(lambda a, b: a + b)

def l_amount(rdd):
    return rdd \
        .map(lambda (_, values): (values['country'], values['amount'])) \
        .reduceByKey(lambda a, b: a + b)

def l_trade_duration(rdd):
    return rdd \
        .map(lambda (_, values):
            (values['country'],
                total_seconds(dateFormat(values['date_closed']) -
                dateFormat(values['date_open'])))) \
        .combineByKey(lambda value: (value, 1),
             lambda x, value: (x[0] + value, x[1] + 1),
             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (key, (value_sum, count)): (key, value_sum / count))

def normalize_feature(rdd, mean_normalization=True):
    max_value = rdd.map(lambda (_, value): value).max()
    min_value = rdd.map(lambda (_, value): value).min()
    count = rdd.count()
    average = rdd \
        .map(lambda (_, value): value) \
        .reduce(lambda a, b: a + b) / float(count)
    return rdd.map(
        lambda (_, value): (value,
            float(value - average
                if mean_normalization else min_value) / max_value))

def generate_features(rdd):
    pc_trade_count_rdd = pc_trade_count(rdd)
    pc_amount_rdd = pc_amount(rdd)
    pc_net_pnl_rdd = pc_net_pnl(rdd)
    pc_trade_duration_rdd = pc_trade_duration(rdd)
    p_trade_count_rdd = p_trade_count(rdd)
    p_amount_rdd = p_amount(rdd)
    p_net_pnl_rdd = p_net_pnl(rdd)
    p_trade_duration_rdd = p_trade_duration(rdd)
    c_trade_count_rdd = c_trade_count(rdd)
    c_amount_rdd = c_amount(rdd)
    c_net_pnl_rdd = c_net_pnl(rdd)
    c_trade_duration_rdd = c_trade_duration(rdd)
    l_trade_count_rdd = l_trade_count(rdd)
    l_amount_rdd = l_amount(rdd)
    l_net_pnl_rdd = l_net_pnl(rdd)
    l_trade_duration_rdd = l_trade_duration(rdd)

def canonicalize_keys(rdd, feature, kind):
    keys = get_keys(rdd)
    if 'pc' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                ((provider_id, currency_pair, transaction_type), country)) \
            .join(feature) \
            .map(lambda ((
                provider_id, currency_pair, transaction_type), (country, value)):
                    ((provider_id, currency_pair, transaction_type, country),
                        value))
    elif 'p' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                (provider_id, (currency_pair, transaction_type, country))) \
            .join(feature) \
            .map(lambda (
                provider_id, (currency_pair, transaction_type, country, value)):
                    ((provider_id, currency_pair, transaction_type, country),
                        value))
    elif 'c' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                (currency_pair, (provider_id, transaction_type, country))) \
            .join(feature) \
            .map(lambda
                (currency_pair, (provider_id, transaction_type, country, value)):
                    ((provider_id, currency_pair, transaction_type, country),
                        value))
    elif 'l' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                (country, (provider_id, currency_pair, transaction_type))) \
            .join(feature) \
            .map(lambda
                (country, (provider_id, currency_pair, transaction_type, value)):
                    ((provider_id, currency_pair, transaction_type, country),
                        value))
    else:
        raise

def get_keys(rdd):
    return rdd \
        .map(lambda (_, values):
            (values['provider_id'],
            values['currency_pair'],
            values['transaction_type'],
            values['country'])) \
        .distinct()

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Currency Ratings')
    sc = SparkContext(conf=conf)

    start_date, end_date = parse_dates('2015-05-07', '7d')
    rdd = get_es_rdd(sc, index='forex/transaction', date_field='date_closed',
        start_date=start_date, end_date=end_date) \
            .persist(StorageLevel.MEMORY_AND_DISK)
