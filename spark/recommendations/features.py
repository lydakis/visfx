from pyspark import SparkContext, SparkConf, StorageLevel
import datetime as dt
import hashlib
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
    per_amount = amount_rdd.map(lambda (key, value): (key, 1.0 / value)
        if value != 0 else (key, 0))
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

def c_trade_duration(rdd):
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

def normalize_feature(rdd, mean_normalization=False):
    max_value = rdd.map(lambda (_, value): value).max()
    min_value = rdd.map(lambda (_, value): value).min()
    count = rdd.count()
    average = rdd \
        .map(lambda (_, value): value) \
        .reduce(lambda a, b: a + b) / float(count)
    return rdd.map(
        lambda (_, value): (value,
            float(value - average / (max_value - min_value)
                if mean_normalization else
                    value - min_value / (max_value - min_value))))

def generate_features(rdd):
    pc_trade_count_rdd = \
        fix_keys(rdd, pc_trade_count(rdd), 'pc_trade_count', 'pc')
    pc_trade_duration_rdd = \
        fix_keys(rdd, pc_trade_duration(rdd), 'pc_trade_duration', 'pc')
    pc_amount_rdd = pc_amount(rdd)
    pc_net_pnl_rdd = pc_net_pnl(rdd)
    pc_pnl_per_amount_rdd = \
        fix_keys(rdd, pnl_per_amount(pc_net_pnl_rdd, pc_amount_rdd),
            'pc_pnl_per_amount', 'pc')
    pc_amount_rdd = fix_keys(rdd, pc_amount_rdd, 'pc_amount', 'pc')
    pc_net_pnl_rdd = fix_keys(rdd, pc_net_pnl_rdd, 'pc_net_pnl', 'pc')
    p_trade_count_rdd = fix_keys(rdd, p_trade_count(rdd), 'p_trade_count', 'p')
    p_trade_duration_rdd = \
        fix_keys(rdd, p_trade_duration(rdd), 'p_trade_duration', 'p')
    p_amount_rdd = p_amount(rdd)
    p_net_pnl_rdd = p_net_pnl(rdd)
    p_pnl_per_amount_rdd = \
        fix_keys(rdd, pnl_per_amount(p_net_pnl_rdd, p_amount_rdd),
            'p_pnl_per_amount', 'p')
    p_amount_rdd = fix_keys(rdd, p_amount_rdd, 'p_amount', 'p')
    p_net_pnl_rdd = fix_keys(rdd, p_net_pnl_rdd, 'p_net_pnl', 'p')
    c_trade_count_rdd = fix_keys(rdd, c_trade_count(rdd), 'c_trade_count', 'c')
    c_trade_duration_rdd = \
        fix_keys(rdd, c_trade_duration(rdd), 'c_trade_duration', 'c')
    c_amount_rdd = c_amount(rdd)
    c_net_pnl_rdd = c_net_pnl(rdd)
    c_pnl_per_amount_rdd = \
        fix_keys(rdd, pnl_per_amount(c_net_pnl_rdd, c_amount_rdd),
            'c_pnl_per_amount', 'c')
    c_amount_rdd = fix_keys(rdd, c_amount_rdd, 'c_amount', 'c')
    c_net_pnl_rdd = fix_keys(rdd, c_net_pnl_rdd, 'c_net_pnl', 'c')
    l_trade_count_rdd = fix_keys(rdd, l_trade_count(rdd), 'l_trade_count', 'l')
    l_trade_duration_rdd = \
        fix_keys(rdd, l_trade_duration(rdd), 'l_trade_duration', 'l')
    l_amount_rdd = l_amount(rdd)
    l_net_pnl_rdd = l_net_pnl(rdd)
    l_pnl_per_amount_rdd = \
        fix_keys(rdd, pnl_per_amount(l_net_pnl_rdd, l_amount_rdd),
            'l_pnl_per_amount', 'l')
    l_amount_rdd = fix_keys(rdd, l_amount_rdd, 'l_amount', 'l')
    l_net_pnl_rdd = fix_keys(rdd, l_net_pnl_rdd, 'l_net_pnl', 'l')
    return [pc_trade_count_rdd, pc_amount_rdd, pc_net_pnl_rdd, \
            pc_trade_duration_rdd, pc_pnl_per_amount_rdd, p_trade_count_rdd, \
            p_amount_rdd, p_net_pnl_rdd, p_trade_duration_rdd, \
            p_pnl_per_amount_rdd, c_trade_count_rdd, c_amount_rdd, \
            c_net_pnl_rdd, c_trade_duration_rdd, c_pnl_per_amount_rdd, \
            l_trade_count_rdd, l_amount_rdd, l_net_pnl_rdd, \
            l_trade_duration_rdd, l_trade_duration_rdd]

def fix_keys(rdd, feature, name, kind):
    keys = get_keys(rdd)
    if 'pc' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                ((provider_id, currency_pair, transaction_type), country)) \
            .join(feature) \
            .map(lambda ((
                provider_id, currency_pair, transaction_type), (country, value)):
                    (make_key((provider_id, currency_pair, transaction_type, country)),
                        {
                            'feature_id': make_key((provider_id, currency_pair, transaction_type, country)),
                            'provider_id': provider_id,
                            'currency_pair': currency_pair,
                            'transaction_type': transaction_type,
                            'country': country,
                            'start_date': start_date,
                            'end_date': end_date,
                            name: value
                        }))
    elif 'p' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                (provider_id, (currency_pair, transaction_type, country))) \
            .join(feature) \
            .map(lambda (
                provider_id, ((currency_pair, transaction_type, country), value)):
                    (make_key((provider_id, currency_pair, transaction_type, country)),
                        {
                            'feature_id': make_key((provider_id, currency_pair, transaction_type, country)),
                            'provider_id': provider_id,
                            'currency_pair': currency_pair,
                            'transaction_type': transaction_type,
                            'country': country,
                            'start_date': start_date,
                            'end_date': end_date,
                            name: value
                        }))
    elif 'c' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                ((currency_pair, transaction_type), (provider_id, country))) \
            .join(feature) \
            .map(lambda
                ((currency_pair, transaction_type), ((provider_id, country), value)):
                    (make_key((provider_id, currency_pair, transaction_type, country)),
                        {
                            'feature_id': make_key((provider_id, currency_pair, transaction_type, country)),
                            'provider_id': provider_id,
                            'currency_pair': currency_pair,
                            'transaction_type': transaction_type,
                            'country': country,
                            'start_date': start_date,
                            'end_date': end_date,
                            name: value
                        }))
    elif 'l' == kind:
        return keys \
            .map(lambda (provider_id, currency_pair, transaction_type, country):
                (country, (provider_id, currency_pair, transaction_type))) \
            .join(feature) \
            .map(lambda
                (country, ((provider_id, currency_pair, transaction_type), value)):
                    ((provider_id, currency_pair, transaction_type, country),
                        {
                            'feature_id': make_key((provider_id, currency_pair, transaction_type, country)),
                            'provider_id': provider_id,
                            'currency_pair': currency_pair,
                            'transaction_type': transaction_type,
                            'country': country,
                            'start_date': start_date,
                            'end_date': end_date,
                            name: value
                        }))
    else:
        raise

def get_keys(rdd):
    return rdd \
        .map(lambda (_, values):
            (values['provider_id'],
            values['currency_pair'],
            values['transaction_type'],
            values['country']))

def total_seconds(timedelta):
    return (
        timedelta.microseconds + 0.0 +
        (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

def dateFormat(date):
    return dt.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')

def make_key(key):
    provider_id, currency_pair, transaction_type, country = key
    m = hashlib.md5()
    m.update(
            str(provider_id) +
            str(currency_pair) +
            str(transaction_type) +
            str(country) +
            str(start_date) + str(end_date))
    return m.hexdigest()

start_date = '2015-05-01'
end_date = '2015-05-07'

if __name__ == '__main__':
    conf = SparkConf().setAppName('Compute Features')
    sc = SparkContext(conf=conf)

    rdd = get_es_rdd(sc, index='forex/transaction', date_field='date_closed',
        start_date=start_date, end_date=end_date) \
            .persist(StorageLevel.MEMORY_AND_DISK)

    features = generate_features(rdd)
    for feature in features:
        save_es_rdd(feature, 'forex/feature', key='feature_id', upsert=True)
