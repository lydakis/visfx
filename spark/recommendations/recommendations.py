from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext
from pyspark.ml.recommendation import ALS
from elasticsearch_interface import get_es_rdd, save_es_rdd, \
    get_currency_pair_dict
from ratings import get_ratings, pca, linear_combination
from utils import parse_range, parse_dates
import math
import json
import hashlib

def generate_predictions(training_df, prediction_df, rank, model=None):
    iterations = 10
    als = ALS(rank=rank, maxIter=iterations, implicitPrefs=True)
    if model == None:
        model = als.fit(training_df)
    return model.transform(prediction_df).dropna()

def train_model(training_df, rank):
    iterations = 10
    als = ALS(rank=rank, maxIter=iterations, implicitPrefs=True)
    return als.fit(training_df)

def compute_model_parameters(ratings):
    training_df, validation_df, test_df = \
        ratings.randomSplit([6.0, 2.0, 2.0], seed=0L)
    validation_for_predict_df = validation_df.drop('rating')
    test_for_predict_df = test_df.drop('rating')
    ranks = [2, 4, 8, 12]
    errors = [0, 0, 0, 0]
    err = 0
    min_evaluation_error = float('inf')
    best_rank = -1
    for rank in ranks:
        predictions = \
            generate_predictions(training_df, validation_for_predict_df, rank) \
                .map(lambda r: ((r[0], r[1]), r[2]))
        rates_and_preds = validation_df \
            .map(lambda r: ((r[0], r[1]), r[2])).join(sc.parallelize(predictions.collect()))
        evaluation_error = math.sqrt(rates_and_preds \
            .map(lambda r: (r[1][0] - r[1][1])**2).mean())
        errors[err] = evaluation_error
        err += 1
        if evaluation_error < min_evaluation_error:
            min_evaluation_error = evaluation_error
            best_rank = rank
    predictions = \
        generate_predictions(training_df, test_for_predict_df, best_rank) \
            .map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = test_df \
        .map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    test_error = math.sqrt(
        rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    return best_rank, errors, test_error

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], \
        (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

def generate_new_user_recommendations(
        model, ratings, best_rank, currency_pair_rdd, new_user_id):
    new_user_unrated_movies_df = currency_pair_rdd.map(lambda (key, value):
        (new_user_id, key)).toDF(["user", "item"])
    return generate_predictions(
        ratings,
        new_user_unrated_movies_df,
        best_rank, model)

def get_top_currency_pairs_recommendation(
        new_user_recommendations_df, currency_pair_rdd, threshold, top_count):
    new_user_recommendations_rating_rdd = \
        new_user_recommendations_df.map(lambda x: (int(x[1]), x[2]))
    new_user_recommendations_rating_title_and_count_rdd = \
        sc.parallelize(new_user_recommendations_rating_rdd.collect()) \
            .join(currency_pair_rdd) \
            .join(currency_pair_rating_counts_rdd)
    new_user_recommendations_rating_title_and_count_rdd = \
        new_user_recommendations_rating_title_and_count_rdd \
            .map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    return \
        new_user_recommendations_rating_title_and_count_rdd \
        .filter(lambda r: r[2]>=threshold) \
        .takeOrdered(top_count, key=lambda x: -x[1])

def get_country(rdd, provider_id):
    return str(rdd.filter(lambda (_, body): provider_id == body['provider_id']) \
        .map(lambda (_, body): body['country']).top(1)[0])

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
    conf = SparkConf().setAppName('Currency Recommendations')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    currency_pair_dict = get_currency_pair_dict(sc)
    query = json.dumps({
        'query': {
            'bool': {
                'must': [
                    { 'term': { 'start_date': start_date }},
                    { 'term': { 'end_date': end_date }}
                ]
            }
        }
    })

    rdd = get_es_rdd(sc, query=query, index='forex/rating')

    c_rdd = get_es_rdd(sc, index='forex/currency_pair')

    currency_pair_rdd = c_rdd.map(lambda (item):
            (1000 + item[1]['currency_pair_id'], 'BUY ' + item[1]['currency_pair'])) \
        .union(c_rdd.map(lambda item:
                (2000 + item[1]['currency_pair_id'], 'SELL ' + item[1]['currency_pair'])))

    ratings = rdd \
        .map(lambda (_, body): (
            body['provider_id'],
            currency_pair_dict[body['transaction_type'] + ' '  + body['currency_pair']],
            body['rating'])).toDF(["user", "item", "rating"]).persist(StorageLevel.MEMORY_AND_DISK)

    best_rank, errors, test_error = compute_model_parameters(ratings)

    currency_pairs_with_ratings_rdd = (ratings
        .map(lambda x: (x[1], x[2])).groupByKey())
    currency_pair_id_with_avg_ratings_rdd = \
        currency_pairs_with_ratings_rdd.map(get_counts_and_averages)
    currency_pair_rating_counts_rdd = \
        currency_pair_id_with_avg_ratings_rdd.map(lambda x: (x[0], x[1][0]))

    providers = rdd.map(lambda (_, body): body['provider_id']).collect()

    model = train_model(ratings, best_rank)
    for new_user_id in providers:
        country = get_country(rdd, new_user_id)
        new_user_recommendations_df = generate_new_user_recommendations(
            model, ratings, best_rank, currency_pair_rdd, new_user_id)
        top_currency_pairs = get_top_currency_pairs_recommendation(
            new_user_recommendations_df, currency_pair_rdd, 0, 25)
        top_currency_pairs_rdd = sc.parallelize(map(lambda (currency_pair, rating, volume):
            (make_key((new_user_id, currency_pair.split()[1],
                    currency_pair.split()[0], country)), {
                'recommendation_id': make_key((new_user_id, currency_pair.split()[1],
                        currency_pair.split()[0], country)),
                'provider_id': new_user_id,
                'currency_pair': currency_pair.split()[1],
                'transaction_type': currency_pair.split()[0],
                'start_date': start_date,
                'end_date': end_date,
                'country': country,
                'rating': rating,
                'volume': volume
            }), top_currency_pairs))
        save_es_rdd(top_currency_pairs_rdd, 'forex/recommendation', key='recommendation_id')
