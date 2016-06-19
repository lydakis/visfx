from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext
from pyspark.ml.recommendation import ALS
from elasticsearch_interface import get_es_rdd, save_es_rdd, \
    get_currency_pair_dict
from ratings import get_ratings, pca, linear_combination
from utils import parse_range, parse_dates
import math

def generate_predictions(training_df, prediction_df, rank):
    seed = 5L
    iterations = 10
    als = ALS(rank=rank, seed=seed, maxIter=iterations)
    model = als.fit(training_df)
    return model.transform(prediction_df).dropna()

def compute_model_parameters(ratings):
    training_df, validation_df, test_df = \
        ratings.randomSplit([6.0, 2.0, 2.0], seed=0L)
    validation_for_predict_df = validation_df.drop('rating')
    test_for_predict_df = test_df.drop('rating')
    ranks = [4, 8, 12]
    errors = [0, 0, 0]
    err = 0
    min_evaluation_error = float('inf')
    best_rank = -1
    for rank in ranks:
        predictions = \
            generate_predictions(training_df, validation_for_predict_df, rank) \
                .map(lambda r: ((r[0], r[1]), r[2])).cache()
        rates_and_preds = validation_df \
            .map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
        evaluation_error = math.sqrt(rates_and_preds \
            .map(lambda r: (r[1][0] - r[1][1])**2).mean())
        errors[err] = evaluation_error
        err += 1
        if evaluation_error < min_evaluation_error:
            min_evaluation_error = evaluation_error
            best_rank = rank
    predictions = \
        generate_predictions(training_df, test_for_predict_df, best_rank) \
            .map(lambda r: ((r[0], r[1]), r[2])).cache()
    rates_and_preds = test_df \
        .map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    test_error = math.sqrt(
        rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    return best_rank, min_evaluation_error, test_error

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], \
        (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

def generate_new_user_recommendations(sqlContext,
    ratings, best_rank, currency_pair_rdd, new_user_id, new_user_ratings):
    new_user_ratings_df = \
        sqlContext.createDataFrame(new_user_ratings, ["user", "item", "rating"])
    complete_data_with_new_ratings_rdd = ratings.unionAll(new_user_ratings_df)
    new_user_ratings_ids = map(lambda x: x[1], new_user_ratings)
    new_user_unrated_movies_df = \
        (currency_pair_rdd
            .filter(lambda x: x[0] not in new_user_ratings_ids)
            .map(lambda x: (new_user_id, x[0]))).toDF(["user", "item"])
    return generate_predictions(
        complete_data_with_new_ratings_rdd,
        new_user_unrated_movies_df,
        best_rank)

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

if __name__ == '__main__':
    conf = SparkConf().setAppName('Currency Recommendations')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    currency_pair_dict = get_currency_pair_dict(sc)
    start_date, end_date = parse_dates('2015-05-01', '7d')
    es_rdd = get_es_rdd(sc, index='forex/transaction', date_field='date_closed',
        start_date=start_date, end_date=end_date) \
            .persist(StorageLevel.MEMORY_AND_DISK)

    currency_pair_rdd = get_es_rdd(sc, index='forex/currency_pair') \
        .map(lambda item:
            (item[1]['currency_pair_id'], item[1]['currency_pair']))

    ratings = get_ratings(sc, es_rdd, linear_combination, '2015-05-01', '7d') \
        .map(lambda item: (
            item[1]['provider_id'],
            currency_pair_dict[item[1]['currency_pair']],
            item[1]['rating'])).toDF(["user", "item", "rating"]).cache()

    best_rank, _, _ = compute_model_parameters(ratings)

    currency_pairs_with_ratings_rdd = (ratings
        .map(lambda x: (x[1], x[2])).groupByKey())
    currency_pair_id_with_avg_ratings_rdd = \
        currency_pairs_with_ratings_rdd.map(get_counts_and_averages)
    currency_pair_rating_counts_rdd = \
        currency_pair_id_with_avg_ratings_rdd.map(lambda x: (x[0], x[1][0]))

    new_user_id = 0
    new_user_ratings = [
        (0, 11, 9.9025095581621425),
        (0, 72, 9.9251950795087698),
        (0, 47, 9.9575823872693228),
        (0, 37, 9.8444613457560095)
    ]

    new_user_recommendations_df = generate_new_user_recommendations(sqlContext,
        ratings, best_rank, currency_pair_rdd, new_user_id, new_user_ratings)

    top_currency_pairs = get_top_currency_pairs_recommendation(
        new_user_recommendations_df, currency_pair_rdd, 100, 25)

    k = 1
    for r in top_currency_pairs:
        print str(k) + ' - ' + str(r)
        k += 1
