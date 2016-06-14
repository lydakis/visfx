from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.ml.recommendation import ALS
from elasticsearch_interface import get_es_rdd, save_es_rdd, \
    get_currency_pair_dict
from utils import parse_range, parse_dates
import math


conf = SparkConf().setAppName('Currency Recommendations')
sc = SparkContext(conf=conf)

start_date, end_date = parse_dates('2015-05-01', '1d')
currency_pair_dict = get_currency_pair_dict(sc)
ratings = get_es_rdd(
    sc, index='forex/rating', date_field='start_date',
    start_date=start_date, end_date=end_date) \
        .map(lambda item: (
            item[1]['provider_id'],
            currency_pair_dict[item[1]['currency_pair']],
            item[1]['rating'])).toDF(["user", "item", "rating"])

training_RDD, validation_RDD, test_RDD = \
    ratings.randomSplit([6.0, 2.0, 2.0], seed=0L)
validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

seed = 5L
iterations = 10
regularization_parameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

min_error = float('inf')
best_rank = -1
best_iteration = -1

for rank in ranks:
    model = ALS.train(ratings, rank, seed=seed, iterations=iterations,
        lambda_=regularization_parameter)
    predictions = model.predictAll(validation_for_predict_RDD).map(
        lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = validation_RDD.map(
        lambda r: ((r[0], r[1]), r[2])).join(predictions)
    error = math.sqrt(rates_and_preds.map(
        lambda r: (r[1][0] - r[1][1])**2).mean())
    errors[err] = error
    err += 1
    print 'For rank %s the RMSE is %s' % (rank, error)
    if error < min_error:
        min_error = error
        best_rank = rank

print 'The best model was trained with rank %s' % best_rank
