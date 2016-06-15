from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.ml.recommendation import ALS
from elasticsearch_interface import get_es_rdd, save_es_rdd, \
    get_currency_pair_dict
from ratings import get_ratings
from utils import parse_range, parse_dates
import math


conf = SparkConf().setAppName('Currency Recommendations')
sc = SparkContext(conf=conf)

# start_date, end_date = parse_dates('2015-05-01', '1d')
currency_pair_dict = get_currency_pair_dict(sc)
# ratings = get_es_rdd(
#     sc, index='forex/rating', date_field='start_date',
#     start_date=start_date, end_date=end_date) \
#         .map(lambda item: (
#             item[1]['provider_id'],
#             currency_pair_dict[item[1]['currency_pair']],
#             item[1]['rating'])).toDF(["user", "item", "rating"])

start_date, end_date = parse_dates('2015-05-01', '7d')
es_rdd = get_es_rdd(sc, index='forex/transaction', date_field='date_closed',
    start_date=start_date, end_date=end_date) \
        .persist(StorageLevel.MEMORY_AND_DISK)

ratings = get_ratings(sc, es_rdd, '2015-05-01', '7d') \
    .map(lambda item: (
        item[1]['provider_id'],
        currency_pair_dict[item[1]['currency_pair']],
        item[1]['rating'])).toDF(["user", "item", "rating"]).cache()

currency_pair_rdd = get_es_rdd(sc, index='forex/currency_pair') \
    .map(lambda item: (item[1]['currency_pair_id'], item[1]['currency_pair']))

training_RDD, validation_RDD, test_RDD = \
    ratings.randomSplit([6.0, 2.0, 2.0], seed=0L)
validation_for_predict_RDD = validation_RDD.drop('rating')
test_for_predict_RDD = test_RDD.drop('rating')

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
    als = ALS(rank=rank, seed=seed, maxIter=iterations)
    model = als.fit(training_RDD)
    predictions = model.transform(validation_for_predict_RDD).dropna().map(
        lambda r: ((r[0], r[1]), r[2])).cache()
    rates_and_preds = validation_RDD.map(
        lambda r: ((r[0], r[1]), r[2])).join(predictions)
    error = math.sqrt(rates_and_preds.map(
        lambda r: (r[1][0] - r[1][1])**2).mean())
    errors[err] = error
    err += 1
    if error < min_error:
        min_error = error
        best_rank = rank

als = ALS(rank=best_rank, seed=seed, maxIter=iterations)
model = als.fit(training_RDD)
predictions = model.transform(test_for_predict_RDD).dropna().map(
    lambda r: ((r[0], r[1]), r[2])).cache()
rates_and_preds = test_RDD.map(
    lambda r: ((r[0], r[1]), r[2])).join(predictions)

error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

print 'For testing data the RMSE is %s' % (error)

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], \
        (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

movie_ID_with_ratings_RDD = (ratings
    .map(lambda x: (x[1], x[2])).groupByKey())
movie_ID_with_avg_ratings_RDD = \
    movie_ID_with_ratings_RDD.map(get_counts_and_averages)
movie_rating_counts_RDD = \
    movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

new_user_ID = 0

new_user_ratings = [
    (0, 11, -0.0014068669533029532), 
    (0, 72, -0.00027848655450174377),
    (0, 47, -0.00024834411584986248),
    (0, 37, -0.00063265862411232381)
]

new_user_ratings_RDD = \
    sqlContext.createDataFrame(new_user_ratings, ["user", "item", "rating"])

complete_data_with_new_ratings_RDD = ratings.unionAll(new_user_ratings_RDD)

from time import time

t0 = time()
als = ALS(rank=best_rank, seed=seed, maxIter=iterations)
new_ratings_model = als.fit(complete_data_with_new_ratings_RDD)
tt = time() - t0

print "New model trained in %s seconds" % round(tt,3)

new_user_ratings_ids = map(lambda x: x[1], new_user_ratings)
new_user_unrated_movies_RDD = \
    (currency_pair_rdd
        .filter(lambda x: x[0] not in new_user_ratings_ids)
        .map(lambda x: (new_user_ID, x[0]))).toDF(["user", "item"])

new_user_recommendations_RDD = \
    new_ratings_model.transform(new_user_unrated_movies_RDD).dropna()

new_user_recommendations_rating_RDD = \
    new_user_recommendations_RDD.map(lambda x: (int(x[1]), x[2]))

new_user_recommendations_rating_title_and_count_RDD = \
    sc.parallelize(new_user_recommendations_rating_RDD.collect()) \
        .join(currency_pair_rdd) \
        .join(movie_rating_counts_RDD)

new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_title_and_count_RDD \
        .map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

top_currency_pairs = \
    new_user_recommendations_rating_title_and_count_RDD \
    .filter(lambda r: r[2]>=25) \
    .takeOrdered(5, key=lambda x: -x[1])

k = 1
for r in top_currency_pairs:
    print str(k) + ' - ' + str(r)
    k += 1
