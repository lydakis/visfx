from pyspark.mllib.recommendation import ALS, Rating
from elasticsearch_interface import es_read_conf, es_write_conf, \
    get_es_rdd, save_es_rdd
import hashlib

if __name__ == '__main__':
    data = get_es_rdd('forex/rating')
    ratings = data.map(lambda item: Rating(
        int(item[1]['provider_id']),
        int(hashlib.sha1(item[1]['currency_pair']).hexdigest(), 16) % (10 ** 8),
        float(item[1]['rating']))).collect()
    rank = 10
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations)
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))
