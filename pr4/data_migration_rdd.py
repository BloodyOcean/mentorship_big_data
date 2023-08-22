from pyspark.sql import SparkSession

from lib.lib import elapse_time


@elapse_time
def process_job(spark):
    # Create a SparkConf and SparkContext
    sc = spark.sparkContext

    # Create RDDs for 'a_data' and 'b_data'
    a_data = [(1, 1), (2, 1), (3, 1), (4, 3), (5, 2), (6, 3), (7, 5)]
    b_data = [(1, 1), (1, 2), (1, 3), (1, 4), (1, 5),
              (2, 1), (2, 2), (2, 3), (2, 4), (2, 5),
              (3, 1), (3, 2), (3, 3), (3, 4), (3, 5),
              (4, 3), (4, 1), (4, 2), (4, 4), (4, 5),
              (5, 2), (5, 1), (5, 4), (5, 3), (5, 5),
              (6, 3), (6, 1), (6, 2), (6, 4), (6, 5),
              (7, 5), (7, 1), (7, 2), (7, 3), (7, 4)]

    a_rdd = sc.parallelize(a_data)
    b_rdd = sc.parallelize(b_data)

    # Approach #1
    a_transformed_rdd = a_rdd.map(lambda x: ((x[0], x[1]), None))
    b_transformed_rdd = b_rdd.map(lambda x: ((x[0], x[1]), None))

    res_rdd = b_transformed_rdd\
        .subtractByKey(a_transformed_rdd)\
        .union(a_transformed_rdd)\
        .map(lambda x: (x[0][0], x[0][1]))\
        .groupByKey()\
        .mapValues(lambda values: list(values))

    print(res_rdd.collect())

    # Approach #2
    res_set_rdd = a_rdd\
        .union(b_rdd)\
        .groupByKey()\
        .mapValues(lambda values: list(set(values)))

    print(res_set_rdd.collect())


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Practice 4") \
        .getOrCreate()

    process_job(spark)
