from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from lib.lib import elapse_time


@elapse_time
def process_job(spark):
    window = Window.partitionBy(F.col('order_id')).orderBy('product_id')

    data = [(1, 1),
            (1, 1),
            (1, 1),
            (1, 3),
            (2, 2),
            (2, 3),
            (2, 5)]

    schema = ['order_id', 'product_id']

    df = spark.createDataFrame(data, schema)

    window_res_df = df\
        .withColumn('max_pr', F.max('product_id').over(window))\
        .withColumn('arr_pr', F.collect_list('product_id').over(window))\
        .withColumn('rn', F.row_number().over(window))\
        .withColumn('dense_rank', F.dense_rank().over(window))\
        .withColumn('rank', F.rank().over(window))

    window_res_df.show()


if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[1]")\
        .appName("Practice 4")\
        .getOrCreate()

    process_job(spark)
