import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from lib.lib import read_csv


def process_job(spark):
    products_df = read_csv(spark, '../datasets/products.csv')
    order_products_df = read_csv(spark, '../datasets/order_products.csv')

    order_products_id_df = (order_products_df.withColumn("id", F.monotonically_increasing_id()))

    order_products_grouped_df = (order_products_id_df
                                 .withColumn('product_id_concat', F.concat_ws('_', F.col('product_id'), F.col('id')))
                                 .select(F.col('order_id'), F.col('product_id_concat'))
                                 .groupBy("order_id")
                                 .agg(F.collect_list("product_id_concat").alias('array_products')))

    exploded_df = (order_products_grouped_df
                   .select(F.col("order_id"), F.explode(F.col("array_products")).alias('product_id_concat'))
                   .withColumn('id', F.split(F.col('product_id_concat'), '_').getItem(1))
                   .withColumn('product_id', F.split(F.col('product_id_concat'), '_').getItem(0))
                   .join(order_products_id_df, 'id'))

    # Array from left side
    joined_array_df = (exploded_df.join(products_df, "product_id", 'left'))

    joined_array_df.show()


def main():
    spark = SparkSession.builder\
        .master("local[1]") \
        .appName("Practice 2") \
        .config('spark.executor.memory', '4g') \
        .getOrCreate()

    start_time = time.time()

    process_job(spark)

    print(f"Time consumed: {time.time() - start_time}")


if __name__ == '__main__':
    main()
