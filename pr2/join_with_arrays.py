from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from lib.lib import read_csv, elapse_time


@elapse_time
def process_job(spark):
    products_df = read_csv(spark, '../datasets/products.csv')
    order_products_df = read_csv(spark, '../datasets/order_products.csv')

    order_products_grouped_df = (order_products_df
                                 .select(F.col('order_id'), F.col('product_id'))
                                 .groupBy("order_id")
                                 .agg(F.collect_list("product_id").alias('array_products')))

    # Array from left side
    joined_array_df = (order_products_grouped_df
                       .join(products_df,
                             F.array_contains(F.col('array_products'), F.col('product_id'))))

    joined_array_df.show()


def main():

    spark = SparkSession.builder.master("local[1]") \
        .appName("Practice 2") \
        .config('spark.executor.memory', '4g') \
        .getOrCreate()

    process_job(spark)


if __name__ == '__main__':
    main()
