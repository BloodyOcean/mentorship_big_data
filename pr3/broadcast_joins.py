from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from lib.lib import read_csv, elapse_time


@elapse_time
def process_job_implicit(spark):
    # Enable broadcast for small DataFrame
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)

    departments_df = read_csv(spark, '../datasets/departments.csv')  # We are going to use this df as broadcast df
    products_df = read_csv(spark, '../datasets/products.csv')

    result_df = products_df.join(departments_df, 'department_id')
    result_df.show()


@elapse_time
def process_job_explicit(spark):
    departments_df = read_csv(spark, '../datasets/departments.csv')
    products_df = read_csv(spark, '../datasets/products.csv')

    result_df = products_df.join(broadcast(departments_df), 'department_id')
    result_df.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Practice 3") \
        .config('spark.executor.memory', '4g') \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    process_job_explicit(spark)
    process_job_implicit(spark)

    input("Press enter to terminate")
