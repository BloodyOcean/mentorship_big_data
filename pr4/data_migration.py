from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from lib.lib import elapse_time


@elapse_time
def process_job(spark):
    # Table A(has customer_id(string) PK, instagram_id(string) unique)
    # and table B(has customer_id(string), and different id's(string) One to many)
    # Add all the id's from B to A but without duplicates (remove duplicates before aggregation)
    # Try not to use array_distinct()
    # Aggregate list of id's from A by PK
    # Try using RDD and DF API
    # Try different approaches avoiding duplicates (one via window func)

    a_data = [(1, 1),
              (2, 1),
              (3, 1),
              (4, 3),
              (5, 2),
              (6, 3),
              (7, 5)]

    a_schema = ['user_id', 'social_id']
    a_df = spark.createDataFrame(a_data, a_schema)

    b_data = [(1, 1), (1, 2), (1, 3), (1, 4), (1, 5),
              (2, 1), (2, 2), (2, 3), (2, 4), (2, 5),
              (3, 1), (3, 2), (3, 3), (3, 4), (3, 5),
              (4, 3), (4, 1), (4, 2), (4, 4), (4, 5),
              (5, 2), (5, 1), (5, 4), (5, 3), (5, 5),
              (6, 3), (6, 1), (6, 2), (6, 4), (6, 5),
              (7, 5), (7, 1), (7, 2), (7, 3), (7, 4)]

    b_schema = ['user_id', 'social_id']
    b_df = spark.createDataFrame(b_data, b_schema)

    res_df = b_df\
        .join(a_df, ['user_id', 'social_id'], 'left_anti')\
        .union(a_df)\
        .groupBy(F.col('user_id'))\
        .agg(F.collect_list(F.col('social_id')).alias('social_list'))

    res_df.show()

    window = Window.partitionBy('user_id', 'social_id').orderBy('social_id')

    windowed_res_df = a_df\
        .union(b_df)\
        .withColumn('rn', F.row_number().over(window))\
        .filter(F.col('rn') == 1)\
        .drop('rn')\
        .groupBy(F.col('user_id'))\
        .agg(F.collect_list(F.col('social_id')).alias('social_list'))

    windowed_res_df.show()

    # We use count to reset GroupedData into DataFrame
    grouped_df = a_df\
        .union(b_df)\
        .groupBy('user_id', 'social_id')\
        .count()\
        .drop('count')\
        .groupBy(F.col('user_id'))\
        .agg(F.collect_list(F.col('social_id')).alias('social_list'))

    grouped_df.show()

    # As spark union doesn't remove duplicates, we can utilize sql union statement
    a_df.createTempView("A")
    b_df.createTempView("B")
    res_sql_df = (spark.sql("SELECT * FROM A UNION SELECT * FROM B")
                  .groupBy(F.col('user_id'))
                  .agg(F.collect_list(F.col('social_id')).alias('social_list')))

    res_sql_df.show()

    res_set_df = (a_df.union(b_df)
                  .groupBy(F.col('user_id'))
                  .agg(F.collect_set(F.col('social_id')).alias('social_list')))

    res_set_df.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Practice 4") \
        .getOrCreate()

    process_job(spark)
