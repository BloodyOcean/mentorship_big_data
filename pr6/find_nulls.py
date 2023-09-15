from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from lib.lib import elapse_time


@elapse_time
def process_job(spark):
    # 1, 2, 3, 5, 6, 8, 9
    df = spark.createDataFrame(
        [(1, "A", "t10", "t10"),
         (2, "A", "t10", "t11"),
         (3, "A", "t10", "t12"),
         (4, "b", "t10", "t13"),
         (5, "b", "t10", None),
         (6, "b", "t10", None),
         (7, "b", "t10", "t10"),
         (8, "c", "t10", None),
         (9, "c", "t10", None),
         ], ["sap_product_code", "fs", "start_time", "delete_time"]
    )

    # Get full deleted items
    df_full_delete = (df.withColumn("mark",
                                    F.when(F.col("delete_time").isNull(), F.lit(0)).otherwise(F.lit(1)))
                      .withColumn("mark_sum",
                                  F.sum(F.col("mark")).over(Window.partitionBy(F.col("fs"))))
                      .withColumn("overall_count",
                                  F.count(F.col("sap_product_code")).over(Window.partitionBy(F.col("fs"))))
                      .where(F.col("mark_sum") == F.col("overall_count")))

    df_full_delete.show()

    df_partial_delete = (df.where(F.col("delete_time").isNull()))

    df_res = df_full_delete.unionByName(df_partial_delete, allowMissingColumns=True)

    df_res.show()


def main():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Practice 6") \
        .getOrCreate()

    process_job(spark)


if __name__ == '__main__':
    main()
