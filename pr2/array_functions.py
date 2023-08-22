from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    spark = SparkSession.builder.master("local[1]") \
        .appName("Practice 2") \
        .getOrCreate()

    # Employee dataframe
    emp = [(1, "Smith", -1, "2018", "10", "M", 3000, [1, 2, 2, 3, 3, 4]),
           (2, "Rose", 1, "2010", "20", "M", 4000, [1, 1, 1, 2, 3]),
           (3, "Williams", 1, "2010", "10", "M", 1000, [1, 5, None, None, 5, 5, 4]),
           (4, "Jones", 2, "2005", "10", "F", 2000, [5, 4, 3, 2, 1]),
           (5, "Brown", 2, "2010", "40", "", -1, [0, 0, 0, 0, 0]),
           (6, "Brown", 2, "2010", "50", "", -1, [1, 2, 1, 2, 1, 2]),
           (7, "Foo", 2, "2010", "50", "", -1, [None]),
           (8, "Bar", 2, "2010", "50", "", -1, None),
           (9, "Baz", 2, "2010", "50", "", -1, [])]

    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary", "array_vals"]

    products_data = [(1, "1|2|3"),
                     (2, "1|5|5"),
                     (3, "0"),
                     (4, "1|2"),
                     (5, "1|2|3|4|5")]

    products_schema = ["id", "products_id"]

    bundle_df = spark.createDataFrame(data=products_data, schema=products_schema)

    emp_df = spark.createDataFrame(data=emp, schema=empColumns)

    arr_union_df = (emp_df
                    .withColumn('arr_col', F.array(F.lit(1)))
                    .withColumn('unioned_arr', F.array_union(F.col('arr_col'), F.col('array_vals')))
                    .withColumn('unioned_arr_reversed', F.array_union(F.col('array_vals'), F.col('arr_col'))))

    arr_union_df.show()

    arr_emp_df = (emp_df
                  .withColumn("array_vals", F.col('array_vals').cast("array<string>"))
                  .select(F.array_join(F.array_sort(F.col('array_vals')), '|').alias('sorted_arr'))
                  .join(bundle_df, F.col("products_id") == F.col('sorted_arr'), 'inner'))

    arr_emp_df.show()


if __name__ == '__main__':
    main()
