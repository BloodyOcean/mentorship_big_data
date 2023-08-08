from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    spark = SparkSession.builder.master("local[1]") \
        .appName("Practice 1") \
        .config('spark.executor.memory', '4g') \
        .getOrCreate()

    # Employee dataframe
    emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
           (2, "Rose", 1, "2010", "20", "M", 4000),
           (3, "Williams", 1, "2010", "10", "M", 1000),
           (4, "Jones", 2, "2005", "10", "F", 2000),
           (5, "Brown", 2, "2010", "40", "", -1),
           (6, "Brown", 2, "2010", "50", "", -1)]

    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]

    emp_df = spark.createDataFrame(data=emp, schema=empColumns)

    # Department dataframe
    dept = [("Finance", 10, [1, 2, 3]),
            ("Marketing", 20, [1, 2, 3]),
            ("Sales", 30, [1, 2, 3]),
            ("IT", 40, [1, 2, 3])
            ]

    deptColumns = ["dept_name", "dept_id", "arr_id"]

    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

    emp_df.createTempView('Employee')
    deptDF.createTempView('Department')

    result_df = emp_df.join(deptDF, F.array_contains(F.col("arr_id"), F.col('emp_id')))
    result_df.show()

    # Test query using Spark SQL
    result_sql_df = spark.sql('SELECT * FROM Employee e LEFT JOIN Department d ON e.emp_dept_id = d.dept_id')
    result_sql_df.show()


if __name__ == '__main__':
    main()
