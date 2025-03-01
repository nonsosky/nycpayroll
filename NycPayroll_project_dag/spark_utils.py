from pyspark.sql import SparkSession

#Initialize Spark Session
def getSparkSession():
    return SparkSession.builder \
        .appName("NYC Payroll ETL") \
        .config("spark.jars", "/Nyc_Payroll_dag/postgresql-42.7.4.jar") \
        .getOrCreate()