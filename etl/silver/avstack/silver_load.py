# etl/silver/avstack/silver_load.py

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, length, regexp_replace

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from etl.silver.avstack.silver_flight_info import (
    clean_transform_flight_info
)

from etl.silver.avstack.silver_dept_info import (
    clean_transform_dept_info
)

from etl.silver.avstack.silver_arr_info import (
    clean_transform_arr_info
)

# ==================================================================
# Setup
# ==================================================================

spark = SparkSession.builder \
    .appName("Bronze to Silver") \
    .config("spark.jars", "jars/postgresql-42.7.7.jar") \
    .getOrCreate()


df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/flight_db") \
    .option("dbtable", "avstack.bronze_info") \
    .option("user", "user") \
    .option("password", "pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df = df.filter(
    col("dept_timezone").isNotNull() &
    col("arr_timezone").isNotNull() &
    col("dept_airport").isNotNull() &
    col("arr_airport").isNotNull() &
    col("airline_iata").isNotNull()
)

latest_processing_time = 0

# ==================================================================
# Action
# ==================================================================


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Deal with the incremental variable of latest_processing_time
# when I am figuring out Airflow
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


clean_transform_flight_info(df, "user", "pass", "5432", "flight_db", latest_processing_time)
clean_transform_dept_info(df, "user", "pass", "5432", "flight_db", latest_processing_time)
clean_transform_arr_info(df, "user", "pass", "5432", "flight_db", latest_processing_time)