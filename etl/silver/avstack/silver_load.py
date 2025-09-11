# etl/silver/avstack/silver_load.py

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

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
# Setup/Function
# ==================================================================

def get_latest_created_at(spark, table_name, jdbc_url, user, password):
    try:
        silver_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        max_ts = silver_df.selectExpr("max(created_at) as max_ts").collect()[0]['max_ts']
        return max_ts if max_ts else "1970-01-01 00:00:00"
    except:
        # Table may not exist yet or be empty
        return "1970-01-01 00:00:00"
    


def load_silver():
    print("[INFO] Start to load data into silver tables.")
    spark = SparkSession.builder \
        .appName("Bronze to Silver") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar") \
        .getOrCreate()
    
    latest_processing_time = get_latest_created_at(spark, 'avstack.silver_flight_info', "jdbc:postgresql://postgres:5432/flight_db", "user", "pass")

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flight_db") \
        .option("dbtable", "avstack.bronze_info") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"[DEBUG] Latest processing time: {latest_processing_time}")

    df = df.filter(
        (
        col("dept_timezone").isNotNull() &
        col("arr_timezone").isNotNull() &
        col("dept_airport").isNotNull() &
        col("arr_airport").isNotNull() &
        col("airline_iata").isNotNull()
        ) &
        (col("created_at") > to_timestamp(lit(latest_processing_time)))
    )

    if df.rdd.isEmpty():
        print("No new data to process.")
        return 

    # ==================================================================
    # Action
    # ==================================================================


    print("[INFO] Loading data into silver_flight_info")
    clean_transform_flight_info(df, "user", "pass", "5432", "flight_db", latest_processing_time)
    print("[INFO] Successfully loaded data into silver_flight_info")

    print("[INFO] Loading data into silver_dept_info")
    clean_transform_dept_info(df, "user", "pass", "5432", "flight_db", latest_processing_time)
    print("[INFO] Successfully loaded data into silver_dept_info")

    print("[INFO] Loading data into silver_arr_info")
    clean_transform_arr_info(df, "user", "pass", "5432", "flight_db", latest_processing_time)
    print("[INFO] Successfully loaded data into silver_arr_info")