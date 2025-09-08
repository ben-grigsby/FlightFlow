# etl/silver/silver_load.py

import os, sys
from pyspark.sql import SparkSession

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

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

df.show(truncate=False)