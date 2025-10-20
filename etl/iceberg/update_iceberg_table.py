# etl/iceberg/update_iceberg_table.py

import os

from pyspark.sql import SparkSession

# ---------------------- Path Setup ----------------------
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET_PATH = os.path.join(DATA_DIR, "iceberg_parquet")

# ---------------------- Spark Session ----------------------
spark = (
    SparkSession.builder
    .appName("UpdateIcebergTable")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "150")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", os.path.join(DATA_DIR, "iceberg_warehouse"))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Spark session initialized with Iceberg support")

# ---------------------- Load Parquet Data ----------------------
df = spark.read.parquet(PARQUET_PATH)
print(f"Loaded {df.count()} records from {PARQUET_PATH}")

# ---------------------- Append to Iceberg Table ----------------------
print("Appending data to Iceberg table local.flights...")
df.writeTo("local.flights").append()

print("Data successfully appended to local.flights")

spark.stop()
print("Update complete and Spark stopped.")