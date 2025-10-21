# etl/iceberg/update_iceberg_table.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

# ------------------ Path Setup ------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # go up 3 levels from this file
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET_ROOT = os.path.join(DATA_DIR, "iceberg_parquet")
WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")

# ------------------ Initialize Spark ------------------
spark = (
    SparkSession.builder
    .appName("DynamicIcebergUpdater")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------ Step 1: Find Current Max Partition ------------------
print("Fetching current max (month, day) from Iceberg table...")

max_part = spark.sql("""
    SELECT 
        MAX(day) AS max_day,
        MAX(month) AS max_month
    FROM local.flights
    WHERE month = (
        SELECT MAX(month)
        FROM local.flights
    )
""").collect()[0]

max_month = int(max_part["max_month"])
max_day   = int(max_part["max_day"])
print(f"Latest record in Iceberg: month={max_month}, day={max_day}")

# ------------------ Step 2: Traverse New Parquet Partitions ------------------
new_partitions_added = 0

for month_folder in sorted(os.listdir(PARQUET_ROOT)):
    if not month_folder.startswith("month="):
        continue

    month_val = int(month_folder.split("=")[1])
    month_path = os.path.join(PARQUET_ROOT, month_folder)

    for day_folder in sorted(os.listdir(month_path)):
        if not day_folder.startswith("day="):
            continue

        day_val = int(day_folder.split("=")[1])

        # Skip data already included
        if (month_val < max_month) or (month_val == max_month and day_val <= max_day):
            continue

        full_path = os.path.join(month_path, day_folder)
        print(f"Inserting data from {full_path}...")

        # Load and patch schema
        df = spark.read.parquet(full_path)
        df = df.withColumn("month", lit(month_val))
        df = df.withColumn("day", lit(day_val))

        df.createOrReplaceTempView("new_data")

        spark.sql("""
            INSERT INTO local.flights
            SELECT * FROM new_data
        """)

        new_partitions_added += 1

if new_partitions_added > 0:
    print(f"Iceberg table successfully updated with {new_partitions_added} new partitions.")
else:
    print("No new partitions found to add — Iceberg table is already up to date.")

spark.stop()