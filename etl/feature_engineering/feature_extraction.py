from pyspark.sql import SparkSession
import os

# -------------- Extraction features from iceberg table for ML analysis --------------

# -------------------- Determine Correct Data Path --------------------
if os.path.exists("/opt/airflow/data"):
    DATA_DIR = "/opt/airflow/data"
else:
    DATA_DIR = "/app/data"

WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")
FEATURES_OUTPUT_PATH = os.path.join(DATA_DIR, "model_features")

print(f"\n================== FEATURE EXTRACTION DEBUG ==================")
print(f"DATA_DIR:             {DATA_DIR}")
print(f"WAREHOUSE_PATH:       {WAREHOUSE_PATH}")
print(f"FEATURES_OUTPUT_PATH: {FEATURES_OUTPUT_PATH}")
print("=============================================================\n")

# -------------------- Initialize Spark --------------------
spark = (
    SparkSession.builder
    .appName("FeatureExtraction")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .getOrCreate()
)

df = spark.sql("""
    SELECT
        flight_date,
        airline_name AS airline,
        dept_airport_iata AS dep_airport,
        arr_airport_iata AS arr_airport,
        dept_timezone,
        arr_timezone,
        dept_scheduled,
        dept_estimated,
        arr_scheduled,
        dept_airport_delay,
        arr_delay,
        month,
        day
    FROM local.flights
    WHERE arr_delay IS NOT NULL AND NOT isnan(arr_delay)
""").sample(fraction=0.015, seed=42)

print(f"Extracted {df.count()} rows — saving to {FEATURES_OUTPUT_PATH} ...")

df.write.mode("overwrite").parquet(FEATURES_OUTPUT_PATH)
print(f"Successfully saved to {FEATURES_OUTPUT_PATH}")

spark.stop()
print("Feature extraction completed successfully.")