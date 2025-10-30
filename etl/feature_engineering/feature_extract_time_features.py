# etl/feature_engineering/feature_extract_time_features.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col
import os

# -------------------- Determine Correct Data Path --------------------
if os.path.exists("/opt/airflow/data"):
    DATA_DIR = "/opt/airflow/data"
else:
    DATA_DIR = "/app/data"

INPUT_PATH = os.path.join(DATA_DIR, "model_features_cleaned")
OUTPUT_PATH = os.path.join(DATA_DIR, "model_features_time")

print(f"\n================== TIME FEATURE EXTRACTION DEBUG ==================")
print(f"DATA_DIR:     {DATA_DIR}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(f"OUTPUT_PATH:  {OUTPUT_PATH}")
print("===================================================================\n")

# -------------------- Initialize Spark --------------------
spark = (
    SparkSession.builder
    .appName("ExtractTimeFeatures")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Spark session started.")
print(f"Loading cleaned feature data from: {INPUT_PATH}")

# -------------------- Load Data --------------------
df = spark.read.parquet(INPUT_PATH)
print(f"Loaded {df.count()} rows.")

# -------------------- Extract Time Features --------------------
df = df.withColumn("dep_time", date_format(to_timestamp(col("dept_scheduled")), "HH:mm"))
df = df.withColumn("arr_time", date_format(to_timestamp(col("arr_scheduled")), "HH:mm"))
df = df.withColumn("dep_hour", date_format(to_timestamp(col("dept_scheduled")), "HH").cast("int"))
df = df.withColumn("arr_hour", date_format(to_timestamp(col("arr_scheduled")), "HH").cast("int"))

print("Extracted time-based features:")
df.select(
    "dept_scheduled", "dep_time", "dep_hour",
    "arr_scheduled", "arr_time", "arr_hour"
).show(5, truncate=False)

# -------------------- Save Updated Dataset --------------------
df.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Time-based features extracted and saved to {OUTPUT_PATH}")

spark.stop()
print("Feature extraction step complete.")