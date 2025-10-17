# etl/feature_engineering/feature_extract_time_features.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col

spark = (
    SparkSession.builder
    .appName("ExtractTimeFeatures")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

df = spark.read.parquet("data/model_features_cleaned/")

# --- Extract time features ---
df = df.withColumn("dep_time", date_format(to_timestamp(col("dept_scheduled")), "HH:mm"))
df = df.withColumn("arr_time", date_format(to_timestamp(col("arr_scheduled")), "HH:mm"))
df = df.withColumn("dep_hour", date_format(to_timestamp(col("dept_scheduled")), "HH").cast("int"))
df = df.withColumn("arr_hour", date_format(to_timestamp(col("arr_scheduled")), "HH").cast("int"))

# --- Show quick preview ---
df.select(
    "dept_scheduled", "dep_time", "dep_hour",
    "arr_scheduled", "arr_time", "arr_hour"
).show(5, truncate=False)

# --- Save updated version for encoding ---
output_path = "data/model_features_time/"
df.write.mode("overwrite").parquet(output_path)

print(f"Time-based features extracted and saved to {output_path}")