# etl/feature_engineergin/feature_cleaned.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from xgboost.spark import SparkXGBRegressor
import os

# -------------------- Determine Correct Data Path --------------------
if os.path.exists("/opt/airflow/data"):
    DATA_DIR = "/opt/airflow/data"
else:
    DATA_DIR = "/app/data"

WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")
FEATURES_PATH = os.path.join(DATA_DIR, "model_features")
CLEANED_PATH = os.path.join(DATA_DIR, "model_features_cleaned")

print(f"\n================== MODEL TRAINING DEBUG ==================")
print(f"DATA_DIR:       {DATA_DIR}")
print(f"WAREHOUSE_PATH: {WAREHOUSE_PATH}")
print(f"FEATURES_PATH:  {FEATURES_PATH}")
print(f"CLEANED_PATH:   {CLEANED_PATH}")
print("==========================================================\n")

# ------------------- Spark session with Iceberg + XGBoost -------------------
spark = (
    SparkSession.builder
    .appName("FlightDelay-XGBoost")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "6g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ------------------- Load feature data -------------------
df = spark.read.parquet(FEATURES_PATH)
df.printSchema()
df.show(5)
num_rows = df.count()
print(f"Total rows: {num_rows}")

# ------------------- Feature selection -------------------
feature_cols = [
    "airline",
    "dep_airport",
    "arr_airport",
    "dept_airport_delay",
    "month",
    "day"
]
target_col = "arr_delay"

df = (
    df
    .withColumn("dept_airport_delay", col("dept_airport_delay").cast("double"))
    .withColumn("arr_delay", col("arr_delay").cast("double"))
)

# ------------------- Save cleaned data -------------------
df.write.mode("overwrite").parquet(CLEANED_PATH)
print(f"Cleaned dataset saved to {CLEANED_PATH}")

spark.stop()
print("Feature cleaning complete, ready for encoding and training.")