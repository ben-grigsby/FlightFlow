# etl/train_spark_model.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from xgboost.spark import SparkXGBRegressor

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
    .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


df = spark.read.parquet("data/model_features/")
df.printSchema()
df.show(5)
num_rows = df.count()
print(f"Total rows: {num_rows}")


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

# Save cleaned numeric data back to disk
df.write.mode("overwrite").parquet("data/model_features_cleaned/")
print("Cleaned dataset saved to data/model_features_cleaned/")