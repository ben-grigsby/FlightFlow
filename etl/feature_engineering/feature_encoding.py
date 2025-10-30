# etl/feature_encoding.py

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import lower, col, to_timestamp, date_format
import os

# ---------------------- Determine Correct Data Path ----------------------
if os.path.exists("/opt/airflow/data"):
    DATA_DIR = "/opt/airflow/data"
else:
    DATA_DIR = "/app/data"

WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")
INPUT_PATH = os.path.join(DATA_DIR, "model_features_time")  # input from previous step
MODEL_PATH = os.path.join(DATA_DIR, "models/encoding_pipeline")
OUTPUT_PATH = os.path.join(DATA_DIR, "model_encoded")

print(f"\n================== FEATURE ENCODING DEBUG ==================")
print(f"DATA_DIR:      {DATA_DIR}")
print(f"WAREHOUSE:     {WAREHOUSE_PATH}")
print(f"INPUT_PATH:    {INPUT_PATH}")
print(f"MODEL_PATH:    {MODEL_PATH}")
print(f"OUTPUT_PATH:   {OUTPUT_PATH}")
print("============================================================\n")

# ---------------------- Spark Session ----------------------
spark = (
    SparkSession.builder
    .appName("FeatureEncoding")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "150")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Spark session started. Reading base features from {INPUT_PATH}")

# ---------------------- Load Base Features ----------------------
df = spark.read.parquet(INPUT_PATH)
print(f"Loaded {df.count()} rows.")

# --- Extract departure and arrival hour from ISO timestamps ---
df = df.withColumn("dep_hour", date_format(to_timestamp(col("dept_scheduled")), "HH").cast("int"))
df = df.withColumn("arr_hour", date_format(to_timestamp(col("arr_scheduled")), "HH").cast("int"))

# ---------------------- Feature Definitions ----------------------
categorical_cols = ["airline", "dep_airport", "arr_airport"]
numeric_cols = ["month", "day", "dep_hour", "arr_hour"]
target_col = "arr_delay"

# Lowercase all categorical string columns
for c in categorical_cols:
    df = df.withColumn(c, lower(col(c)))

# ---------------------- Encoding ----------------------
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical_cols]

assembler_inputs = [f"{c}_vec" for c in categorical_cols] + numeric_cols
assembler = VectorAssembler(
    inputCols=assembler_inputs,
    outputCol="features",
    handleInvalid="keep"
)

# ---------------------- Pipeline ----------------------
pipeline = Pipeline(stages=indexers + encoders + [assembler])
encoded_model = pipeline.fit(df)
encoded_df = encoded_model.transform(df)

# ---------------------- Save the Fitted Pipeline ----------------------
encoded_model.write().overwrite().save(MODEL_PATH)
print(f"Encoding pipeline saved to {MODEL_PATH}")

# ---------------------- Save Encoded Data ----------------------
encoded_df.select("features", target_col).write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Feature encoding complete — saved to {OUTPUT_PATH}")

if __name__ == '__main__':
    encoded_df.select("features", target_col, "dep_hour", "arr_hour").show(5, truncate=False)

spark.stop()
print("Feature encoding process finished successfully.")