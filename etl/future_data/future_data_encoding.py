# etl/future_data/future_data_encoding.py

import os 

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel 


# ---------------------- Path Setup ----------------------
# Works for both local and Airflow environments
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # goes up 3 levels to project root
DATA_DIR = os.path.join(BASE_DIR, "data")

FUTURE_PARQUET_PATH = os.path.join(DATA_DIR, "future_parquet/")
ENCODING_PIPELINE_PATH = os.path.join(DATA_DIR, "models/encoding_pipeline")
ENCODED_OUTPUT_PATH = os.path.join(DATA_DIR, "model_encoded_future/")

# --------------- Spark Session ---------------
spark = (
    SparkSession.builder
    .appName("FutureFeatureEncoding")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)

# --------------- Load Future Data ---------------
future_df = spark.read.parquet(FUTURE_PARQUET_PATH)
print(f"Loaded {future_df.count()} future flights")

# --------------- Load Saved Encoding Pipeline ---------------
pipeline_model = PipelineModel.load(ENCODING_PIPELINE_PATH)
print("Loaded encoding pipeline")

# --------------- Transform Future Data ---------------
encoded_future_df = pipeline_model.transform(future_df)

encoded_future_df.select("features").show(5, truncate=False)
print(f"Encoded {encoded_future_df.count()} future flights")

# --------------- Save Encoded Future Data ---------------
encoded_future_df.select("features").write.mode("overwrite").parquet(ENCODED_OUTPUT_PATH)
print(f"Saved encoded future features to {ENCODED_OUTPUT_PATH}")

spark.stop()