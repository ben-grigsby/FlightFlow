# scripts/modeling/predict_future.py

import os
from pyspark.sql import SparkSession
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import col, monotonically_increasing_id, round, floor, lpad, concat_ws
import xgboost.spark as sxgb


# ---------------------- Path Setup ----------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

MODEL_PATH = os.path.join(DATA_DIR, "models/xgboost_flight_delay")
FUTURE_FEATURES_PATH = os.path.join(DATA_DIR, "model_encoded_future/")
FUTURE_RAW_PATH = os.path.join(DATA_DIR, "future_parquet/")
OUTPUT_PATH = os.path.join(DATA_DIR, "predictions/future_flights/")


# ---------------------- Spark Session ----------------------
spark = (
    SparkSession.builder
    .appName("PredictFutureDelays")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------- Load Trained XGBoost Model ----------------------
print(f"Loading trained regression model from: {MODEL_PATH}")
xgb_model = sxgb.SparkXGBRegressorModel.load(MODEL_PATH)

# ---------------------- Load Encoded Future Data ----------------------
encoded_df = spark.read.parquet(FUTURE_FEATURES_PATH)
print(f"Loaded encoded future data: {encoded_df.count()} rows")

# Ensure features column has correct type
encoded_df = encoded_df.withColumn("features", col("features").cast(VectorUDT()))

# ---------------------- Predict ----------------------
print("Running predictions on future flights...")
pred_df = xgb_model.transform(encoded_df)

# ---------------------- Load Original Future Flight Info ----------------------
raw_future_df = spark.read.parquet(FUTURE_RAW_PATH)
print(f"Loaded raw flight info: {raw_future_df.count()} rows")

# ---------------------- Combine Predictions ----------------------
pred_df = pred_df.withColumn("row_id", monotonically_increasing_id())
raw_future_df = raw_future_df.withColumn("row_id", monotonically_increasing_id())

joined = (
    raw_future_df.join(pred_df.select("row_id", "prediction"), on="row_id", how="inner")
    .drop("row_id")
    .withColumnRenamed("prediction", "predicted_arr_delay")
)

# Convert from milliseconds → minutes
joined = joined.withColumn("predicted_arr_delay_mins", round(col("predicted_arr_delay") / 60000, 1))

# Convert to hours:minutes for readability
joined = joined.withColumn("delay_hours", floor(col("predicted_arr_delay_mins") / 60))
joined = joined.withColumn("delay_minutes", floor(col("predicted_arr_delay_mins") % 60))

joined = joined.withColumn(
    "predicted_delay_hhmm",
    concat_ws("h ", col("delay_hours"), lpad(col("delay_minutes").cast("string"), 2, "0"))
)

# ---------------------- Save Predictions ----------------------
joined.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Predictions saved to: {OUTPUT_PATH}")

# ---------------------- Show Sample Output ----------------------
joined.select(
    "airline", "dep_airport", "arr_airport", "dep_hour", "arr_hour",
    "predicted_arr_delay_mins", "predicted_delay_hhmm"
).orderBy(col("predicted_arr_delay").desc()).show(10, truncate=False)

spark.stop()