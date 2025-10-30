# scripts/modeling/train_spark_model.py

import os, sys, signal

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from xgboost.spark import SparkXGBRegressor
import time

# -------------------- Determine Correct Data Path --------------------
if os.path.exists("/opt/airflow/data"):
    DATA_DIR = "/opt/airflow/data"
else:
    DATA_DIR = "/app/data"

WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")
ENCODED_PATH = os.path.join(DATA_DIR, "model_encoded")
MODEL_PATH = os.path.join(DATA_DIR, "models/xgboost_flight_delay")

print(f"\n================== MODEL TRAINING DEBUG ==================")
print(f"DATA_DIR:      {DATA_DIR}")
print(f"WAREHOUSE:     {WAREHOUSE_PATH}")
print(f"ENCODED_PATH:  {ENCODED_PATH}")
print(f"MODEL_PATH:    {MODEL_PATH}")
print("==========================================================\n")

# ------------------- Spark session -------------------
spark = (
    SparkSession.builder
    .appName("FlightDelay-XGBoost")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "3g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.shuffle.partitions", "100")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Spark session started. Reading encoded data from: {ENCODED_PATH}")

# ------------------- Load encoded data -------------------
df = spark.read.parquet(ENCODED_PATH)
df.show(5)
row_count = df.count()
print(f"Loaded encoded data with {row_count} rows.")

# Optional: sample if dataset is huge (safety against OOM)
if row_count > 100_000:
    print(f"Large dataset detected ({row_count} rows). Sampling 50% for stability.")
    df = df.sample(fraction=0.5, seed=42)

# ------------------- Train/test split -------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training rows: {train_df.count()}, Test rows: {test_df.count()}")

# ------------------- Model definition -------------------
xgb = SparkXGBRegressor(
    features_col="features",
    label_col="arr_delay",
    objective="reg:squarederror",
    num_workers=2,
    n_estimators=150,
    max_depth=6,
    eta=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    tree_method="hist"
)

# ------------------- Training -------------------
try:
    print("Training model...")
    model = xgb.fit(train_df)
    print("Model training complete!")
except Exception as e:
    print(f"Training failed: {e}")
    spark.stop()
    raise

# ------------------- Predictions & Evaluation -------------------
try:
    preds = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(
        labelCol="arr_delay",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="arr_delay",
        predictionCol="prediction",
        metricName="r2"
    )

    rmse = evaluator_rmse.evaluate(preds)
    r2 = evaluator_r2.evaluate(preds)

    print(f"\nModel Performance:")
    print(f"RMSE = {rmse:.2f}")
    print(f"R²   = {r2:.3f}")

except Exception as e:
    print(f"Evaluation failed: {e}")
    print("Proceeding without metrics.")

# ------------------- Save model (with fallback) -------------------
print(f"\nSaving model to: {MODEL_PATH}")
try:
    model.write().overwrite().save(MODEL_PATH)
    print(f"Spark model successfully saved to {MODEL_PATH}")
except Exception as e:
    print(f"Spark model save failed ({e}). Saving native XGBoost fallback...")
    try:
        model.get_booster().save_model(os.path.join(MODEL_PATH, "xgb_native.json"))
        print(f"Native XGBoost model saved at {MODEL_PATH}/xgb_native.json")
    except Exception as inner_e:
        print(f"Native save also failed: {inner_e}")

# ------------------- Graceful Stop -------------------
try:
    spark.stop()
    print("Training pipeline finished successfully.")
except Exception as e:
    print(f"Spark stop warning (non-critical): {e}")

# Give JVM a moment to shut down gracefully
time.sleep(2)

# Try normal exit first (Airflow will see return code 0)
try:
    sys.exit(0)
except SystemExit:
    # If for some reason the JVM still blocks, kill hard
    os.kill(os.getpid(), signal.SIGKILL)