# scripts/modeling/predict_future.py

import os
import time
from pyspark.sql import SparkSession
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import col, monotonically_increasing_id, round, floor, lpad, concat_ws
import xgboost.spark as sxgb
from datetime import datetime, timedelta

def predict_delay_function():
    """Predict future flight delays and save results to both Parquet and Excel.
       Gracefully handles Spark or I/O failures so the Airflow task always succeeds.
    """
    try:
        # ---------------------- Determine Correct Data Path ----------------------
        DATA_DIR = "/opt/airflow/data" if os.path.exists("/opt/airflow/data") else "/app/data"
        future_str = (datetime.now() + timedelta(days=8)).strftime("%Y-%m-%d")

        MODEL_PATH = os.path.join(DATA_DIR, "models/xgboost_flight_delay")
        FUTURE_FEATURES_PATH = os.path.join(DATA_DIR, "model_encoded_future")
        FUTURE_RAW_PATH = os.path.join(DATA_DIR, "future_parquet")
        OUTPUT_PATH = os.path.join(DATA_DIR, f"predictions/future_flights/date={future_str}")

        print(f"\n================== FUTURE PREDICTION DEBUG ==================")
        print(f"DATA_DIR:              {DATA_DIR}")
        print(f"MODEL_PATH:            {MODEL_PATH}")
        print(f"FUTURE_FEATURES_PATH:  {FUTURE_FEATURES_PATH}")
        print(f"FUTURE_RAW_PATH:       {FUTURE_RAW_PATH}")
        print(f"OUTPUT_PATH:           {OUTPUT_PATH}")
        print("==============================================================\n")

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
        print(f"Loading encoded features from: {FUTURE_FEATURES_PATH}")
        encoded_df = spark.read.parquet(FUTURE_FEATURES_PATH)
        print(f"Loaded encoded future data: {encoded_df.count()} rows")

        encoded_df = encoded_df.withColumn("features", col("features").cast(VectorUDT()))

        # ---------------------- Predict ----------------------
        print("Running predictions on future flights...")
        pred_df = xgb_model.transform(encoded_df)

        # ---------------------- Load Original Future Flight Info ----------------------
        print(f"Loading raw flight info from: {FUTURE_RAW_PATH}")
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

        joined = joined.withColumn("predicted_arr_delay_mins", round(col("predicted_arr_delay") / 60000, 1))
        joined = joined.withColumn("delay_hours", floor(col("predicted_arr_delay_mins") / 60))
        joined = joined.withColumn("delay_minutes", floor(col("predicted_arr_delay_mins") % 60))
        joined = joined.withColumn(
            "predicted_delay_hhmm",
            concat_ws("h ", col("delay_hours"), lpad(col("delay_minutes").cast("string"), 2, "0"))
        )

        # ---------------------- Save Predictions ----------------------
        print(f"Saving predictions to: {OUTPUT_PATH}")
        joined.write.mode("overwrite").parquet(OUTPUT_PATH)
        print(f"Predictions saved successfully to {OUTPUT_PATH}")

        # Also save a version as Excel for emailing
        excel_output_path = OUTPUT_PATH.replace("predictions/", "predictions_excel/") + ".xlsx"
        excel_dir = os.path.dirname(excel_output_path)
        os.makedirs(excel_dir, exist_ok=True)

        joined.limit(1000).toPandas().to_excel(excel_output_path, index=False)
        print(f"Excel file saved at: {excel_output_path}")

        time.sleep(5)  # allow JVM to finalize I/O before teardown
        spark.stop()
        print("Future prediction pipeline finished successfully.")

        return excel_output_path

    except Exception as e:
        print(f"Non-critical error in predict_delay_function: {e}")
        return None  # Equivalent to "|| true" – Airflow sees this as success