# etl/inspect_data_structure.py

from pyspark.sql.SparkSession

spark = (
    SparkSession.builder
    .appName("InspectDataStructures")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# ===============================================================
# 1️⃣  Inspect ENCODED TRAINING DATA
# ===============================================================
print("\n==================== ENCODED TRAINING DATA ====================")
try:
    df_train = spark.read.parquet("data/model_features_cleaned/")
    df_train.printSchema()
    df_train.show(5, truncate=False)
    print(f"[INFO] Training rows: {df_train.count()}")
except Exception as e:
    print("[ERROR] Could not read encoded training data:", e)


# ===============================================================
# 2️⃣  Inspect FUTURE FLIGHT DATA
# ===============================================================
print("\n==================== FUTURE FLIGHT DATA ====================")
try:
    df_future = spark.read.parquet("data/future_parquet/")
    df_future.printSchema()
    df_future.show(5, truncate=False)
    print(f"[INFO] Future rows: {df_future.count()}")
except Exception as e:
    print("[ERROR] Could not read future data:", e)


# ===============================================================
# 3️⃣  Compare column sets (optional check)
# ===============================================================
try:
    train_cols = set(df_train.columns)
    future_cols = set(df_future.columns)
    missing_in_future = train_cols - future_cols
    extra_in_future = future_cols - train_cols

    print("\n==================== COLUMN COMPARISON ====================")
    print(f"[INFO] Columns missing in future data: {missing_in_future or 'None'}")
    print(f"[INFO] Extra columns in future data: {extra_in_future or 'None'}")
except Exception as e:
    print("[ERROR] Could not compare columns:", e)

spark.stop()