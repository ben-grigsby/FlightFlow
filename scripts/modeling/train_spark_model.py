# etl/train_spark_model.py

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from xgboost.spark import SparkXGBRegressor

# ------------------- Spark session -------------------
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

# ------------------- Load encoded data -------------------
df = spark.read.parquet("data/model_encoded/")
df.show(5)
print(f"Loaded encoded data with {df.count()} rows.")

# ------------------- Train/test split -------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training rows: {train_df.count()}, Test rows: {test_df.count()}")

# ------------------- Model definition -------------------
xgb = SparkXGBRegressor(
    features_col="features",
    label_col="arr_delay",
    objective="reg:squarederror",
    num_workers=4,
    n_estimators=150,
    max_depth=6,
    eta=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    tree_method="hist"
)

# ------------------- Training -------------------
print("Training model...")
model = xgb.fit(train_df)
print("Model training complete!")

# ------------------- Predictions & Evaluation -------------------
preds = model.transform(test_df)

evaluator = RegressionEvaluator(
    labelCol="arr_delay",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(preds)
r2_evaluator = RegressionEvaluator(
    labelCol="arr_delay",
    predictionCol="prediction",
    metricName="r2"
)
r2 = r2_evaluator.evaluate(preds)

df.select("arr_delay").summary("min", "max", "mean").show()

print(f"\nModel Performance:")
print(f"RMSE = {rmse:.2f}")
print(f"R²   = {r2:.3f}")

# ------------------- Save model -------------------
model_output_path = "data/models/xgboost_flight_delay"
print(f"\nSaving model to: {model_output_path}")
model.write().overwrite().save(model_output_path)

print("Model successfully saved!")
spark.stop()