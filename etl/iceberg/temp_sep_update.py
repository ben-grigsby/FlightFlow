import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# ------------------ Path Setup ------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # goes up 3 levels
DATA_DIR = os.path.join(BASE_DIR, "data")

# ✅ FIX: zero-padded month directory
PARQUET_ROOT = os.path.join(DATA_DIR, "iceberg_parquet", "month=09")
WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")

print(f"Using PARQUET_ROOT={PARQUET_ROOT}")

# ------------------ Spark ------------------
spark = (
    SparkSession.builder
    .appName("ManualIcebergInsert_Sept")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

for day in range(23, 31):
    day_path = os.path.join(PARQUET_ROOT, f"day={day:02d}")
    print(f"Checking {day_path}")
    if not os.path.exists(day_path):
        print(f"⚠️ Missing: {day_path}")
        continue

    print(f"📥 Inserting {day_path}...")
    df = spark.read.parquet(day_path)
    df = df.withColumn("month", lit(9)).withColumn("day", lit(day))
    df.createOrReplaceTempView("new_data")

    spark.sql("""
        INSERT INTO local.flights
        SELECT * FROM new_data
    """)

print("✅ September data successfully added.")
spark.stop()