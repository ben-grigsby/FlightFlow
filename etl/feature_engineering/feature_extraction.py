# etl/feature_extraction.py

from pyspark.sql import SparkSession

# -------------- Extraction features from iceberg table for ML analysis --------------

spark = (
    SparkSession.builder
    .appName("FeatureExtraction")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)


df = spark.sql("""
    SELECT
        flight_date,
        airline_name AS airline,
        dept_airport_iata AS dep_airport,
        arr_airport_iata AS arr_airport,
        dept_timezone,
        arr_timezone,
        dept_scheduled,
        dept_estimated,
        arr_scheduled,
        dept_airport_delay,
        arr_delay,
        month,
        day
    FROM local.flights
    WHERE arr_delay IS NOT NULL AND NOT isnan(arr_delay)
""").sample(fraction=0.015, seed=42)

df.write.mode("overwrite").parquet("data/model_features/")
df.show()