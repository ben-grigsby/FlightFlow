# etl/inspect_iceberg_table.py

from pyspark.sql import SparkSession

# ---------------------------
# Spark session (read-only)
# ---------------------------
spark = (
    SparkSession.builder
    .appName("InspectIcebergTable")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n================== SCHEMA ==================")
spark.sql("DESCRIBE local.flights").show(truncate=False)

print("\n================== EXTENDED METADATA ==================")
spark.sql("DESCRIBE EXTENDED local.flights").show(100, truncate=False)

print("\n================== SAMPLE RECORDS ==================")
spark.sql("SELECT * FROM local.flights LIMIT 10").show(truncate=False)

print("\n================== PARTITION COUNTS ==================")
spark.sql("""
    SELECT month, day, COUNT(*) AS row_count
    FROM local.flights
    GROUP BY month, day
    ORDER BY month, day
""").show(truncate=False)

print("\n================== ICEBERG SNAPSHOTS ==================")
spark.sql("SELECT * FROM local.flights.snapshots").show(truncate=False)

print("\n================== ICEBERG FILES ==================")
spark.sql("SELECT * FROM local.flights.files LIMIT 5").show(truncate=False)

spark.stop()