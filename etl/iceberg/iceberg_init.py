# etl/iceberg_init.py

from pyspark.sql import SparkSession

# -------------- Initialize Spark with Iceberg support --------------
spark = (
    SparkSession.builder
    .appName("IcebergInit")
    .config("spark.driver.memory", "6g")   # increase heap size
    .config("spark.executor.memory", "4g")
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.3")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

print("Spark with Iceberg initialized! \n")

# ---- Create Iceberg table from existing Parquet partitions ----
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.flights
    USING iceberg
    PARTITIONED BY (month, day)
    AS 
    SELECT * FROM parquet.`data/iceberg_parquet`
""")

print("Iceberg table created successfully! \n")

# --- Verify record count and partitions ---
spark.sql("SELECT COUNT(*) AS total_rows FROM local.flights").show()
spark.sql("SELECT month, COUNT(*) AS rows_per_month FROM local.flights GROUP BY month ORDER BY month").show()

spark.sql("DESCRIBE TABLE local.flights").show(1000, truncate=False)

spark.stop()