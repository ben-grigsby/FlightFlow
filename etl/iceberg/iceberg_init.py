# etl/iceberg_init.py

from pyspark.sql import SparkSession
import os

def iceberg_init():
    """
    Initializes the Iceberg table `local.flights` if it doesn't exist.
    Prints out paths for full visibility inside Airflow logs.
    """

    # -------------------- Determine Correct Data Path --------------------
    if os.path.exists("/opt/airflow/data"):
        DATA_DIR = "/opt/airflow/data"
    else:
        DATA_DIR = "/app/data"

    WAREHOUSE_PATH = os.path.join(DATA_DIR, "iceberg_warehouse")
    PARQUET_PATH = os.path.join(DATA_DIR, "iceberg_parquet")

    print("\n================== ICEBERG INIT DEBUG INFO ==================")
    print(f"DATA_DIR:        {DATA_DIR}")
    print(f"WAREHOUSE_PATH:  {WAREHOUSE_PATH}")
    print(f"PARQUET_PATH:    {PARQUET_PATH}")
    print("=============================================================\n")

    # -------------------- Initialize Spark with Iceberg Support --------------------
    spark = (
        SparkSession.builder
        .appName("IcebergInit")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .getOrCreate()
    )

    print("Spark with Iceberg initialized!\n")

    # -------------------- Create Iceberg Table --------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.flights
        USING iceberg
        PARTITIONED BY (month, day)
        AS 
        SELECT * FROM parquet.`{PARQUET_PATH}`
    """)

    print(f"Iceberg table created successfully at: {WAREHOUSE_PATH}/flights")
    print(f"Parquet data source used: {PARQUET_PATH}\n")

    # -------------------- Verification Queries --------------------
    print("🔍 Verifying Iceberg table contents...\n")
    spark.sql("SELECT COUNT(*) AS total_rows FROM local.flights").show()
    spark.sql("""
        SELECT month, COUNT(*) AS rows_per_month
        FROM local.flights
        GROUP BY month
        ORDER BY month
    """).show()
    spark.sql("DESCRIBE TABLE local.flights").show(1000, truncate=False)

    spark.stop()
    print("\nIceberg initialization completed successfully.\n")


if __name__ == '__main__':
    iceberg_init()