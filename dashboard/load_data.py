# dashboard/load_data.py

import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from datetime import datetime, timedelta



def ib_preview_table():
    spark = (
        SparkSession.builder
        .appName("TopCountries24h")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")  # ← ADD THIS
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    query = f"""
    SELECT 
        * 
    FROM local.flights
    LIMIT 10
    """

    df = spark.sql(query)
    pd_df = df.toPandas()

    spark.stop()

    return pd_df



def ib_busiest_cities_24h():
    spark = (
        SparkSession.builder
        .appName("TopCities24h")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    # Filter last 24 hours
    query = """
        SELECT 
            SPLIT(dept_timezone, '/')[1] AS city,
            COUNT(*) AS flight_count
        FROM local.flights
        WHERE dept_timezone LIKE '%/%'
        AND CAST(dept_scheduled AS TIMESTAMP) >= (current_timestamp() - INTERVAL 1 DAY)
        GROUP BY SPLIT(dept_timezone, '/')[1]
        ORDER BY flight_count DESC
        LIMIT 10
    """

    df = spark.sql(query)
    pd_df = df.toPandas()
    spark.stop()
    return pd_df


def get_gold_table(limit=100):
    engine = create_engine("postgresql://user:pass@postgres:5432/flight_db")
    query = f"""
        SELECT * 
        FROM opensky.gold_flight_stats_by_country
        LIMIT {limit}
    """
    df = pd.read_sql(query, engine)
    
    return df


def get_top_origin_countries():
    engine = engine = create_engine("postgresql://user:pass@postgres:5432/flight_db")
    query = f"""
        SELECT 
            *
        FROM opensky.gold_top_origin_countries
    """
    df = pd.read_sql(query, engine)

    return df