# etl/future_data/future_data_etl.py

import os
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row

# ---------------------- Path Setup ----------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # goes up 1 level to project root from /etl/
DATA_DIR = os.path.join(BASE_DIR, "data")

KAFKA_LOGS_DIR = os.path.join(DATA_DIR, "kafka_logs/avstack/future/")
OUTPUT_PATH = os.path.join(DATA_DIR, "future_parquet/")

# ---------------------- Spark Session ----------------------
spark = (
    SparkSession.builder
    .appName("FutureFlightETL")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------- Prepare Date Info ----------------------
future_date = (datetime.utcnow().date() + timedelta(days=8))
month = future_date.month
day = future_date.day

# ---------------------- Extract JSON Data ----------------------
rows = []
for file in os.listdir(KAFKA_LOGS_DIR):
    if not file.endswith(".json"):
        continue

    with open(os.path.join(KAFKA_LOGS_DIR, file)) as f:
        flights = json.load(f)

        for flight in flights:
            dep = flight.get("departure", {})
            arr = flight.get("arrival", {})
            airline = flight.get("airline", {})
            flight_info = flight.get("flight", {})

            # Extract scheduled times
            dep_time_str = dep.get("scheduledTime")
            arr_time_str = arr.get("scheduledTime")

            # Safely convert "HH:mm" strings → integer hours
            def extract_hour(time_str):
                if time_str and len(time_str) >= 2:
                    try:
                        return int(time_str.split(":")[0])
                    except Exception:
                        return None
                return None

            dep_hour = extract_hour(dep_time_str)
            arr_hour = extract_hour(arr_time_str)

            rows.append(Row(
                airline = airline.get("name"),
                arr_airport = arr.get("iataCode"),
                dep_airport = dep.get("iataCode"),
                dep_hour = dep_hour,
                arr_hour = arr_hour,
                month = month,
                day = day
            ))

# ---------------------- Convert to Spark DataFrame ----------------------
df = spark.createDataFrame(rows)
df.show(5, truncate=False)
print(f"Total parsed flights: {df.count()}")

# ---------------------- Save to Parquet for Inference ----------------------
df.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Saved future flight data to {OUTPUT_PATH}")

spark.stop()