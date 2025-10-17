# etl/future_data_etl.py

from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime, timedelta

import json 
import os

# ------------------- Spark session -------------------

spark = (
    SparkSession.builder
    .appName("FutureFlightETL")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)

# Path where consumer saves JSON files
json_dir = "data/kafka_logs/avstack/future/"

future_date = (datetime.utcnow().date() + timedelta(days=8))
month = future_date.month
day = future_date.day

rows = []
for file in os.listdir(json_dir):
    if not file.endswith(".json"):
        continue
    with open(os.path.join(json_dir, file)) as f:
        flights = json.load(f)
        for flight in flights:
            dep = flight.get("departure", {})
            arr = flight.get("arrival", {})
            airline = flight.get("airline", {})
            flight_info = flight.get("flight", {})

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

# Convert to Spark DF
df = spark.createDataFrame(rows)

df.show(5)
print(f"Total parsed flights: {df.count()}")

# Save to parquet for inference
output_path = "data/future_parquet/"
df.write.mode("overwrite").parquet(output_path)
print(f"Saved future flight data to {output_path}")