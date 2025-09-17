# scripts/opensky/bash_altitude_alert.py

import time
import os
import datetime
import json
import logging

from kafka import KafkaConsumer

# Build absolute log path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file_a = os.path.join(BASE_DIR, "..", "..", "data", "logs", "altitude_alert.log")
os.makedirs(os.path.dirname(log_file_a), exist_ok=True)

print(f"Log file will be written to: {os.path.abspath(log_file_a)}")

pid_path = os.path.join(BASE_DIR, "..", "..", "data", "alerting.pid")

# === Robust Logger Setup ===
logger = logging.getLogger("altitude_alert_logger")
logger.setLevel(logging.INFO)



# Prevent duplicate handlers
if not logger.handlers:
    fh = logging.FileHandler(log_file_a)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

logger.info(f"🚨 Altitude Alert Logger Initialized. PID: {os.getpid()}")

def run_kafka_consumer_fast(consumer, user_value):

    for message in consumer:
        payload = message.value
        data = payload.get('states')

        if data is None:
            logger.error(f"Missing 'states' key in Kafka payload: {payload}")
            continue

        for entry in data:
            baro_alt = entry[7]
            geo_alt = entry[13]

            
            if geo_alt:
                if geo_alt < user_value:
                    logger.warning(f"Altitude dropped below threshold! Aircraft: {entry[0]}")
                    logger.warning(f"Geometric Altitude: {geo_alt}       Threshold: {user_value}")
                else:
                    logger.info(f"No warning for aircraft {entry[0]}")
            else:
                logger.info(f"No altitude information for aircraft {entry[0]}")


    logger.info("Completed all checks for current batch.")



# Save PID for Airflow to stop the process later
with open(pid_path, 'w') as f:
    f.write(str(os.getpid()))

consumer = KafkaConsumer(
        'opensky_data',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        fetch_max_bytes=20_000_000,
        max_partition_fetch_bytes=20_000_000,
        consumer_timeout_ms=10000,
        group_id='opensky_altitude_alert_consumer_01'
    )

try:
    while True:
        logger.info("Checking new batch from Kafka...")
        run_kafka_consumer_fast(consumer, user_value=1000)
        time.sleep(11)
except KeyboardInterrupt:
    logger.info("Stopped altitude alert monitoring.")