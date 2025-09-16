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

logging.basicConfig(
    filename=log_file_a,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def run_kafka_consumer_fast(consumer, user_value):

    for message in consumer:
        payload = message.value
        data = payload.get('states')

        if data is None:
            logging.error(f"Missing 'states' key in Kafka payload: {payload}")
            continue

        for entry in data:
            baro_alt = entry[7]
            geo_alt = entry[13]

            
            if geo_alt:
                if geo_alt < user_value:
                    logging.warning("Altitude dropped below threshold!")
                    logging.warning(f"Geometric Altitude: {geo_alt}       Threshold: {user_value}")
                else:
                    logging.info(f"No warning for aircraft {entry[0]}")
            else:
                logging.info(f"No altitude information for aircraft {entry[0]}")


    logging.info("Completed all checks for current batch.")



# Save PID for Airflow to stop the process later
with open('/tmp/altitude_alert.pid', 'w') as f:
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
        logging.info("Checking new batch from Kafka...")
        run_kafka_consumer_fast(consumer, user_value=1000)
        time.sleep(11)
except KeyboardInterrupt:
    logging.info("Stopped altitude alert monitoring.")