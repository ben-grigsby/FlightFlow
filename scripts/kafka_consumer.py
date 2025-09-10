# dags/aviationstack/kafka_consumer.py

from kafka import KafkaConsumer
from airflow.utils.log.logging_mixin import LoggingMixin



import datetime

import json
import os
import sys


# ==================================================================
# Constants
# ==================================================================

db = "flight_db"
user = "user"
password = "pass"


# ==================================================================
# Set-up
# ==================================================================


consumer = KafkaConsumer(
    'aviation_flight_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    fetch_max_bytes=20_000_000,
    max_partition_fetch_bytes=20_000_000,
    consumer_timeout_ms=10000,
    group_id='aviation_flight_data_test_group_01'
)


# ==================================================================
# Action
# ==================================================================



def run_kafka_consumer():
    print("Starting consumer...")

    for message in consumer:
        payload = message.value

        log = LoggingMixin().log
        log.info(f"Kafka payload: {payload}")

        # Safe extraction
        data = payload.get('data')
        if data is None:
            raise ValueError(f"Missing 'data' key in Kafka payload: {payload}")

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/kafka_logs/as_message_{timestamp}.json"

        # Make directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        log.info(f"Saved payload to {filepath}")
        return filepath