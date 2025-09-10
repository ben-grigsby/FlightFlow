# dags/aviationstack/kafka_consumer.py

from kafka import KafkaConsumer

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
    auto_offset_reset='earliest',
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

    message = next(consumer)

    payload = message.value
    data = payload['data']
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"data/kafka_logs/as_message_{timestamp}.json"

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    
    return filepath
    