# dags/opensky/kafka_producer.py

from kafka import KafkaProducer
from airflow.utils.log.logging_mixin import LoggingMixin

import datetime
import json
import os
import sys
import requests
import time


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(BASE_DIR, "..", "..", "data", "logs", "data_streaming.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)


def timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ==================================================================
# Action
# ==================================================================

def run_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=20000000
    )

    URL = "https://opensky-network.org/api/states/all"

    try:
        response = requests.get(URL)
        response.raise_for_status()
    except requests.RequestException as e:
        with open(log_file, "a") as f:
            f.write(f"\n [{timestamp()}] [ERROR] OpenSky API request failed: {e}")
        return

    data = response.json()

    producer.send("opensky_data", value=data)
    producer.flush()
    with open(log_file, "a") as f:
        f.write(f"\n[{timestamp()}] [INFO] API response keys: {data.keys()}")

