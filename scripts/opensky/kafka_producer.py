# dags/opensky/kafka_producer.py

from kafka import KafkaProducer
from airflow.utils.log.logging_mixin import LoggingMixin

import datetime
import json
import os
import sys
import requests
import time


# ==================================================================
# Action
# ==================================================================

def run_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=20000000
    )

    log = LoggingMixin().log

    URL = "https://opensky-network.org/api/states/all"
    log.info("Requesting data from OpenSky API.")

    try:
        response = requests.get(URL)
        response.raise_for_status()
    except requests.RequestException as e:
        log.error(f"OpenSky API request failed: {e}")
        return

    data = response.json()
    log.info("Sending full payload...")

    producer.send("opensky_data", value=data)
    producer.flush()
    log.info(f"API response keys: {data.keys()}")
    log.info("Sent.")
