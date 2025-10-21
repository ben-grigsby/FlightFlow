# scripts/aviationstack/kafka_consumer.py

from kafka import KafkaConsumer
from airflow.utils.log.logging_mixin import LoggingMixin

import datetime
import json
import os
import sys
import logging
import pandas as pd
import uuid



# ==================================================================
# Action
# ==================================================================



def run_kafka_consumer(max_idle_seconds=30):

    consumer = KafkaConsumer(
    'aviation_flight_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    fetch_max_bytes=20_000_000,
    max_partition_fetch_bytes=20_000_000,
    consumer_timeout_ms=10000,
    group_id='aviation_flight_data_test_group_03'
    )

    # ==================================================================
    # Live Kafka Consumer
    # ==================================================================

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    last_message_time = datetime.datetime.now()
    
    logger.info("Starting Kafka Consumer")

    for message in consumer:
        logger.info("Retrieving message from consumer")
        payload = message.value

        data = payload.get('data')
        if not data:   # catches None or empty list
            logger.warning(f"No flight data found in payload: {list(payload.keys())}")
            continue   # skip writing empty files

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        filename = f"file_{timestamp}_{uuid.uuid4().hex[:6]}.json"
        filepath = f"data/kafka_logs/avstack/{filename}"
        

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved payload with {len(data)} records to {filepath}")
        consumer.commit()

        last_message_time = datetime.datetime.now()

        if (datetime.datetime.now() - last_message_time).seconds > max_idle_seconds:
            logger.info(f"No new messages for {max_idle_seconds} seconds. Shutting down.")
            break


def run_future_kafka_consumer(max_idle_seconds=30):

    consumer = KafkaConsumer(
    'aviation_flight_data_future',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    fetch_max_bytes=20_000_000,
    max_partition_fetch_bytes=20_000_000,
    consumer_timeout_ms=10000,
    group_id='aviation_flight_data_test_group_01'
    )

    # ==================================================================
    # Live Kafka Consumer
    # ==================================================================

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    last_message_time = datetime.datetime.now()
    
    logger.info("Starting Kafka Future Consumer")

    for message in consumer:
        logger.info("Retrieving message from future consumer")
        payload = message.value

        data = payload
        if not data:   # catches None or empty list
            logger.warning(f"No flight data found in payload: {list(payload.keys())}")
            continue   # skip writing empty files

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        filename = f"file_{timestamp}_{uuid.uuid4().hex[:6]}.json"
        filepath = f"data/kafka_logs/avstack/future/{filename}"
        

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved payload with {len(data)} records to {filepath}")
        consumer.commit()

        last_message_time = datetime.datetime.now()

        if (datetime.datetime.now() - last_message_time).seconds > max_idle_seconds:
            logger.info(f"No new messages for {max_idle_seconds} seconds. Shutting down.")
            break


if __name__ == '__main__':
    run_kafka_consumer()