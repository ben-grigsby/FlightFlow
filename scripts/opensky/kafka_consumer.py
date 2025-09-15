# scripts/opensky/kafka_consumer.py
from kafka import KafkaConsumer
from airflow.utils.log.logging_mixin import LoggingMixin

import datetime
import json
import os

def run_kafka_consumer():

    log = LoggingMixin().log

    consumer = KafkaConsumer(
    'opensky_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        fetch_max_bytes=20_000_000,
        max_partition_fetch_bytes=20_000_000,
        consumer_timeout_ms=10000,
        group_id='open_sky_flight_test_group_01'
    )

    for message in consumer:
        payload = message.value

        data = payload.get('states')
        if data is None:
            log.error(f"Missing 'states' key in Kafka payload: {payload}")
            continue
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/kafka_logs/opensky/as_message_{timestamp}.json"
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        try:
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
            log.info(f"Saved payload to {filepath}")
        except Exception as e:
            log.error(f"Failed to save payload to {filepath}: {e}")
            continue
        
        log.info(f"Saved payload to {filepath}")
        return filepath
