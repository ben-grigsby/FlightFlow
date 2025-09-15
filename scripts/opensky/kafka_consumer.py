# scripts/opensky/kafka_consumer.py
from kafka import KafkaConsumer
from airflow.utils.log.logging_mixin import LoggingMixin

import datetime
import json
import os

def run_kafka_consumer_slow():

    log = LoggingMixin().log

    consumer = KafkaConsumer(
    'opensky_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        fetch_max_bytes=20_000_000,
        max_partition_fetch_bytes=20_000_000,
        consumer_timeout_ms=10000,
        group_id='opensky_full_pipeline_consumer_01'
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


def run_kafka_consumer_fast(user_value):

    log = LoggingMixin().log

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

    for message in consumer:
        payload = message.value

        data = payload.get('states')
        if data is None:
            log.error(f"Missing 'states' key in Kafka payload: {payload}")
            continue
        
        for entry in data:
            baro_alt = entry[7]
            geo_alt = entry[13]

            if geo_alt:
                if geo_alt < user_value:
                    log.warning("Altitude dropped below threshold!")
                    log.warning(f"Geometric Altitude: {geo_alt}       Baromatric Altitude: {user_value}")
                
                else:
                    log.info(f"No warning for aircraft {entry[0]}")
            
            else:
                log.info(f"No information for aircraft {entry[0]}")
    
    log.info("Completed all checks for current batch.")


# run_kafka_consumer_slow()
# run_kafka_consumer_fast(5000)