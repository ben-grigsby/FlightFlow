# scripts/opensky/kafka_consumer.py

from kafka import KafkaConsumer
import datetime
import json
import os
import logging
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file_r = os.path.join(BASE_DIR, "..", "..", "data", "logs", "data_receiving.log")
os.makedirs(os.path.dirname(log_file_r), exist_ok=True)

logger = logging.getLogger("data_receiving_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fh = logging.FileHandler(log_file_r)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

# Define expected OpenSky state vector schema
COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "heading", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source"
]






def run_kafka_consumer_slow():
    logger.info("Starting receiving process...")

    consumer = KafkaConsumer(
        'opensky_data',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        fetch_max_bytes=20_000_000,
        max_partition_fetch_bytes=20_000_000,
        consumer_timeout_ms=10000,  # Wait up to 10 seconds with no new messages
        group_id='opensky_full_pipeline_consumer_01'
    )

    rows = []
    for message in consumer:
        payload = message.value
        data = payload.get('states')

        if not data:
            logger.warning(f"Missing or empty 'states' key in payload: {payload}")
            continue

        for entry in data:
            if len(entry) != len(COLUMNS):
                logger.warning(f"Unexpected entry length: {len(entry)} != {len(COLUMNS)} → skipping: {entry}")
                continue
            rows.append(entry)

    if not rows:
        logger.info("No new messages to process.")
        return None

    df = pd.DataFrame(rows, columns=COLUMNS)

    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_path = os.path.join(BASE_DIR, "..", "..", "data", "kafka_parquet", f"opensky_batch_{timestamp_str}.parquet")
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    try:
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Saved {len(df)} rows to Parquet at {parquet_path}")
        return parquet_path
    except Exception as e:
        logger.error(f"Failed to save Parquet file: {e}")
        return None