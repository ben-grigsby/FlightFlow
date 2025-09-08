# dags/aviationstack/test_consumer.py

from kafka import KafkaConsumer

import json
import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from etl.bronze.avstack.bronze_load import (
    insert_into_bronze_ddl
)


from etl.sql_utilities import (
    connect_to_db
)


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
    group_id='aviation_flight_data_test_group_03'
)


cur, conn = connect_to_db(db, user, password)


# ==================================================================
# Action
# ==================================================================

print("Starting consumer...")

for i, message in enumerate(consumer):
    print(f"Message {i} received")
    print("Processing data...")
    
    with open(f"as_message_{i}.json", "w") as f:
        json.dump(message.value, f, indent=2)

    payload = message.value
    data = payload['data']
    # print(data)

    insert_into_bronze_ddl(cur, conn, data)

    