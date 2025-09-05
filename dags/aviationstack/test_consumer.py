# dags/aviationstack/test_consumer.py

from kafka import KafkaConsumer

import json
import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from etl.SQL_queries import (
    avstack_dim_timezone,
    avstack_dim_airport,
    avstack_dim_airline_info,
    avstack_dim_flight_info,
    avstack_fact_arr_dept_info,
    avstack_fact_dept_info,
    avstack_fact_arr_info
)


from etl.SQL_functions import(
    all_table_insertion
)

consumer = KafkaConsumer(
    'aviation_flight_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    fetch_max_bytes=20_000_000,
    max_partition_fetch_bytes=20_000_000,
    consumer_timeout_ms=10000,
    group_id='aviation_flight_data_test_group_00'
)

print("Starting consumer...")

for i, message in enumerate(consumer):
    print("Message received")
    print("Processing data...")
    
    with open(f"as_message_{i}.json", "w") as f:
        json.dump(message.value, f, indent=2)

    payload = message.value
    data = payload['data']

    id_lst = all_table_insertion(
        "flight_db", 
        "user", 
        "pass", 
        data, 
        avstack_dim_timezone,
        avstack_dim_airport,
        avstack_dim_airline_info,
        avstack_dim_flight_info,
        avstack_fact_arr_dept_info,
        avstack_fact_dept_info,
        avstack_fact_arr_info
        )

    print("Completed PostgreSQL data load.")

