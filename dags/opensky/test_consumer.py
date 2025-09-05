from kafka import KafkaConsumer

import json
import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from etl.SQL_functions import (
    insert_into_postgres,
)

from etl.SQL_queries import (
    insert_opensky_flight_data
)


consumer = KafkaConsumer(
    'flight_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    fetch_max_bytes=20_000_000,
    max_partition_fetch_bytes=20_000_000,
    group_id='flight_data_test_group_03',
    enable_auto_commit=True,
    consumer_timeout_ms=3000
)

print("⚙️  Starting consumer...")

try:
    for i, message in enumerate(consumer):
        print("Message received")


        payload = message.value

        if isinstance(payload, bytes):
            payload = payload.decode('utf-8')
            payload = json.loads(payload)

        print(f"Inserting message_{i} into PostgreSQL db.")

        with open(f"os_message_{i}.json", "w") as f:
            json.dump(payload, f, indent=2)

        insert_into_postgres(
            'flight_db', 
            'user', 
            'pass', 
            payload['states'],
            insert_opensky_flight_data
            )
        
    print("Finished inserting into Postgre db.")

except KeyboardInterrupt:
    print("Stopped consumer manually.")
finally:
    consumer.close()