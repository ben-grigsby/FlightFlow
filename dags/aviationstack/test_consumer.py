from kafka import KafkaConsumer

import json

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

print("Starting consumer...")

for i, message in enumerate(consumer):
    print("Message received")
    with open(f"as_message_{i}.json", "w") as f:
        json.dump(message.value, f, indent=2)