from kafka import KafkaProducer
import json 
import time
import sys
import requests

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=20000000
)

URL = "https://opensky-network.org/api/states/all"

while True:

    response = requests.get(URL)
    print("Status Code:", response.status_code)

    data = response.json()

    full_data = data

    print("Sending full payload...")
    producer.send("flight_data", value=full_data)
    producer.flush()
    print("✅ Sent.")

    time.sleep(11)