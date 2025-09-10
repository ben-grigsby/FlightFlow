# scripts/kafka_producer.py

from kafka import KafkaProducer
import json
import requests

def run_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=20000000
    )

    URL = "http://api.aviationstack.com/v1/flights"
    API_KEY = 'your_api_key_here'
    params = {'access_key': API_KEY}

    response = requests.get(URL, params=params)
    print("Status Code: ", response.status_code)
    
    data = response.json()
    print("Sending full payload...")

    producer.send("aviation_flight_data", value=data)
    producer.flush()
    print("Sent.")