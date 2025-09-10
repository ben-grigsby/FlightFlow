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
    API_KEY = 'a21fadb9119de29308677536faf1bb6b'
    params = {'access_key': API_KEY}

    response = requests.get(URL, params=params)
    print("Status Code: ", response.status_code)
    print("URL: ", response.url)
    print(API_KEY)
    
    data = response.json()
    print("Sending full payload...")

    producer.send("aviation_flight_data", value=data)
    producer.flush()
    print("API response keys:", data.keys())
    print("Sent.")