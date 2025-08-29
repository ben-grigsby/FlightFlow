from kafka import KafkaProducer
import json 
import time
import requests

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


while True:
    response = requests.get('https://opensky-network.org/api')
    data = response.json()

    producer.send('[TOPIC_NAME]', value=data)

    time.sleep(11)

