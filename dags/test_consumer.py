from kafka import KafkaConsumer
import json 


consumer = KafkaConsumer(
    '[TOPIC_NAME]',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earlier',
    group_id='[CONSUMER_GROUP_NAME]',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    data = message.value

    print("Recieved: ", data)