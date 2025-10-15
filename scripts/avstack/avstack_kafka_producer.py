# scripts/avstack/kafka_producer.py

from dotenv import load_dotenv
from kafka import KafkaProducer

import sys
import os
import json
import requests
import datetime
import time

load_dotenv(dotenv_path="/opt/airflow/.env")

# ==================================================================
# Action
# ==================================================================


def run_kafka_producer():
    AVIATION_API_KEY = os.getenv("AVSTACK_API_KEY")

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=20000000
    )

    URL = "http://api.aviationstack.com/v1/flights"
    API_KEY = AVIATION_API_KEY
    params = {
        'access_key': API_KEY,
        'flight_date': "2025-09-20",
        'limit': 100
        }

    response = requests.get(URL, params=params)
    print("Status Code: ", response.status_code)
    # print("URL: ", response.url)
    # print(API_KEY)
    
    data = response.json()
    print("Sending full payload...")

    producer.send("aviation_flight_data", value=data)
    producer.flush()
    print("API response keys:", data.keys())
    print("Sent.")



def run_kafka_producer_historic(start_date="2025-01-16", end_date="2025-02-01", limit=1000, daily_cap=90000):
    API_KEY = os.getenv("AVSTACK_API_KEY")

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    current = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    while current <= end:
        url = "http://api.aviationstack.com/v1/flights"
        offset = 0
        total = None
        ingested = 0

        while True:
            # Stop early if we hit the daily cap
            if ingested >= daily_cap:
                print(f"⚠️ Reached cap of {daily_cap} flights for {current}, moving to next day.")
                break

            params = {
                "access_key": API_KEY,
                "flight_date": current.strftime("%Y-%m-%d"),
                "limit": limit,
                "offset": offset
            }

            print("📡 Sending request:", url, params)
            response = requests.get(url, params=params)
            print("Status Code:", response.status_code)
            data = response.json()

            # send one "page" of flights to Kafka
            producer.send("aviation_flight_data", value=data)
            producer.flush()

            # Figure out total once
            if total is None:
                total = data.get("pagination", {}).get("total", 0)
                print(f"🔢 Total flights for {current}: {total} (capped at {daily_cap})")

            count = data.get("pagination", {}).get("count", 0)
            ingested += count
            print(f"✅ Sent {count} records (offset={offset}), total sent today={ingested}")

            with open("pagination_log.txt", "a") as log_file:
                pagination_info = data.get("pagination", {})
                log_file.write(f"{current} | offset={offset} | info={pagination_info}\n")

            offset += limit

            # stop if we’ve paged through all OR hit the cap
            if offset >= total or ingested >= daily_cap:
                break
                

        # move to next day
        current += datetime.timedelta(days=1)
        time.sleep(60)

    print("🎉 Completed producing messages for range")



def run_kafka_producer_future():
    API_KEY = os.getenv("AVSTACK_API_KEY")

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    url = "https://api.aviationstack.com/v1/flights"

    params = {
        "access_key": API_KEY,
        "iataCode": "JFK",         # Airport code
        "type": "departure",       # or "arrival"
        "date": "2025-09-30"       # Future date
    }

    print("📡 Sending request:", url, params)
    response = requests.get(url, params=params)
    print("Status Code:", response.status_code)
    print("Response preview:", response.text[:400])  # show first 400 chars

    data = response.json()

    producer.send("aviation_flight_data_future", value=data)
    producer.flush()
    print(f"✅ Sent {len(data.get('data', []))} future flights to Kafka")
    

if __name__ == "__main__":
    run_kafka_producer_historic()