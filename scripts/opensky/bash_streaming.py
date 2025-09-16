# scripts/opensky/bash_streaming.py

import time
import os
import datetime

from kafka_producer import run_kafka_producer


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(BASE_DIR, "..", "..", "data", "logs", "data_streaming.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)

def timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Save the current process ID to a file so Airflow or shell can later stop it
with open('/tmp/streaming.pid', 'w') as f:
    f.write(str(os.getpid()))

try:
    i = 0
    while True:
        with open(log_file, "a") as f:
            f.write(f"\n[{timestamp()}] [INFO] Streaming time: {i}")
        run_kafka_producer()
        time.sleep(11)
        i += 1
except KeyboardInterrupt:
    with open(log_file, "a") as f:
        f.write("[{timestamp()}] [INFO] Stopped streaming")