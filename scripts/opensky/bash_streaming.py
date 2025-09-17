# scripts/opensky/bash_streaming.py

import time
import os
import datetime
import logging

from kafka_producer import run_kafka_producer


# === Paths ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(BASE_DIR, "..", "..", "data", "logs", "data_streaming.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)

pid_path = os.path.join(BASE_DIR, "..", "..", "data", "streaming.pid")

# === Logging ===
logger = logging.getLogger("data_streaming_logger")
logger.setLevel(logging.INFO)

# Avoid adding multiple handlers if this script is imported/run multiple times
if not logger.handlers:
    fh = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# === PID Check ===
if os.path.exists(pid_path):
    print(f"Streaming already running (PID file found at {pid_path}). Exiting.")
    exit(0)

# Save the current process ID to a file so Airflow or shell can later stop it
with open(pid_path, 'w') as f:
    f.write(str(os.getpid()))

# === Main Loop ===
try:
    i = 0
    while True:
        logger.info(f"(PID: {os.getpid()}) Streaming time: {i}")
        run_kafka_producer()
        time.sleep(11)
        i += 1
except KeyboardInterrupt:
    logger.info(f"(PID: {os.getpid()}) Stopped streaming")
finally:
    if os.path.exists(pid_path):
        os.remove(pid_path)