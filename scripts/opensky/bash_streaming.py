# scripts/opensky/bash_streaming.py

import time
import os

from kafka_producer import run_kafka_producer
from airflow.utils.log.logging_mixin import LoggingMixin

# Save the current process ID to a file so Airflow or shell can later stop it
with open('/tmp/streaming.pid', 'w') as f:
    f.write(str(os.getpid()))

log = LoggingMixin().log

try:
    i = 0
    while True:
        log.info(f"Streaming time: {i}")
        run_kafka_producer()
        time.sleep(11)
        i += 1
except KeyboardInterrupt:
    log.info("Stopped streaming")