# dags/opensky_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os

# Allow Airflow to import from outside the dags folder
sys.path.append("/opt/airflow/scripts/opensky")
sys.path.append("/opt/airflow/etl")

