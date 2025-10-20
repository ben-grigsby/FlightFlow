# dags/future_flights_pred_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime 

import sys

sys.path.append("/opt/airflow/scripts/")
sys.path.append("/opt/airflow/etl/")
sys.path.append("/opt/airflow/")

from avstack.avstack_kafka_producer import run_kafka_producer_future
from avstack.avstack_kafka_consumer import run_future_kafka_consumer

with DAG(
    'avstack_acquire_future_data',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 10, 20),
    catchup=False,
    tags=['avstack_future']
) as dag:
    
    kafka_producer = PythonOperator(
        task_id='run_kafka_producer',
        python_callable=run_kafka_producer_future
    )

    kafka_consumer = PythonOperator(
        task_id='run_kafka_consumer',
        python_callable=run_future_kafka_consumer
    )

    parquet_conversion = BashOperator(
        task_id='parquet_conversion',
        bash_command="python3 /opt/airflow/etl/future_data/future_data_etl.py"
    )

    data_encoding = BashOperator(
        task_id='future_data_encoding',
        bash_command="python3 /opt/airflow/etl/future_data/future_data_encoding.py"
    )

    predict_delay = BashOperator(
        task_id='predicting_future_delays',
        bash_command="python3 /opt/airflow/scripts/modeling/predict_future.py"
    )

    kafka_producer >> kafka_consumer >> parquet_conversion >> data_encoding >> predict_delay