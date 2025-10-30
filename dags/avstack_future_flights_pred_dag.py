# dags/future_flights_pred_dag.py

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime 

import sys

sys.path.append("/opt/airflow/scripts/")
sys.path.append("/opt/airflow/etl/")
sys.path.append("/opt/airflow/")

from avstack.avstack_kafka_producer import run_kafka_producer_future
from avstack.avstack_kafka_consumer import run_future_kafka_consumer
from modeling.predict_future import predict_delay_function
from modeling.email_predictions import send_flight_predictions_email

with DAG(
    'avstack_future_flight_delay_prediction',
    schedule_interval='0 3 * * *',
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

    predict_delay = PythonOperator(
        task_id='predict_and_return_excel_path',
        python_callable=predict_delay_function
    )

    # send_email = PythonOperator(
    #     task_id='send_prediction_email',
    #     python_callable=send_flight_predictions_email
    # )

    kafka_producer >> kafka_consumer >> parquet_conversion >> data_encoding >> predict_delay # >> send_email