# # dags/aviationstack_dag.py

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime

# import sys
# import os

# # Allow Airflow to import from outside the dags folder
# sys.path.append("/opt/airflow/scripts/avstack")
# sys.path.append("/opt/airflow/etl")

# from kafka_producer import run_kafka_producer
# from kafka_consumer import run_kafka_consumer

# from bronze.avstack.bronze_load import insert_into_bronze_ddl
# from silver.avstack.silver_load import load_silver
# from gold.avstack.gold_load import load_gold


# with DAG(
#     dag_id='aviationstack_pipeline_dag',
#     schedule_interval='0 0,4,8 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
#     tags=['aviationstack'],
# ) as dag:

#     run_kafka_task_producer = PythonOperator(
#         task_id='run_kafka_producer',
#         python_callable=run_kafka_producer
#     )

#     run_kafka_task_consumer = PythonOperator(
#         task_id='run_kafka_consumer',
#         python_callable=run_kafka_consumer,
#         do_xcom_push=True
#     )

#     run_bronze_load_ddl = PythonOperator(
#         task_id='run_bronze_load',
#         python_callable=insert_into_bronze_ddl,
#         provide_context=True
#     )

#     run_silver_load_ddl = PythonOperator(
#         task_id='run_silver_load',
#         python_callable=load_silver,
#     )

#     run_gold_load_ddl = PythonOperator(
#         task_id='run_gold_load',
#         python_callable=load_gold,
#         do_xcom_push=False
#     )



#     run_kafka_task_producer >> run_kafka_task_consumer >> run_bronze_load_ddl >> run_silver_load_ddl >> run_gold_load_ddl