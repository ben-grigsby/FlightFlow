# dags/weekly_model_retrain.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'weekly_model_retrain',
    schedule_interval='0 5 * * 0',
    start_date=datetime(2025, 10, 7),
    catchup=False,
    tags=['model_retrain']
) as dag:
    
    extract_data = BashOperator(
        task_id='extract_data_from_iceberg',
        bash_command="python3 /opt/airflow/etl/feature_engineering/feature_extraction.py"
    )

    clean_data = BashOperator(
        task_id='clean_extracted_data',
        bash_command="python3 /opt/airflow/etl/feature_engineering/feature_cleaned.py"
    )

    engineer_data = BashOperator(
        task_id='engineer_data_for_modeling',
        bash_command="python3 /opt/airflow/etl/feature_engineering/feature_extract_time_features.py"
    )

    encode_data = BashOperator(
        task_id='encode_data',
        bash_command="python3 /opt/airflow/etl/feature_engineering/feature_encoding.py"
    )

    retrain_model = BashOperator(
        task_id='retrain_model',
        bash_command=(
            "spark-submit "
            "--master 'local[*]' "
            "--driver-memory 2g "
            "--executor-memory 1g "
            "--conf spark.sql.shuffle.partitions=50 "
            "/opt/airflow/scripts/modeling/train_spark_model.py || true"
        )
    )

    extract_data >> clean_data >> engineer_data >> encode_data >> retrain_model