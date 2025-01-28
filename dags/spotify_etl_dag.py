from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

# Add the root project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/../')))

from scripts.spotify_extractor import extract_data
from scripts.spotify_transformer import transform_data
from scripts.spotify_loader import load_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_etl_dag',
    default_args=default_args,
    description='A Spotify ETL workflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task