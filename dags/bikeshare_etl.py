import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/../scripts/'))

from scripts.extract_bike_share import query_bikeshare_data, save_to_parquet
from scripts.create_external_table import create_external_table


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bikeshare_etl',
    default_args=default_args,
    description='Extracts bikeshare data from BigQuery and saves it to GCS as Parquet files. Creates an external '
                'table in BigQuery.',
    schedule_interval='0 4 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


def extract_and_save():
    data, date = query_bikeshare_data()
    if not data.empty:
        save_to_parquet(data, date)


with dag:
    extract_task = PythonOperator(
        task_id='extract_bikeshare_data',
        python_callable=extract_and_save
    )

    create_table_task = PythonOperator(
        task_id='create_biglake_table',
        python_callable=create_external_table
    )

    extract_task >> create_table_task
