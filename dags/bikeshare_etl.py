import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.config import load_config

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/../scripts/"))

from scripts.create_external_table import create_external_table
from scripts.extract_bike_share import query_bikeshare_data, save_to_parquet

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "bikeshare_etl",
    default_args=default_args,
    description="Extracts bikeshare data from BigQuery and saves it to GCS as Parquet files. Creates an external "
    "table in BigQuery.",
    schedule_interval="0 4 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


start = Variable.get("start_date", default_var=None)
end = Variable.get("end_date", default_var=None)


def get_config():
    logging.info("Loading configuration %s", load_config())
    return load_config()


def extract_and_save(start, end):
    config = get_config()
    results, date = query_bikeshare_data(config, start, end)
    logging.info(f"Data fetched successfully. Total records: {len(results)}")
    save_to_parquet(config, results, date)


def create_table():
    config = get_config()
    bucket_name = config["bucket_name"]
    folder_name = config["folder_name"]
    dataset_id = config["dataset"]
    table_id = config["table_id"]
    create_external_table(bucket_name, folder_name, dataset_id, table_id)


with dag:
    get_config_task = PythonOperator(task_id="get_configs", python_callable=get_config)

    extract_task = PythonOperator(
        task_id="extract_bikeshare_data",
        python_callable=extract_and_save,
        op_args=[start, end],
    )

    create_table_task = PythonOperator(
        task_id="create_biglake_table", python_callable=create_table
    )

    get_config_task >> extract_task >> create_table_task
