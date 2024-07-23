import json
import logging

from google.cloud import bigquery
import yaml


def load_config(config_path) -> dict:
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def get_service_account_info(path):
    with open(path, 'r') as file:
        service_account_info = json.load(file)
    return service_account_info


def create_external_table() -> None:
    client = bigquery.Client()
    service_account_info = get_service_account_info('/opt/airflow/dags/scripts/service_account.json')
    config = load_config('/opt/airflow/dags/scripts/config.yml')
    bucket_name = config['production']['gcp_bucket_name']
    project_id = service_account_info['project_id']
    dataset_id = config['production']['dataset_id']
    table_id = config['production']['table_id']
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    gcs_path = f"gs://{bucket_name}/austin-bikeshare-daily/*/bikesahre_trips.parquet"

    schema = [
        bigquery.SchemaField("trip_id", "STRING"),
        bigquery.SchemaField("subscriber_type", "STRING"),
        bigquery.SchemaField("bike_id", "STRING"),
        bigquery.SchemaField("bike_type", "STRING"),
        bigquery.SchemaField("start_time", "TIMESTAMP"),
        bigquery.SchemaField("start_station_id", "INT64"),
        bigquery.SchemaField("start_station_name", "STRING"),
        bigquery.SchemaField("end_station_id", "STRING"),
        bigquery.SchemaField("end_station_name", "STRING"),
        bigquery.SchemaField("duration_minutes", "INT64")
    ]

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [gcs_path]
    external_config.schema = schema
    external_config.options.hive_partitioning_mode = "AUTO"

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    dataset = client.create_dataset(dataset_id, exists_ok=True)
    table = client.create_table(table, exists_ok=True)
    logging.info(f"Table {table_id} created.")


if __name__ == "__main__":
    create_external_table()
