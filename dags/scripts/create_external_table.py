import json
import logging
import os

from google.cloud import bigquery

SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


def get_service_account_info():
    with open(SERVICE_ACCOUNT_PATH, "r") as file:
        service_account_info = json.load(file)
    return service_account_info


def create_external_table(bucket_name, folder_name, dataset_id, table_id) -> None:
    client = bigquery.Client()
    service_account_info = get_service_account_info()
    project_id = service_account_info["project_id"]
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    gcs_path = f"gs://{bucket_name}/{folder_name}/*.parquet"

    schema = [
        bigquery.SchemaField("trip_id", "STRING"),
        bigquery.SchemaField("subscriber_type", "STRING"),
        bigquery.SchemaField("bike_id", "STRING"),
        bigquery.SchemaField("bike_type", "STRING"),
        bigquery.SchemaField("start_time", "TIMESTAMP"),
        bigquery.SchemaField("start_station_id", "STRING"),
        bigquery.SchemaField("start_station_name", "STRING"),
        bigquery.SchemaField("end_station_id", "STRING"),
        bigquery.SchemaField("end_station_name", "STRING"),
        bigquery.SchemaField("duration_minutes", "INT64"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [gcs_path]
    external_config.schema = schema
    external_config.options.hive_partitioning_mode = "AUTO"

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    client.create_dataset(dataset_id, exists_ok=True)
    client.create_table(table, exists_ok=True)
    logging.info(f"Table {table_id} created.")
