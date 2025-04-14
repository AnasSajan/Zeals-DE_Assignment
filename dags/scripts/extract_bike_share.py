import logging

import pandas as pd
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage

from utils.config import load_config


def get_query_file() -> str:
    config = load_config()
    query_file_path = config["query_file_path"]
    with open(query_file_path, "r") as file:
        query = file.read()
    return query


def query_bikeshare_data(config, start_date, end_date) -> tuple:
    client = bigquery.Client()
    # Check if the BigQuery Storage API is available
    try:
        from google.cloud import bigquery_storage

        bqstorage_client = bigquery_storage.BigQueryReadClient()
    except ImportError:
        bqstorage_client = None
    if not start_date:
        start_date = pendulum.now("Asia/Tokyo").subtract(days=1).format("YYYY-MM-DD")
    if not end_date:
        end_date = pendulum.now("Asia/Tokyo").format("YYYY-MM-DD")
    logging.info(f"Start date: {start_date}, End date: {end_date}")
    logging.info("Bucket name: %s", config["bucket_name"])
    now = pendulum.now().format("YYYY-MM-DD HH:mm:ss")
    query = (
        get_query_file()
        .replace("#start_date#", f"{start_date}")
        .replace("#end_date#", f"{end_date}")
        .replace("#ingested_at#", f"{now}")
    )
    logging.info(f"Querying data for {start_date} from BigQuery")
    logging.info(f"Query: {query}")
    query_job = client.query(query)
    results = query_job.result().to_dataframe(bqstorage_client=bqstorage_client)

    return results, start_date


def save_to_parquet(config, daily_data, date) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(config["bucket_name"])
    dataset = config["dataset"]
    folder_name = config["folder_name"]

    # Check if bucket exists, if not create it
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket)
        logging.info(f"Bucket {bucket} created.")

    base_path = f"{folder_name}/{date}"

    for hour in range(24):
        # Ensure start_time is datetime before filtering
        daily_data["start_time"] = pd.to_datetime(daily_data["start_time"])

        # Filter data by hour
        hourly_data = daily_data[daily_data["start_time"].dt.hour == hour]

        if not hourly_data.empty:
            hour_data = hourly_data.reset_index(drop=True)
            hour_path = f"{base_path}/{hour:02d}/{dataset}.parquet"
            blob = bucket.blob(hour_path)

            output_stream = pa.BufferOutputStream()
            pq.write_table(pa.Table.from_pandas(hour_data), output_stream)
            blob.upload_from_string(
                output_stream.getvalue().to_pybytes(),
                content_type="application/octet-stream",
            )
            logging.info(f"Saved data for {date} hour {hour:02d} to {hour_path}")
