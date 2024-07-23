import pendulum
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

import logging


def query_bikeshare_data() -> tuple:
    client = bigquery.Client()

    # Check if the BigQuery Storage API is available
    try:
        from google.cloud import bigquery_storage
        bqstorage_client = bigquery_storage.BigQueryReadClient()
    except ImportError:
        bqstorage_client = None
    yesterday = pendulum.yesterday().format('YYYY-MM-DD')
    yesterday_dummy = pendulum.yesterday().subtract(months=8).format('YYYY-MM-DD')

    query = f"""
    SELECT *
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` 
    WHERE  DATE(start_time)= '{yesterday_dummy}'
    """

    query_job = client.query(query)
    results = query_job.result().to_dataframe(bqstorage_client=bqstorage_client)

    # Convert start_time to datetime
    results['start_time'] = pd.to_datetime(results['start_time'])

    return results, yesterday


def save_to_parquet(daily_data, date) -> None:
    storage_client = storage.Client()
    bucket_name = 'austin-bikeshare'
    bucket = storage_client.bucket(bucket_name)

    # Check if bucket exists, if not create it
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
        logging.info(f"Bucket {bucket_name} created.")

    base_path = f'austin-bikeshare-daily/{date}'

    for hour in range(24):
        # Ensure start_time is datetime before filtering
        daily_data['start_time'] = pd.to_datetime(daily_data['start_time'])

        # Filter data by hour
        hourly_data = daily_data[daily_data['start_time'].dt.hour == hour]

        if not hourly_data.empty:
            hour_data = hourly_data.reset_index(drop=True)
            hour_path = f'{base_path}/{hour:02d}/bikesahre_trips.parquet'
            blob = bucket.blob(hour_path)

            output_stream = pa.BufferOutputStream()
            pq.write_table(pa.Table.from_pandas(hour_data), output_stream)
            blob.upload_from_string(output_stream.getvalue().to_pybytes(), content_type='application/octet-stream')
            logging.info(f"Saved data for {date} hour {hour:02d} to {hour_path}")


if __name__ == "__main__":
    data, date = query_bikeshare_data()
    if not data.empty:
        logging.info(f"Data fetched successfully. Total records: {len(data)}")
        save_to_parquet(data, date)
    else:
        logging.info("No data available to fetch.")
