
## Prerequisites

- Docker
- Docker Compose
- Google Cloud SDK
- GCP Service Account with appropriate permissions

## Setup Instructions

### 1. Google Cloud Setup

1. **Create a GCP Project** if you don't have one.
2. **Enable BigQuery API** and **Google Cloud Storage API**.
3. **Generate a Service Account Key**:
    - Go to the [Google Cloud Console](https://console.cloud.google.com/).
    - Navigate to `IAM & Admin` -> `Service Accounts`.
    - Create a new service account or use an existing one.
    - Assign the necessary roles:
        - BigQuery Data Viewer
        - BigQuery Job User
        - BigQuery User
        - Storage Admin
    - Create and download a JSON key file for this service account and place it inside `dags/scripts/` directory as `service_account.json`.
    - Add your project ID to [dags/scripts/config.yml](dags/scripts/config.yml) file.

### 2. Run the docker compose stack 

Ensure Docker and Docker Compose are installed on your system. If you don't already have it installed, please follow the installation instructions on the [official Docker website](https://docs.docker.com/get-docker/) and [Docker Compose documentation](https://docs.docker.com/compose/install/).


**Step 1**:  
Run this command to initialize the user `from the root directory of the repository`:
- ```shell
    docker compose up airflow-init
  ```

**Step 2** :


Run this command start all services `from the root directory of the repository`:

- ```shell
    docker compose up -d
  ```


### 3. Accessing the Airflow UI and Running DAGs

- Log into the webserver available at: http://localhost:8080. The account has the default login `airflow` and the password `airflow`.

- Trigger the DAG `bikeshare_etl` to run the ETL.

### 4. Check the Data in Google Cloud

- Go to Cloud Strorage and check the bucket `austin-bikeshare` created containing the daily data partitioned by date and hour. 
- Navigate to BigQuery and check the dataset `bikeshare_trips` created containing the table `bikeshare_external` with the data reflected from the hourly partitioned files.

### 5. Run Data Analysis Queries

- [Data Analysis Queries](dags/scripts/queries.sql) : Run the SQL queries in the file to analyze the data in BigQuery.


## Improvements:

- Following steps can be taken to implement data quality checks:
    - Implement data quality checks the data is correctly ingested into Google Storage Buckets from the public dataset.
    - Implement data quality checks to ensure the data is correctly written in the external table inside BigQuery .