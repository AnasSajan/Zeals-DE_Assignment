# Use a base image with Google Cloud SDK already installed
FROM google/cloud-sdk:latest AS gcloud

# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.9.3

# Copy Google Cloud SDK from the previous stage
COPY --from=gcloud / /google-cloud-sdk/

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         python3-venv \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies in the virtual environment
COPY requirements.txt /
RUN pip install -r /requirements.txt
RUN pip install psycopg2-binary apache-airflow-providers-celery

USER airflow

# Copy DAGs and scripts to the Airflow directory
COPY dags/ /opt/airflow/dags/
COPY dags/scripts/ /opt/airflow/dags/scripts/
COPY utils/ /opt/airflow/utils/

# Set PYTHONPATH to include dags and scripts directories
ENV PYTHONPATH="/opt/airflow/utils:/opt/airflow/dags:/opt/airflow/dags/scripts:$PYTHONPATH"

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/dags/scripts/service_account.json"
COPY .env /opt/airflow/.env


# Use the official Airflow entrypoint
ENTRYPOINT ["/entrypoint"]

# Default command
CMD ["webserver"]
