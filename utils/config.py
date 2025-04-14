import os

import yaml
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")
config_file_path = os.getenv("CONFIG_FILE_PATH")


def load_config() -> dict:
    with open(config_file_path, "r") as file:
        config = yaml.safe_load(file)
    return config.get("production", {})
