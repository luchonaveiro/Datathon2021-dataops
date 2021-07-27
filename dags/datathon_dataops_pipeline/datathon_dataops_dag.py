from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import os
import requests
import logging
import gzip
import shutil
from datathon_dataops_pipeline.calculate_metrics_utils import calculate_metrics
from datathon_dataops_pipeline.download_data_utils import download_data

# Define logger and configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


args = {
    "owner": "luciano.naveiro",
    "start_date": days_ago(2),
}

with DAG(
    dag_id="datathon_dataops_dag",
    default_args=args,
    schedule_interval=None,
    catchup=False,
) as dag:

    download_data_task = PythonOperator(
        task_id="download_data",
        provide_context=False,
        python_callable=download_data,
        retries=3,
    )

    calculate_metrics_task = PythonOperator(
        task_id="calculate_metrics",
        provide_context=False,
        python_callable=calculate_metrics,
    )


# Define dependencies
download_data_task >> calculate_metrics_task
