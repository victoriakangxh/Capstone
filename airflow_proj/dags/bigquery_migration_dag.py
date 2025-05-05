import os
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)

from constants import PROJECT_ID, DATASET_ID

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="bigquery_migration",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["data_warehouse"],
)
def bigquery_migration():
    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_ID,
        project_id=PROJECT_ID,
    )


bigquery_migration_dag = bigquery_migration()
