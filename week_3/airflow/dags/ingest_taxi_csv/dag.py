import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scripts import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

COLORS = ['yellow', 'green', 'fhv']
dataset_url_prefix = "https://s3.amazonaws.com/nyc-tlc/trip+data/{color}_tripdata_{dataset_month}.csv"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

dataset_month = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m') }}"

default_args = {
    "owner": "igor.l.smirnov@gmail.com",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2020,12,31),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_gcs_all_taxi",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    for color in COLORS:
        dataset_url = dataset_url_prefix.format(color=color, dataset_month=dataset_month)
        dataset_file = os.path.split(dataset_url)[1]

        download_dataset_task = BashOperator(
            task_id=f"download_{color}_dataset_task",
            bash_command=f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_{color}_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{dataset_file}",
                "local_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        download_dataset_task >> local_to_gcs_task