import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from scripts import format_to_parquet, upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

dataset_url_prefix = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{dataset_month}.csv"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

dataset_month = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m') }}"
table_name = "yellow_taxi_{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}"
dataset_url = dataset_url_prefix.format(dataset_month=dataset_month)
dataset_file = os.path.split(dataset_url)[1]
parquet_file = dataset_file.replace('.csv', '.parquet')

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
    dag_id="ingest_gcs_yellow_taxi",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    convert_task = PythonOperator(
        task_id="convert_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": table_name,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )


    download_dataset_task >> convert_task >> local_to_gcs_task >> bigquery_external_table_task