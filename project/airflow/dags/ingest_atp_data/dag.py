import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from scripts import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET_EXT = os.environ.get("BIGQUERY_DATASET", 'tennis_ext')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'tennis_data')

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Dataset URLs
dataset_url_prefix = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{ds_year}.csv"
ds_years = list(range(1968,2023))
dataset_url_matches = [dataset_url_prefix.format(ds_year=ds_year) for ds_year in ds_years]
dataset_url_prefix = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_rankings_{ds_year}.csv"
ds_years = ['70s', '80s', '90s', '00s', '10s', '20s', 'current']
dataset_url_ranking = [dataset_url_prefix.format(ds_year=ds_year) for ds_year in ds_years]
dataset_url_players = ["https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_players.csv"]

default_args = {
    "owner": "igor.l.smirnov@gmail.com",
    "start_date": datetime(2022,1,1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_atp_data",
    schedule_interval="@once",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-project'],
) as dag:

    for dataset_url in dataset_url_players + dataset_url_ranking + dataset_url_matches:
        dataset_file = os.path.split(dataset_url)[1]
        dataset_name = dataset_file.split('.')[0]

        download_dataset_task = BashOperator(
            task_id=f"download_{dataset_name}_task",
            bash_command=f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_{dataset_name}_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{dataset_file}",
                "local_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_ext_table_{dataset_name}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_EXT,
                    "tableId": dataset_name,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "CSV",
                    "autodetect": True,
                    "allowJaggedRows": True,
                    "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}"],
                },
            },
        )

        CREATE_BQ_TBL_QUERY = f"""
        CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{dataset_name}
        AS
        SELECT * FROM {BIGQUERY_DATASET_EXT}.{dataset_name};
        """
        
        create_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_table_{dataset_name}_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task >> create_table_task