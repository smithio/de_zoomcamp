import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

INPUT_FILETYPE = "csv"
BQ_DATASET_EXTERNAL = "tripdata_external"
BQ_DATASET_PARTITIONED = "tripdata"

COLORS = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime', 'fhv': 'pickup_datetime'}

default_args = {
    "owner": "igor.l.smirnov@gmail.com",
    "start_date": datetime(2021,1,1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_to_bq",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for color, ds_col in COLORS.items():
        # hack for FHV data
        # CSV file for 2020-01 is corrupted and causes problems
        # so I'm not loading 2020 data for FHV at all
        if color == 'fhv':
            sourceUris = [f"gs://{BUCKET}/{color}/{color}_tripdata_2019*.csv"]
        else:
            sourceUris = [f"gs://{BUCKET}/{color}/*.csv"]

        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_files_{color}_tripdata_task',
            source_bucket=BUCKET,
            source_object=f'raw/{color}_tripdata*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{color}/{color}_tripdata',
            move_object=True
        )

        create_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_create_{color}_tripdata_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET_EXTERNAL,
                    "tableId": f"{color}_tripdata_external",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "CSV",
                    "autodetect": True,
                    "allowJaggedRows": True,
                    "sourceUris": sourceUris,
                },
            },
        )

        CREATE_BQ_TBL_QUERY = f"""
        CREATE OR REPLACE TABLE {BQ_DATASET_PARTITIONED}.{color}_tripdata
        PARTITION BY DATE({ds_col})
        AS
        SELECT * FROM {BQ_DATASET_EXTERNAL}.{color}_tripdata_external;
        """
        
        create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_{color}_tripdata_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        move_files_gcs_task >> create_external_table_task >> create_partitioned_table_task