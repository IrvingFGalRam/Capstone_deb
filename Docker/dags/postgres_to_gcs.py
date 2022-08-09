"""Database flush to csv Workflow
Author: Irving FGR
Description: Flushes the content of a table into a GCS bucket as a CSV.
"""

import csv
import logging
import tempfile

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# General constants
DAG_ID = "gcp_postgres_to_gcs_csv"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"

# GCP constants
GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET_NAME = "capstone-project-wzl-storage"
GCS_PATH = "tmp/"
GCS_POSTGRES_CSV_ID = "_psql.csv"

# Postgres constants
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE_NAME = "user_purchase"


def postgres_to_gcs(
    gcs_bucket: str,
    gcs_path: str,
    postgres_table: str,
    file_name: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    """Flushes the content of a table into a GCS bucket as a CSV.
    Args:
        gcs_bucket (str): Name of the bucket.
        gcs_path (str): Path to save the csv.
        postgres_table (str): Name of the postgres table.
        file_name (str): Name of the file to upload
        gcp_conn_id (str): Name of the Google Cloud connection ID.
        postgres_conn_id (str): Name of the postgres connection ID.
    """
    gcs_hook = GoogleCloudStorageHook(gcp_conn_id=gcp_conn_id)
    pg_hook = PostgresHook.get_hook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from " + postgres_table)
    result = cursor.fetchall()
    with open(file_name, 'w') as tmp:
        a = csv.writer(tmp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow([i[0] for i in cursor.description])
        a.writerows(result)
        logging.info("Uploading to bucket, " + file_name)
        gcs_hook.upload(gcs_bucket, gcs_path + file_name, tmp.name)


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, STABILITY_STATE],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    continue_process = DummyOperator(task_id="continue_process")

    postgres_to_gcs_csv = PythonOperator(
        task_id="postgres_to_gcs_csv",
        python_callable=postgres_to_gcs,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_path": GCS_PATH,
            "postgres_table": POSTGRES_TABLE_NAME,
            "file_name": POSTGRES_TABLE_NAME + GCS_POSTGRES_CSV_ID,
        }
    )

    validate_data_csv = BranchSQLOperator(
        task_id="validate_data_csv",
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[postgres_to_gcs_csv.task_id],
    )

    end_workflow = DummyOperator(
        task_id="end_workflow",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    start_workflow >> validate_data_csv >> [continue_process, postgres_to_gcs_csv] >> end_workflow


    # dag.doc_md = __doc__