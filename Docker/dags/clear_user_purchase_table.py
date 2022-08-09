"""Database Clearing Workflow
Author: Irving FGR
Description: Drops postgres table from a GCP SqlInstance's DB.
"""

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# General constants
DAG_ID = "gcp_database_clear"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"

# GCP constants
GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET_NAME = "capstone-project-wzl-storage"
# GCS_KEY_NAME = "noheader_test_log_reviews.csv"

# Postgres constants
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE_NAME = "user_purchase"


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, STABILITY_STATE],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DROP TABLE IF EXISTS {POSTGRES_TABLE_NAME}",
    )
    continue_process = DummyOperator(task_id="continue_process")

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow >> clear_table >> end_workflow
    )

    # dag.doc_md = __doc__