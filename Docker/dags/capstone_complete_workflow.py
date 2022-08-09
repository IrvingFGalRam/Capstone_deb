"""Complete capstone project workflow
Author: Irving FGR
Description: Bronze to Gold complete workflow.
"""

import os
import csv
import logging

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


# General constants
DAG_ID = "complete_workflow"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"


# GCP constants
GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET_NAME = "capstone-project-wzl-storage"
GCS_KEY_NAME = "tmp/user_purchase.csv"
GCS_PATH = "tmp/"
GCS_POSTGRES_CSV_ID = "_psql.csv"

# Postgres constants
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE_NAME = "user_purchase"

# General constants
CLUSTER_NAME = "cluster-dataproc-spark-deb"
REGION = "us-central1"
ZONE = "us-central1-a"

# ENV VAR
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
# Variables to Arguments
ARG_FORMAT = Variable.get("ARG_FORMAT", default_var="avro")

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},

    },
    "software_config": {
        "image_version": "2.0",
        "properties": {
            "spark:spark.jars.packages": "com.databricks:spark-xml_2.12:0.13.0,org.apache.spark:spark-mllib_2.12:3.1.3,org.apache.spark:spark-avro_2.12:3.1.3"
        }
    }
}

TIMEOUT = {"seconds": 1 * 2 * 60 * 60}

# Jobs definitions
SPARK_JOB_T_CMR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformClassifiedMovieReview",
        "args": [
            "gs://capstone-project-wzl-storage/bronze/movie_review.csv",
            "gs://capstone-project-wzl-storage/silver/classified_movie_review",
            ARG_FORMAT
        ]
    }
}
SPARK_JOB_T_RL = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformReviewLogs",
        "args": [
            "gs://capstone-project-wzl-storage/bronze/log_reviews.csv",
            "gs://capstone-project-wzl-storage/silver/review_logs",
            ARG_FORMAT
        ]
    }
}
SPARK_JOB_T_UP = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformUserPurchase",
        "args": [
            "gs://capstone-project-wzl-storage/tmp/user_purchase_psql.csv",
            "gs://capstone-project-wzl-storage/silver/user_purchase",
            ARG_FORMAT
        ]
    }
}
SPARK_JOB_T_MA = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformMovieAnalytics",
        "args": [
            ARG_FORMAT
        ]
    }
}


def ingest_data_from_gcs(
    gcs_bucket: str,
    gcs_object: str,
    postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    """Ingest data from an GCS location into a postgres table.
    Args:
        gcs_bucket (str): Name of the bucket.
        gcs_object (str): Name of the object.
        postgres_table (str): Name of the postgres table.
        gcp_conn_id (str): Name of the Google Cloud connection ID.
        postgres_conn_id (str): Name of the postgres connection ID.
    """
    import tempfile

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)

    with tempfile.NamedTemporaryFile() as tmp:
        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=tmp.name
        )
        psql_hook.copy_expert(f"COPY {postgres_table} FROM STDIN DELIMITER ',' CSV HEADER", tmp.name)


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
    tags=[CLOUD_PROVIDER, STABILITY_STATE, CLUSTER],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    continue_process = DummyOperator(task_id="continue_process")
    continue_process_2 = DummyOperator(task_id="continue_process_2")

    verify_key_existence = GCSObjectExistenceSensor(
        task_id="verify_key_existence",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_KEY_NAME,
    )

    create_table_entity = PostgresOperator(
        task_id="create_table_entity",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS user_purchase (
                invoice_number VARCHAR(10),
                stock_code VARCHAR(20),
                detail VARCHAR(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            )
        """,
    )

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DELETE FROM {POSTGRES_TABLE_NAME}",
    )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_from_gcs,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_object": GCS_KEY_NAME,
            "postgres_table": POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    validate_data = BranchSQLOperator(
        task_id="validate_data",
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[clear_table.task_id],
    )

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
        follow_task_ids_if_false=[continue_process_2.task_id],
        follow_task_ids_if_true=[postgres_to_gcs_csv.task_id],
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    spark_task_t_rl = DataprocSubmitJobOperator(
        task_id="spark_task_t_rl",
        job=SPARK_JOB_T_RL,
        region=REGION,
        project_id=PROJECT_ID,

    )

    spark_task_t_cmr = DataprocSubmitJobOperator(
        task_id="spark_task_t_cmr",
        job=SPARK_JOB_T_CMR,
        region=REGION,
        project_id=PROJECT_ID,

    )

    spark_task_t_up = DataprocSubmitJobOperator(
        task_id="spark_task_t_up",
        job=SPARK_JOB_T_UP,
        region=REGION,
        project_id=PROJECT_ID,

    )

    spark_task_t_ma = DataprocSubmitJobOperator(
        task_id="spark_task_t_ma",
        job=SPARK_JOB_T_MA,
        region=REGION,
        project_id=PROJECT_ID,

    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    # Create Postgres table
    (
        start_workflow
        >> verify_key_existence
        >> create_table_entity
        >> validate_data >> [clear_table, continue_process] >> ingest_data
        >> validate_data_csv >> [continue_process_2, postgres_to_gcs_csv]
        >> create_cluster
        >> spark_task_t_rl
        >> spark_task_t_cmr
        >> spark_task_t_up
        >> spark_task_t_ma
        >> delete_cluster
        >> end_workflow
    )
