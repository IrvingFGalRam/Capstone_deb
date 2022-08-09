"""Spark cluster and job subbmiter DAG
Author: Irving FGR
Description: Creates an ephimeral dataproc spark cluster to submit jobs.
"""

import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

# General constants
DAG_ID = "dataproc_destroy_cluster"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
CLUSTER_NAME = "cluster-dataproc-spark-deb"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"
REGION = "us-central1"
ZONE = "us-central1-a"

TIMEOUT = {"seconds": 1 * 2 * 60 * 60}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, CLUSTER],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> delete_cluster >> end_workflow
