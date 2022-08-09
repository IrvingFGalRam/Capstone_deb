"""Spark cluster and job subbmiter DAG
Author: Irving FGR
Description: Creates an ephimeral dataproc spark cluster to submit jobs.
"""

import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

# General constants
DAG_ID = "dataproc_create_cluster"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
CLUSTER_NAME = "cluster-dataproc-spark-deb"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"
REGION = "us-central1"
ZONE = "us-central1-a"

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

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, CLUSTER],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,

    )

    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> create_cluster  >> end_workflow
