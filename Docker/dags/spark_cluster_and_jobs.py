"""Spark cluster and job subbmiter DAG
Author: Irving FGR
Description: Creates an ephimeral dataproc spark cluster to submit jobs.
"""

import os
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
DAG_ID = "dataproc_spark_complete_flow"
CLUSTER_NAME = "cluster-dataproc-spark-deb"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"
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

    (
            start_workflow
            >> create_cluster
            >> spark_task_t_rl
            >> spark_task_t_cmr
            >> spark_task_t_up
            >> spark_task_t_ma
            >> delete_cluster
            >> end_workflow
    )

    # from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    # list(dag.tasks) >> watcher()

# from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
# test_run = get_test_run(dag)