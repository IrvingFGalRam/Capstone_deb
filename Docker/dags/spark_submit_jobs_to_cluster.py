"""Spark cluster and job subbmiter DAG
Author: Irving FGR
Description: Creates an ephimeral dataproc spark cluster to submit jobs.
"""

import os
from airflow.models import Variable
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

# General constants
DAG_ID = "dataproc_submit_job"
CLUSTER_NAME = "cluster-dataproc-spark-deb"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"
REGION = "us-central1"
ZONE = "us-central1-a"

# ENV VAR
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
# Variables to Arguments
JOB_NAME_SELECTOR = Variable.get("JOB_NAME_SELECTOR")   # "test", "show", "rl", "crm", "up", "ma"
ARG_TABLE_NAME = Variable.get("ARG_TABLE_NAME")
ARG_FORMAT = Variable.get("ARG_FORMAT", default_var="avro")
ARG_N_RECORDS = Variable.get("ARG_N_RECORDS")

TIMEOUT = {"seconds": 1 * 2 * 60 * 60}

# Jobs definitions
SPARK_JOB_TEST = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TestSparkSession",
    },
}
SPARK_JOB_SHOW_TABLE = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.ShowTable",
        "args": [
            "gs://capstone-project-wzl-storage/silver/" + ARG_TABLE_NAME,
            ARG_FORMAT,
            ARG_N_RECORDS
        ]
    }
}
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

JOB_DICT = {
    "test": SPARK_JOB_TEST,
    "show": SPARK_JOB_SHOW_TABLE,
    "rl": SPARK_JOB_T_RL,
    "crm": SPARK_JOB_T_CMR,
    "up": SPARK_JOB_T_UP,
    "ma": SPARK_JOB_T_MA
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, CLUSTER],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    spark_custom_task = DataprocSubmitJobOperator(
        task_id="spark_custom_task",
        job=JOB_DICT[str(JOB_NAME_SELECTOR)],
        region=REGION,
        project_id=PROJECT_ID
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> spark_custom_task >> end_workflow
