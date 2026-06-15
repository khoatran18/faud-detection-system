# import textwrap
# from datetime import datetime, timedelta
# from airflow.sdk import DAG, dag, task
#
# from batch_layer.train_ml.train_pipeline import train_pipeline
#
#
# @dag(
#     schedule="*/10 * * * *",
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
#     tags=['training', 'model', 'fraud', 'prediction']
# )
# def train_batch_jobs():
#
#     @task()
#     def train_pipeline_dag() -> None:
#         train_pipeline()
#
#     train_pipeline_dag()
#
# train_batch_jobs()

import textwrap
from datetime import datetime, timedelta

import k8s
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
        dag_id="train_batch_jobs_k8s",
        schedule="0 0 1 * *",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['training', 'model', 'fraud', 'prediction']
) as dag:
    job_train_fraud_detection = KubernetesPodOperator(
        task_id="job_train_fraud_detection",
        name="pyspark-fraud-training",
        namespace="fpp",
        image="khoa2k4/project2:batch_jobs_v1",
        image_pull_policy="Always",

        cmds=["python", "-m", "batch_layer.train_ml.train_pipeline"],

        kubernetes_conn_id="kubernetes_default",
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=False
    )

    job_train_fraud_detection
