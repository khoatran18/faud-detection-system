import textwrap
from datetime import datetime, timedelta
from airflow.sdk import DAG, dag, task

from batch_layer.train_ml.train_pipeline import train_pipeline


@dag(
    schedule="*/10 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['training', 'model', 'fraud', 'prediction']
)
def train_batch_jobs():

    @task()
    def train_pipeline_dag() -> None:
        train_pipeline()

    train_pipeline_dag()

train_batch_jobs()
