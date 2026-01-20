from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _hello():
    print("### Hello from NAU Analytics DAG repo in main branch v5###")


with DAG(
    dag_id="test_hello_world",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # só trigger manual
    catchup=False,
    tags=["test", "debug"],
) as dag:
    hello = PythonOperator(
        task_id="say_hello",
        python_callable=_hello,
    )
