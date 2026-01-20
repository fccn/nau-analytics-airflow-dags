from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _hello():
    print("### Hello from NAU Analytics DAG repo branch feat_spark_submit###")

def _hellosub():
    import subprocess
    print(subprocess.check_output(["git", "branch", "--show-current"]).decode())



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
    print_repo = PythonOperator(
        task_id="print_repo_branch",
        python_callable=_hellosub
    )
    hello >> print_repo