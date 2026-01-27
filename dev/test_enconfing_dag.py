from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore
from airflow.sdk import Variable,Connection #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
from airflow import DAG #type: ignore
from datetime import datetime



test_conn = Connection.get("test_connection")

test_conn_USER = test_conn.login
test_conn_PASSWORD =test_conn.password


with DAG(
    dag_id="echo_test_connection",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run at 17:00 every day
    catchup=False,
    tags=["example"],
) as dag:
    spark_submit_task_full_tables = KubernetesPodOperator(
    namespace='dev-nau-analytics',
    service_account_name='spark-role',

    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:d465952',
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],

    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"ECHO {test_conn_USER} {test_conn_PASSWORD} "
    ],
    name='spark-submit-task-full-table',
    task_id='spark_submit_task_full_table',
    get_logs=True,
    on_finish_action="keep_pod",
    )