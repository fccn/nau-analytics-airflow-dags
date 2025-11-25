from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore

from airflow.operators.python import PythonOperator #type: ignore
from airflow import DAG #type: ignore
from datetime import datetime

def say_hello() -> None:
    print("Hello from Airflow!")

with DAG(
    dag_id="spark_submit_dag",
    start_date=datetime(2023, 1, 1),
    schedule="0 17 * * *",  # Run at 17:00 every day
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=say_hello,
    )

    spark_submit_task = KubernetesPodOperator(
    namespace='analytics',
    service_account_name='spark-role',

    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:featurespark-shell-docker-image',
    image_pull_policy='Always',
    # ✔ override entrypoint to run spark-submit
    cmds=[],

    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
    "--master", "k8s://https://kubernetes.default.svc:443",
    "--deploy-mode", "cluster",
    "--name", "spark-pi",
    "--conf", "spark.kubernetes.container.image=nauedu/nau-analytics-external-data-product:feature-ingestion-script-improvements",
    "--conf", "spark.kubernetes.namespace=analytics",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-role",
    "--conf", "spark.executor.instances=3",
    "--conf", "spark.executor.cores=2",
    "--conf", "spark.executor.memory=2g",
    "--conf", "spark.kubernetes.submission.waitAppCompletion=true",
    "--conf", "spark.kubernetes.submission.reportFailureOnDriverError=true",
    "--conf", "spark.kubernetes.driver.deleteOnTermination=true",
    "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
    "--conf", "spark.kubernetes.container.image.pullPolicy=Always",
    "local:///opt/spark/work-dir/src/bronze/get_full_tables.py"
    ],
    name='spark-submit-task',
    task_id='spark_submit_task',
    get_logs=True,
    is_delete_operator_pod=False,
    )


    # Set dependency: first Python task, then KubernetesPodOperator
    hello_task >> spark_submit_task
