from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore
from airflow.sdk import Variable,Connection #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
from airflow import DAG #type: ignore
from datetime import datetime






try:
    savepath = Variable.get("save_path")
    metadatapath = Variable.get("metadata_path")
    flag = Variable.get("first_ingestion_flag")
    database = Variable.get("mysqldatabase")
    mysql_conn = Connection.get("mysql_connection_info")
    user = mysql_conn.login
    host = mysql_conn.host
    secret = mysql_conn.password
    port = mysql_conn.port
    s3_conn = Connection.get("s3_dev_connection")
    S3_ACCESS_KEY = s3_conn.login
    S3_SECRET_KEY = s3_conn.password
    S3_ENDPOINT = Variable.get("s3endpoint")
except Exception:
    raise Exception("Could not get the variables or secrets")
with DAG(
    dag_id="spark_submit-incremental-ingestion",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run at 17:00 every day
    catchup=False,
    tags=["example"],
) as dag:

    spark_submit_task_incremental_tables = KubernetesPodOperator(
    namespace='analytics',
    service_account_name='spark-role',

    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:d465952',
    image_pull_policy='Always',
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],

    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name incremental-tables-ingestion \
          --conf spark.kubernetes.container.image=nauedu/nau-analytics-external-data-product:feature-ingestion-script-improvements \
          --conf spark.kubernetes.namespace=analytics \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.executor.instances=3 \
          --conf spark.executor.cores=2 \
          --conf spark.executor.memory=2g \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.kubernetes.driverEnv.MYSQL_DATABASE={database} \
          --conf spark.kubernetes.driverEnv.MYSQL_HOST={host} \
          --conf spark.kubernetes.driverEnv.MYSQL_PORT={port} \
          --conf spark.kubernetes.driverEnv.MYSQL_USER={user} \
          --conf spark.kubernetes.driverEnv.MYSQL_SECRET={secret} \
          --conf spark.kubernetes.driverEnv.S3_ACCESS_KEY={S3_ACCESS_KEY} \
          --conf spark.kubernetes.driverEnv.S3_SECRET_KEY={S3_SECRET_KEY} \
          --conf spark.kubernetes.driverEnv.S3_ENDPOINT={S3_ENDPOINT} \
          --conf spark.kubernetes.driver.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/incremental_load.py \
          --savepath {savepath}\
          --metadatapath {metadatapath}\
          --table student_courseenrollment_history \
          --first_ingestion_flag {flag} \
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name='spark-submit-task-incremental-table',
    task_id='spark_submit_task_incremental_table',
    get_logs=True,
    on_finish_action="keep_pod",
    )


