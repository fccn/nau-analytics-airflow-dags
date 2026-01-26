from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore
from airflow.sdk import Variable,Connection #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
from airflow import DAG #type: ignore
from datetime import datetime






try:
    undesired_column = Variable.get("undesired_column")

    
    mysql_conn = Connection.get("sql_source_dev_connection")
    user = mysql_conn.login
    host = mysql_conn.host
    secret = mysql_conn.password
    port = mysql_conn.port
    database = mysql_conn.extra_dejson.get("mysqldatabase") 


    s3_conn = Connection.get("s3_dev_connection")
    S3_ACCESS_KEY = s3_conn.login
    S3_SECRET_KEY = s3_conn.password
    S3_ENDPOINT =s3_conn.extra_dejson.get("s3endpoint")
    
    iceberg_catalog_conn = Connection.get("iceberg_dev_connection")
    ICEBERG_CATALOG_HOST =iceberg_catalog_conn.host
    ICEBERG_CATALOG_PORT = iceberg_catalog_conn.port
    ICEBERG_CATALOG_NAME = iceberg_catalog_conn.extra_dejson.get("bronze_iceberg_catalog_name")
    ICEBERG_CATALOG_WAREHOUSE = iceberg_catalog_conn.extra_dejson.get("bronze_iceberg_catalog_warehouse")
    ICEBERG_CATALOG_USER = iceberg_catalog_conn.login
    ICEBERG_CATALOG_PASSWORD =iceberg_catalog_conn.password

except Exception:
    raise Exception("Could not get the variables or secrets")
with DAG(
    dag_id="spark_submit-bronze-full-ingestion",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run at 17:00 every day
    catchup=False,
    tags=["example"],
) as dag:
    spark_submit_task_full_tables = KubernetesPodOperator(
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
          --name full-tables-ingestion \
          --conf spark.kubernetes.container.image=nauedu/nau-analytics-external-data-product:feature-ingestion-script-improvements \
          --conf spark.kubernetes.namespace=analytics \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.executor.instances=1 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=512m \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.kubernetes.driverEnv.MYSQL_DATABASE={database} \
          --conf spark.kubernetes.driverEnv.MYSQL_HOST={host} \
          --conf spark.kubernetes.driverEnv.MYSQL_PORT={port} \
          --conf spark.kubernetes.driverEnv.MYSQL_USER={user} \
          --conf spark.kubernetes.driverEnv.MYSQL_SECRET={secret} \
          --conf spark.kubernetes.driverEnv.S3_ACCESS_KEY={S3_ACCESS_KEY} \
          --conf spark.kubernetes.driverEnv.S3_SECRET_KEY={S3_SECRET_KEY} \
          --conf spark.kubernetes.driverEnv.S3_ENDPOINT={S3_ENDPOINT} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_HOST={ICEBERG_CATALOG_HOST} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PORT={ICEBERG_CATALOG_PORT} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_NAME={ICEBERG_CATALOG_NAME} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_USER={ICEBERG_CATALOG_USER} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PASSWORD={ICEBERG_CATALOG_PASSWORD} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_WAREHOUSE={ICEBERG_CATALOG_WAREHOUSE} \
          --conf spark.kubernetes.driver.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/get_full_tables.py\
          --undesired_column {undesired_column}\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name='spark-submit-task-full-table',
    task_id='spark_submit_task_full_table',
    get_logs=True,
    on_finish_action="keep_pod",
    )


    # Set dependency: first Python task, then KubernetesPodOperator
    spark_submit_task_full_tables  #type: ignore
