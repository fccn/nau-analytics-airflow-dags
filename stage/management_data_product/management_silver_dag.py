from airflow import DAG  # type: ignore
from datetime import datetime
from airflow.sdk import Variable, Connection  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore

_LEGACY_IMAGE = "nauedu/nau-analytics-spark-shell:d465952"


def get_connection_properties(dag: DAG) -> dict:
    try:
        s3_conn = Connection.get("s3_stage_connection")
        iceberg_conn = Connection.get("iceberg_stage_connection")
        iceberg_extra = iceberg_conn.extra_dejson
        google_string_connection = Connection.get("google_account")
        return {
            "dag": dag,
            "docker_image": Variable.get("management_docker_image"),
            "namespace": Variable.get("namespace"),
            "ENVIRONMENT": Variable.get("ENVIRONMENT"),
            "GOOGLE_ACCOUNT_JSON":google_string_connection.password,
            "GOOGLE_SHEET_ID":Variable.get("JIRA_GOOGLE_SHEET_ID"),
            "S3_ACCESS_KEY": s3_conn.login,
            "S3_SECRET_KEY": s3_conn.password,
            "S3_ENDPOINT": s3_conn.extra_dejson.get("s3endpoint"),
            "ICEBERG_CATALOG_HOST": iceberg_conn.host,
            "ICEBERG_CATALOG_PORT": iceberg_conn.port,
            "ICEBERG_CATALOG_USER": iceberg_conn.login,
            "ICEBERG_CATALOG_PASSWORD": iceberg_conn.password,
            "BRONZE_ICEBERG_DATABASE_CATALOG_NAME": iceberg_extra.get("bronze_iceberg_database_catalog_name"),
            "BRONZE_ICEBERG_CATALOG_NAME": iceberg_extra.get("bronze_iceberg_catalog_name"),
            "BRONZE_ICEBERG_CATALOG_WAREHOUSE": iceberg_extra.get("bronze_iceberg_catalog_warehouse"),
            "SILVER_ICEBERG_DATABASE_CATALOG_NAME": iceberg_extra.get("silver_iceberg_database_catalog_name"),
            "SILVER_ICEBERG_CATALOG_NAME": iceberg_extra.get("silver_iceberg_catalog_name"),
            "SILVER_ICEBERG_CATALOG_WAREHOUSE": iceberg_extra.get("silver_iceberg_catalog_warehouse"),
            "GOLD_ICEBERG_DATABASE_CATALOG_NAME": iceberg_extra.get("gold_iceberg_database_catalog_name"),
            "GOLD_ICEBERG_CATALOG_NAME": iceberg_extra.get("gold_iceberg_catalog_name"),
            "GOLD_ICEBERG_CATALOG_WAREHOUSE": iceberg_extra.get("gold_iceberg_catalog_warehouse"),
        }
    except Exception:
        raise Exception(f"Could not get the variables or secrets: {Exception}")


def make_ingestion_task(
    cfg: dict,
    task_name: str,
    spark_job_name: str,
    script: str,
    image: str | None = None,
) -> KubernetesPodOperator:
    pod_image = image or cfg["docker_image"]
    return KubernetesPodOperator(
        namespace=cfg["namespace"],
        service_account_name="spark-role",
        image=pod_image,
        startup_timeout_seconds=600,
        cmds=["/bin/bash", "-c"],
        arguments=[
            f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name {spark_job_name} \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg["ENVIRONMENT"]} \
          --conf spark.kubernetes.driverEnv.GOOGLE_ACCOUNT_JSON={cfg["GOOGLE_ACCOUNT_JSON"]} \
          --conf spark.kubernetes.driverEnv.GOOGLE_SHEET_ID={cfg["GOOGLE_SHEET_ID"]} \
          --conf spark.kubernetes.driverEnv.S3_ACCESS_KEY={cfg["S3_ACCESS_KEY"]} \
          --conf spark.kubernetes.driverEnv.S3_SECRET_KEY={cfg["S3_SECRET_KEY"]} \
          --conf spark.kubernetes.driverEnv.S3_ENDPOINT={cfg["S3_ENDPOINT"]} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_HOST={cfg["ICEBERG_CATALOG_HOST"]} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PORT={cfg["ICEBERG_CATALOG_PORT"]} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_USER={cfg["ICEBERG_CATALOG_USER"]} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PASSWORD={cfg["ICEBERG_CATALOG_PASSWORD"]} \
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_DATABASE_CATALOG_NAME={cfg["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"]} \
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_CATALOG_NAME={cfg["BRONZE_ICEBERG_CATALOG_NAME"]} \
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_CATALOG_WAREHOUSE={cfg["BRONZE_ICEBERG_CATALOG_WAREHOUSE"]} \
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_DATABASE_CATALOG_NAME={cfg["SILVER_ICEBERG_DATABASE_CATALOG_NAME"]} \
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_CATALOG_NAME={cfg["SILVER_ICEBERG_CATALOG_NAME"]} \
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_CATALOG_WAREHOUSE={cfg["SILVER_ICEBERG_CATALOG_WAREHOUSE"]} \
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_DATABASE_CATALOG_NAME={cfg["GOLD_ICEBERG_DATABASE_CATALOG_NAME"]} \
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_CATALOG_NAME={cfg["GOLD_ICEBERG_CATALOG_NAME"]} \
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_CATALOG_WAREHOUSE={cfg["GOLD_ICEBERG_CATALOG_WAREHOUSE"]} \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/silver/python/{script}\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
            """
        ],
        name=task_name,
        task_id=f"{task_name}_1",
        get_logs=True,
        on_finish_action="delete_pod",
        dag=cfg["dag"],
    )


default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
}

silver_dag = DAG(
    dag_id="management_silver_dag",
    default_args=default_args,
    schedule="0 1 * * *",
    tags=["downtimes_silver_table_ingestion", "stage", "management_data_product"],
)

cfg = get_connection_properties(silver_dag)

# (task_name, spark_job_name, script, image)
# image=None uses cfg["docker_image"]; _LEGACY_IMAGE tasks pin to a specific image tag
INGESTION_TASKS = [
    ("jira_google_sheet_ingestion",  "jira_google_sheet_ingestion-ingestion","silver_jira_ingestion.py",  None),
    ("downtimes_google_sheet_silver",  "downtimes_google_sheet_silver-ingestion","silver_gestao_downtimes.py",  None),
]

tasks = [make_ingestion_task(cfg, *task) for task in INGESTION_TASKS]

for upstream, downstream in zip(tasks, tasks[1:]):
    upstream >> downstream  # type: ignore