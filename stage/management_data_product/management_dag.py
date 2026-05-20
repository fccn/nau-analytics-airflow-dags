from airflow import DAG  # type: ignore
from datetime import datetime
import requests #type: ignore
import json
import base64
from airflow.sdk import Variable, Connection  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
from airflow.utils.email import send_email_smtp #type: ignore
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level to log
    format="%(asctime)s - %(levelname)s - %(message)s"
)

_LEGACY_IMAGE = "nauedu/nau-analytics-spark-shell:d465952"


def email_fail_alert(context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = getattr(ti, "run_id", "unknown")
    execution_time = getattr(ti, "start_date", "unknown")

    env = "stage"

    subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"
    html_content = f"""
    <h3>🚨 Airflow Task Failed!</h3>
    <ul>
      <li><b>DAG:</b> {dag_id}</li>
      <li><b>Task:</b> {task_id}</li>
      <li><b>Run ID:</b> {run_id}</li>
      <li><b>Execution Time:</b> {execution_time}</li>
      <li><b>environment:</b> {env}</li>
    </ul>
    """

    send_email_smtp(
        to=Variable.get("cc1"),
        subject=subject,
        html_content=html_content,
        conn_id="smtp_error_email", 
    )



def task_fail_alert(context):
    TEAMS_WEBHOOK_URL = Connection.get("WEBHOOK_URL").password
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_time = getattr(ti, "start_date", "unknown")
    run_id = getattr(ti, "run_id", "unknown")
    try_number = getattr(ti, "try_number", "unknown")
    error = str(context.get("exception", "No exception captured"))

    message = {
    "text": f"🚨 **Airflow Task Failed!**\n\n **DAG:** {dag_id}\n\n **Task:** {task_id}\n\n **Run ID:** {run_id}\n\n **Execution Time:** {execution_time}\n\n **Try:** {try_number}\n\n"
        
    }
    
    resp = requests.post(
        TEAMS_WEBHOOK_URL,
        json=message,
        headers={"Content-Type": "application/json"},
    )

    if resp.status_code in (200, 202):
        logging.info("Teams alert sent successfully")
    else:
        logging.error(f"Failed to send message to Teams: {resp.status_code} {resp.text}")

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
            "GOOGLE_ACCOUNT_JSON": base64.b64encode(json.dumps(json.loads(google_string_connection.password)).encode()).decode(),
            "GOOGLE_SHEET_ID":Variable.get("JIRA_GOOGLE_SHEET_ID"),
            "DOWNTIMES_GOOGLE_SHEET_ID": base64.b64encode(Variable.get("DOWNTIMES_GOOGLE_SHEET_ID").encode()).decode(),
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
          --deploy-mode client \
          --name {spark_job_name} \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg["ENVIRONMENT"]} \
          --conf 'spark.kubernetes.driverEnv.GOOGLE_ACCOUNT_JSON={cfg["GOOGLE_ACCOUNT_JSON"]}' \
          --conf spark.kubernetes.driverEnv.GOOGLE_SHEET_ID={cfg["GOOGLE_SHEET_ID"]} \
          --conf spark.kubernetes.driverEnv.DOWNTIMES_GOOGLE_SHEET_ID={cfg["DOWNTIMES_GOOGLE_SHEET_ID"]} \
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
          local:///opt/spark/work-dir/src/{script}\
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
    "on_failure_callback":email_fail_alert
}

bronze_dag = DAG(
    dag_id="management_dag",
    default_args=default_args,
    schedule="0 1 * * *",
    tags=["management_DAG_ingestion", "prod", "management_data_product"],
)

cfg = get_connection_properties(bronze_dag)

# (task_name, spark_job_name, script, image)
# image=None uses cfg["docker_image"]; _LEGACY_IMAGE tasks pin to a specific image tag
TASKS = [
    ("jira_google_sheet_ingestion",  "jira_google_sheet_ingestion-ingestion","bronze/python/bronze_jira_ingestion.py",  None),
    ("downtimes_google_sheet_ingestion",  "downtimes_google_sheet_ingestion-ingestion","bronze/python/bronze_downtimes_ingestion.py",  None),
    ("jira_google_sheet_silver",  "jira_google_sheet_silver-ingestion","silver/python/silver_gestao_jira.py",  None),
    ("downtimes_google_sheet_silver",  "downtimes_google_sheet_silver-ingestion","silver/python/silver_gestao_downtimes.py",  None),
    ("jira_google_sheet_gold",  "jira_google_sheet_gold-ingestion","gold/python/gold_gestao_jira.py",  None),
    ("downtimes_google_sheet_gold",  "downtimes_google_sheet_gold-ingestion","gold/python/gold_gestao_downtimes.py",  None),
]

tasks = [make_ingestion_task(cfg, *task) for task in TASKS]

for upstream, downstream in zip(tasks, tasks[1:]):
    upstream >> downstream  # type: ignore