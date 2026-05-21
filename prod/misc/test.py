from airflow import DAG  # type: ignore
from datetime import datetime
import requests #type: ignore
import json
import base64
from airflow.sdk import Variable, Connection  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
import smtplib
from email.message import EmailMessage

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level to log
    format="%(asctime)s - %(levelname)s - %(message)s"
)

_LEGACY_IMAGE = "nauedu/nau-analytics-spark-shell:d465952"


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level to log
    format="%(asctime)s - %(levelname)s - %(message)s"
)

_LEGACY_IMAGE = "nauedu/nau-analytics-spark-shell:d465952"
def email_fail_alert(context):
    smtp_conn = Connection.get("smtp_error_email")
    smth_host = smtp_conn.host
    smtp_port = smtp_conn.port
    sender = smtp_conn.extra_dejson.get("from_email")
    receiver = smtp_conn.extra_dejson.get("to")
    cc_list = smtp_conn.extra_dejson.get("cc1", [])
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = getattr(ti, "run_id", "unknown")
    execution_time = getattr(ti, "start_date", "unknown")

    env = Variable.get("ENVIRONMENT")

    subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"
    content = f"""
    🚨 Airflow Task Failed!
    
       - DAG: {dag_id}
       - Task:{task_id}
       - Run ID: {run_id}
       - Execution Time: {execution_time}
       - environment: {env}
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver
    msg["Cc"] = ", ".join(cc_list)
    msg.set_content(content)
    logging.info("Sending Airflow failure alert email")
    try:
        server = smtplib.SMTP(smth_host, smtp_port, timeout=10)
        server.ehlo()
        logging.info("SMTP connection successful")
        server.send_message(msg)
        logging.info("email sent successful")
        server.quit()
    except Exception as e:
        logging.error(f"Connection failed: {e}")


def task_fail_alert(context):
    TEAMS_WEBHOOK_URL = Connection.get("WEBHOOK_URL").password
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_time = getattr(ti, "start_date", "unknown")
    run_id = getattr(ti, "run_id", "unknown")
    try_number = getattr(ti, "try_number", "unknown")
    env = Variable.get("ENVIRONMENT")
    message = {
    "text": f"🚨 **Airflow Task Failed!**\n\n **DAG:** {dag_id}\n\n **Task:** {task_id}\n\n **Run ID:** {run_id}\n\n **Execution Time:** {execution_time}\n\n **Try:** {try_number}\n\n environment: {env}\n\n"
        
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

def send_error_alerts(context):
    email_fail_alert(context=context)
    #task_fail_alert(context=context)


def email_sucess_alert(context):
    smtp_conn = Connection.get("smtp_error_email")
    smth_host = smtp_conn.host
    smtp_port = smtp_conn.port
    sender = smtp_conn.extra_dejson.get("from_email")
    receiver = smtp_conn.extra_dejson.get("to")
    cc_list = smtp_conn.extra_dejson.get("cc1", [])
    ti = context["task_instance"]
    dag_id = ti.dag_id
    run_id = getattr(ti, "run_id", "unknown")
    execution_time = getattr(ti, "start_date", "unknown")

    env = Variable.get("ENVIRONMENT")

    subject = f"✔️ Airflow DAG Completed Successfully!: {dag_id}"
    content = f"""
    ✔️ Airflow DAG Completed Successfully!
    
       - DAG: {dag_id}
       - Run ID: {run_id}
       - Execution Time: {execution_time}
       - environment: {env}
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(cc_list)
    msg["Cc"] = ", ".join(cc_list)
    msg.set_content(content)
    logging.info("Sending Airflow failure alert email")
    try:
        server = smtplib.SMTP(smth_host, smtp_port, timeout=10)
        server.ehlo()
        logging.info("SMTP connection successful")
        server.send_message(msg)
        logging.info("email sent successful")
        server.quit()
    except Exception as e:
        logging.error(f"Connection failed: {e}")

def dag_sucess_alert(context):
    TEAMS_WEBHOOK_URL = Connection.get("WEBHOOK_URL").password
    ti = context["task_instance"]
    dag_id = ti.dag_id
    execution_time = getattr(ti, "start_date", "unknown")
    run_id = getattr(ti, "run_id", "unknown")
    try_number = getattr(ti, "try_number", "unknown")
    env = Variable.get("ENVIRONMENT")
    message = {
    "text": f"✔️ **Airflow DAG Completed Successfully!** \n\n **DAG:** {dag_id}\n\n **Run ID:** {run_id}\n\n **Execution Time:** {execution_time}\n\n **Try:** {try_number}\n\n environment: {env}\n\n"
        
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

def send_sucess_alerts(context):
    email_sucess_alert(context=context)
    #dag_sucess_alert(context=context)

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


def make_misc_task(
    cfg: dict,
    task_name: str,
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
            echo "hello" | return 0 
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
    "on_failure_callback":send_error_alerts,
    "on_success_callback":send_sucess_alerts,
}

bronze_dag = DAG(
    dag_id="misc_dag",
    default_args=default_args,
    schedule=None,
    tags=["management_DAG_ingestion", "prod", "management_data_product"],
)

cfg = get_connection_properties(bronze_dag)

# (task_name, spark_job_name, script, image)
# image=None uses cfg["docker_image"]; _LEGACY_IMAGE tasks pin to a specific image tag
task = make_misc_task(cfg=cfg,task_name="misc")
task #type: ignore