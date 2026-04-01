from airflow import DAG
from datetime import datetime
from airflow.sdk import Variable, Connection
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes import client, config as k8s_config  # type: ignore

def get_connection_properties(dag: DAG) -> dict:
    try:
        config = {}
        config["docker_image"] = Variable.get("docker_image")
        config["dag"] = dag
        config["namespace"] = Variable.get("namespace")

        mysql_conn = Connection.get("sql_source_stage_connection")
        config["database"] = mysql_conn.extra_dejson.get("mysqldatabase")
        config["host"] = mysql_conn.host
        config["user"] = mysql_conn.login
        config["port"] = mysql_conn.port
        config["secret"] = mysql_conn.password

        s3_conn = Connection.get("s3_stage_connection")
        config["S3_ACCESS_KEY"] = s3_conn.login
        config["S3_SECRET_KEY"] = s3_conn.password
        config["S3_ENDPOINT"] = s3_conn.extra_dejson.get("s3endpoint")

        iceberg_catalog_conn = Connection.get("iceberg_stage_connection")
        config["ICEBERG_CATALOG_HOST"] = iceberg_catalog_conn.host
        config["ICEBERG_CATALOG_PORT"] = iceberg_catalog_conn.port
        config["ICEBERG_CATALOG_USER"] = iceberg_catalog_conn.login
        config["ICEBERG_CATALOG_PASSWORD"] = iceberg_catalog_conn.password

        config["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"] = iceberg_catalog_conn.extra_dejson.get("bronze_iceberg_database_catalog_name")
        config["BRONZE_ICEBERG_CATALOG_NAME"] = iceberg_catalog_conn.extra_dejson.get("bronze_iceberg_catalog_name")
        config["BRONZE_ICEBERG_CATALOG_WAREHOUSE"] = iceberg_catalog_conn.extra_dejson.get("bronze_iceberg_catalog_warehouse")

        config["SILVER_ICEBERG_DATABASE_CATALOG_NAME"] = iceberg_catalog_conn.extra_dejson.get("silver_iceberg_database_catalog_name")
        config["SILVER_ICEBERG_CATALOG_NAME"] = iceberg_catalog_conn.extra_dejson.get("silver_iceberg_catalog_name")
        config["SILVER_ICEBERG_CATALOG_WAREHOUSE"] = iceberg_catalog_conn.extra_dejson.get("silver_iceberg_catalog_warehouse")

        config["GOLD_ICEBERG_DATABASE_CATALOG_NAME"] = iceberg_catalog_conn.extra_dejson.get("gold_iceberg_database_catalog_name")
        config["GOLD_ICEBERG_CATALOG_NAME"] = iceberg_catalog_conn.extra_dejson.get("gold_iceberg_catalog_name")
        config["GOLD_ICEBERG_CATALOG_WAREHOUSE"] = iceberg_catalog_conn.extra_dejson.get("gold_iceberg_catalog_warehouse")

        config["ENVIRONMENT"] = Variable.get("ENVIRONMENT")
        return config
    except Exception:
        raise Exception(f"Could not get the variables or secrets: {Exception}")

def delete_spark_driver_pod(app_name: str, namespace: str):
    def callback(context):
        try:
            k8s_config.load_incluster_config()
            v1 = client.CoreV1Api()

            # List ALL pods and log them so we can see what labels exist
            all_pods = v1.list_namespaced_pod(namespace=namespace)
            print(f"[cleanup] All pods in namespace '{namespace}':")
            for pod in all_pods.items:
                print(f"  - {pod.metadata.name} | phase: {pod.status.phase} | labels: {pod.metadata.labels}")

            # Try the label we think is correct
            label_selector = f"spark-app-name={app_name}"
            print(f"[cleanup] Searching with label_selector: '{label_selector}'")
            pods = v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
            print(f"[cleanup] Found {len(pods.items)} pod(s) matching label")

            for pod in pods.items:
                print(f"[cleanup] Deleting: {pod.metadata.name} (phase: {pod.status.phase})")
                v1.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=namespace,
                    body=client.V1DeleteOptions(grace_period_seconds=0)
                )
                print(f"[cleanup] Deleted: {pod.metadata.name}")

        except Exception as e:
            import traceback
            print(f"[cleanup] ERROR: {e}")
            print(traceback.format_exc())
    return callback


def build_spark_submit(cfg: dict, app_name: str, script_path: str) -> str:
    return f"""
        spark-submit \\
          --master k8s://https://kubernetes.default.svc:443 \\
          --deploy-mode cluster \\
          --name {app_name} \\
          --conf spark.kubernetes.container.image={cfg['docker_image']} \\
          --conf spark.kubernetes.namespace={cfg['namespace']} \\
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \\
          --conf spark.kubernetes.submission.waitAppCompletion=true \\
          --conf spark.executor.instances=2 \\
          --conf spark.executor.cores=1 \\
          --conf spark.executor.memory=8g \\
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg['ENVIRONMENT']} \\
          --conf spark.kubernetes.driverEnv.MYSQL_DATABASE={cfg['database']} \\
          --conf spark.kubernetes.driverEnv.MYSQL_HOST={cfg['host']} \\
          --conf spark.kubernetes.driverEnv.MYSQL_PORT={cfg['port']} \\
          --conf spark.kubernetes.driverEnv.MYSQL_USER={cfg['user']} \\
          --conf spark.kubernetes.driverEnv.MYSQL_SECRET={cfg['secret']} \\
          --conf spark.kubernetes.driverEnv.S3_ACCESS_KEY={cfg['S3_ACCESS_KEY']} \\
          --conf spark.kubernetes.driverEnv.S3_SECRET_KEY={cfg['S3_SECRET_KEY']} \\
          --conf spark.kubernetes.driverEnv.S3_ENDPOINT={cfg['S3_ENDPOINT']} \\
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_HOST={cfg['ICEBERG_CATALOG_HOST']} \\
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PORT={cfg['ICEBERG_CATALOG_PORT']} \\
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_USER={cfg['ICEBERG_CATALOG_USER']} \\
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PASSWORD={cfg['ICEBERG_CATALOG_PASSWORD']} \\
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_DATABASE_CATALOG_NAME={cfg['BRONZE_ICEBERG_DATABASE_CATALOG_NAME']} \\
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_CATALOG_NAME={cfg['BRONZE_ICEBERG_CATALOG_NAME']} \\
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_CATALOG_WAREHOUSE={cfg['BRONZE_ICEBERG_CATALOG_WAREHOUSE']} \\
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_DATABASE_CATALOG_NAME={cfg['SILVER_ICEBERG_DATABASE_CATALOG_NAME']} \\
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_CATALOG_NAME={cfg['SILVER_ICEBERG_CATALOG_NAME']} \\
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_CATALOG_WAREHOUSE={cfg['SILVER_ICEBERG_CATALOG_WAREHOUSE']} \\
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_DATABASE_CATALOG_NAME={cfg['GOLD_ICEBERG_DATABASE_CATALOG_NAME']} \\
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_CATALOG_NAME={cfg['GOLD_ICEBERG_CATALOG_NAME']} \\
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_CATALOG_WAREHOUSE={cfg['GOLD_ICEBERG_CATALOG_WAREHOUSE']} \\
          --conf spark.kubernetes.driver.deleteOnTermination=true \\
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \\
          --conf spark.kubernetes.executor.deleteOnTermination=true \\
          --conf spark.kubernetes.container.image.pullPolicy=Always \\
          local:///opt/spark/work-dir/src/bronze/python/{script_path} \\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
    """


def make_spark_task(
    cfg: dict,
    app_name: str,
    task_id: str,
    pod_name: str,
    script_path: str,
) -> KubernetesPodOperator:
    cleanup = delete_spark_driver_pod(app_name, cfg["namespace"])
    return KubernetesPodOperator(
        namespace=cfg["namespace"],
        service_account_name="spark-role",
        image=cfg["docker_image"],
        startup_timeout_seconds=600,
        cmds=["/bin/bash", "-c"],
        arguments=[build_spark_submit(cfg, app_name, script_path)],
        name=pod_name,
        task_id=task_id,
        get_logs=True,
        on_finish_action="delete_pod",
        on_success_callback=cleanup,
        on_failure_callback=cleanup,
        dag=cfg["dag"],
    )


default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
}

bronze_dag_test = DAG(
    dag_id="bronze_dag_ing_test",
    default_args=default_args,
    schedule="0 3 * * *",
    tags=["bronze_table_ingestion", "stage"],
)

cfg = get_connection_properties(bronze_dag_test)

tasks = [
    ("course_overviews_courseoverview-ingestion",       "course_overviews_courseoverview_ingestion_1",       "course_overviews_courseoverview_ingestion",       "bronze_course_overviews_courseoverview_ingestion.py"),
    ("certificates_generatedcertificate_ingestion",     "certificates_generatedcertificate_ingestion_1",     "certificates_generatedcertificate_ingestion",     "bronze_certificates_generatedcertificate_ingestion.py"),
    ("grades_persistentcoursegrade-ingestion",          "grades_persistentcoursegrade_ingestion_1",          "grades_persistentcoursegrade_ingestion",          "bronze_grades_persistentcoursegrade_ingestion.py"),
    ("auth_user_ingestion",                             "auth_user_ingestion_1",                             "auth_user_ingestion",                             "bronze_auth_user_ingestion.py"),
    ("bronze_auth_userprofile_ingestion",               "auth_userprofile_ingestion_1",                      "auth_userprofile_ingestion",                      "bronze_auth_userprofile_ingestion.py"),
    ("organizations_organization-ingestion",            "organizations_organization_ingestion_1",            "organizations_organization_ingestion",            "bronze_organizations_organization_ingestion.py"),
    ("student_courseaccessrole-ingestion",              "student_courseaccessrole_ingestion_1",              "student_courseaccessrole_ingestion",              "bronze_student_courseaccessrole_ingestion.py"),
    ("student_courseenrollment-ingestion",              "student_courseenrollment_ingestion_1",              "student_courseenrollment_ingestion",              "bronze_student_courseenrollment_ingestion.py"),
    ("student_userattribute-ingestion",                 "student_userattribute_ingestion_1",                 "student_userattribute_ingestion",                 "bronze_student_userattribute_ingestion.py"),
    ("organizations_ho_ingestion",                      "organizations_ho_ingestion_1",                      "organizations_ho_ingestion",                      "bronze_organizations_ho_ingestion.py"),
    ("student_courseenrollment_history_ingestion",      "student_courseenrollment_history_ingestion_1",      "student_courseenrollment_history_ingestion",      "bronze_student_courseenrollment_history_ingestion.py"),
]

task_objects = [
    make_spark_task(cfg, app_name, task_id, pod_name, script)
    for app_name, task_id, pod_name, script in tasks
]

for i in range(len(task_objects) - 1):
    task_objects[i] >> task_objects[i + 1]  # type: ignore