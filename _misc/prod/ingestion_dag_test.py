from airflow import DAG
from datetime import datetime
from airflow.sdk import Variable, Connection
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes import client, config as k8s_config  # type: ignore


def get_connection_properties(dag: DAG) -> dict:
    try:
        mysql_conn = Connection.get("sql_source_prod_connection")
        s3_conn = Connection.get("s3_prod_connection")
        iceberg_conn = Connection.get("iceberg_prod_connection")
        iceberg_extra = iceberg_conn.extra_dejson
        return {
            "dag": dag,
            "docker_image": Variable.get("docker_image"),
            "namespace": Variable.get("namespace"),
            "ENVIRONMENT": Variable.get("ENVIRONMENT"),
            "database": mysql_conn.extra_dejson.get("mysqldatabase"),
            "host": mysql_conn.host,
            "user": mysql_conn.login,
            "port": mysql_conn.port,
            "secret": mysql_conn.password,
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


def make_driver_cleanup_task(app_name: str, namespace: str, dag: DAG, task_id: str) -> PythonOperator:
    """
    Dedicated Airflow task that runs after the Spark task and deletes
    the driver pod. Runs on the Airflow worker — no image dependency.
    """
    def delete_driver(**kwargs):
        import time
        k8s_config.load_incluster_config()
        v1 = client.CoreV1Api()
        
        # Debug: print which namespace we're searching and list ALL pods there
        print(f"[cleanup] searching namespace: '{namespace}'")
        try:
            all_pods = v1.list_namespaced_pod(namespace)
            print(f"[cleanup] total pods in namespace: {len(all_pods.items)}")
            for pod in all_pods.items:
                print(f"[cleanup]   {pod.metadata.name} | {pod.status.phase} | labels: {pod.metadata.labels}")
        except Exception as e:
            print(f"[cleanup] ERROR listing pods: {e}")
            # Try listing across ALL namespaces to confirm connectivity
            try:
                all_pods = v1.list_pod_for_all_namespaces()
                print(f"[cleanup] total pods cluster-wide: {len(all_pods.items)}")
                for pod in all_pods.items:
                    if "driver" in pod.metadata.name:
                        print(f"[cleanup] driver pod found in ns={pod.metadata.namespace}: {pod.metadata.name}")
            except Exception as e2:
                print(f"[cleanup] ERROR listing all pods: {e2}")

    return PythonOperator(
        task_id=task_id,
        python_callable=delete_driver,
        dag=dag,
    )


def build_spark_submit(cfg: dict, app_name: str, script_path: str) -> str:
    return f"""
        spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name {app_name} \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg['namespace']} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg['ENVIRONMENT']} \
          --conf spark.kubernetes.driverEnv.MYSQL_DATABASE={cfg['database']} \
          --conf spark.kubernetes.driverEnv.MYSQL_HOST={cfg['host']} \
          --conf spark.kubernetes.driverEnv.MYSQL_PORT={cfg['port']} \
          --conf spark.kubernetes.driverEnv.MYSQL_USER={cfg['user']} \
          --conf spark.kubernetes.driverEnv.MYSQL_SECRET={cfg['secret']} \
          --conf spark.kubernetes.driverEnv.S3_ACCESS_KEY={cfg['S3_ACCESS_KEY']} \
          --conf spark.kubernetes.driverEnv.S3_SECRET_KEY={cfg['S3_SECRET_KEY']} \
          --conf spark.kubernetes.driverEnv.S3_ENDPOINT={cfg['S3_ENDPOINT']} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_HOST={cfg['ICEBERG_CATALOG_HOST']} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PORT={cfg['ICEBERG_CATALOG_PORT']} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_USER={cfg['ICEBERG_CATALOG_USER']} \
          --conf spark.kubernetes.driverEnv.ICEBERG_CATALOG_PASSWORD={cfg['ICEBERG_CATALOG_PASSWORD']} \
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_DATABASE_CATALOG_NAME={cfg['BRONZE_ICEBERG_DATABASE_CATALOG_NAME']} \
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_CATALOG_NAME={cfg['BRONZE_ICEBERG_CATALOG_NAME']} \
          --conf spark.kubernetes.driverEnv.BRONZE_ICEBERG_CATALOG_WAREHOUSE={cfg['BRONZE_ICEBERG_CATALOG_WAREHOUSE']} \
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_DATABASE_CATALOG_NAME={cfg['SILVER_ICEBERG_DATABASE_CATALOG_NAME']} \
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_CATALOG_NAME={cfg['SILVER_ICEBERG_CATALOG_NAME']} \
          --conf spark.kubernetes.driverEnv.SILVER_ICEBERG_CATALOG_WAREHOUSE={cfg['SILVER_ICEBERG_CATALOG_WAREHOUSE']} \
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_DATABASE_CATALOG_NAME={cfg['GOLD_ICEBERG_DATABASE_CATALOG_NAME']} \
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_CATALOG_NAME={cfg['GOLD_ICEBERG_CATALOG_NAME']} \
          --conf spark.kubernetes.driverEnv.GOLD_ICEBERG_CATALOG_WAREHOUSE={cfg['GOLD_ICEBERG_CATALOG_WAREHOUSE']} \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/{script_path}
    """


def make_spark_task(cfg: dict, app_name: str, task_id: str, pod_name: str, script_path: str) -> KubernetesPodOperator:
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
    tags=["bronze_table_ingestion", "prod"],
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

# Build interleaved spark + cleanup task chain
all_tasks = []
for app_name, task_id, pod_name, script in tasks:
    spark_task = make_spark_task(cfg, app_name, task_id, pod_name, script)
    cleanup_task = make_driver_cleanup_task(
        app_name=app_name,
        namespace=cfg["namespace"],
        dag=bronze_dag_test,
        task_id=f"cleanup_{task_id}",
    )
    all_tasks.append(spark_task)
    all_tasks.append(cleanup_task)

# Chain: spark1 >> cleanup1 >> spark2 >> cleanup2 >> ...
for i in range(len(all_tasks) - 1):
    all_tasks[i] >> all_tasks[i + 1]  # type: ignore