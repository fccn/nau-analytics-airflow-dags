from airflow import DAG  # type: ignore
from datetime import datetime
from airflow.sdk import Variable, Connection  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
from kubernetes.client import V1ResourceRequirements

_LEGACY_IMAGE = "nauedu/nau-analytics-spark-shell:d465952"


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


def make_gold_operator(cfg: dict, name: str, script: str, executor_cores: int = 2, executor_instances: int = 1, pod_image: str | None = None) -> KubernetesPodOperator:
    image = pod_image or cfg["docker_image"]

    driver_memory = "8g"
    executor_memory = "8g"
    memory_overhead = "2g"

    return KubernetesPodOperator(
        namespace=cfg["namespace"],
        service_account_name="spark-role",
        image=image,
        startup_timeout_seconds=600,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        cmds=["/bin/bash", "-c"],
        arguments=[
            f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name {name} \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.driver.cores=2 \
          --conf spark.driver.memory={driver_memory} \
          --conf spark.driver.memoryOverhead={memory_overhead} \
          --conf spark.executor.instances={executor_instances} \
          --conf spark.executor.cores={executor_cores} \
          --conf spark.executor.memory={executor_memory} \
          --conf spark.executor.memoryOverhead={memory_overhead} \
          --conf spark.kubernetes.driver.request.cores=2 \
          --conf spark.kubernetes.driver.limit.cores=4 \
          --conf spark.kubernetes.executor.request.cores={executor_cores} \
          --conf spark.kubernetes.executor.limit.cores={executor_cores * 2} \
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg["ENVIRONMENT"]} \
          --conf spark.kubernetes.driverEnv.MYSQL_DATABASE={cfg["database"]} \
          --conf spark.kubernetes.driverEnv.MYSQL_HOST={cfg["host"]} \
          --conf spark.kubernetes.driverEnv.MYSQL_PORT={cfg["port"]} \
          --conf spark.kubernetes.driverEnv.MYSQL_USER={cfg["user"]} \
          --conf spark.kubernetes.driverEnv.MYSQL_SECRET={cfg["secret"]} \
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
          local:///opt/spark/work-dir/src/gold/{script}\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
        ],
        name=name,
        task_id=f"{name}_1",
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

gold_dag = DAG(
    dag_id="gold_dag",
    default_args=default_args,
    schedule="0 5 * * *",
    tags=["gold_table_transform", "prod"],
)

cfg = get_connection_properties(gold_dag)

dim_time_task                     = make_gold_operator(cfg, "dim_time_gold",                   "gold_dim_time.py",                  executor_cores=1)
dim_user_task                     = make_gold_operator(cfg, "dim_user_gold",                   "gold_dim_user.py",                  executor_instances = 2)
dim_organization_task             = make_gold_operator(cfg, "dim_organization_gold",            "gold_dim_organization.py")
dim_course_edition_task           = make_gold_operator(cfg, "dim_course_edition_gold",          "gold_dim_course_edition.py")
fact_certificate_d_task           = make_gold_operator(cfg, "fact_certificate_d_gold",          "gold_fact_certificate_d.py")
fact_student_grades_task          = make_gold_operator(cfg, "fact_student_grades_gold",         "gold_fact_student_grades.py")
fact_course_edition_daily_task    = make_gold_operator(cfg, "fact_course_edition_daily_gold",   "gold_fact_course_edition_daily.py")
fact_course_enrollment_daily_task = make_gold_operator(cfg, "fact_course_enrollment_daily_gold","gold_fact_course_enrollment_d.py", executor_instances = 2)

dim_time_task >> dim_user_task >> dim_organization_task >> dim_course_edition_task >> fact_certificate_d_task >> fact_student_grades_task >> fact_course_edition_daily_task >> fact_course_enrollment_daily_task  # type: ignore
