"""
One-off, manually-triggered DAG for fccn/nau-technical#981.

Runs src/gold/backfill_certificate_verify_uuid_mode.py (in
nau-analytics-external-data-product) exactly once, to backfill
verify_uuid (silver + gold) and mode (gold) for certificate rows that were
written before those columns existed in silver/gold -- see that script's
own docstring for the full rationale and the dedup/re-runnable-safety
notes.

Unlike prod/stage, dev does not have a recurring gold_dag.py/silver_dag.py
for external_data_product today (confirmed live: only the bronze ingestion
DAGs are registered here; dev's gold_local/silver_local certificate tables
were populated by earlier one-off runs). This DAG follows this folder's own
existing pattern instead (test_task_dag.py / ingestion_dag.py: hardcoded
"dev-nau-analytics" namespace, s3_dev_connection/iceberg_dev_connection)
rather than mirroring prod/stage's structure.

Safe to trigger more than once if needed -- the underlying script only
updates rows that are still NULL.
"""
from airflow import DAG  # type: ignore
from datetime import datetime
from airflow.sdk import Variable, Connection  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
from kubernetes.client import V1ResourceRequirements  # type: ignore


def get_connection_properties(dag: DAG) -> dict:
    try:
        config = {}
        config["docker_image"] = Variable.get(
            "backfill_certificate_docker_image", default=None
        ) or Variable.get("docker_image")
        config["ENVIRONMENT"] = Variable.get("ENVIRONMENT")
        config["dag"] = dag
        config["namespace"] = "dev-nau-analytics"

        s3_conn = Connection.get("s3_dev_connection")
        config["S3_ACCESS_KEY"] = s3_conn.login
        config["S3_SECRET_KEY"] = s3_conn.password
        config["S3_ENDPOINT"] = s3_conn.extra_dejson.get("s3endpoint")

        iceberg_catalog_conn = Connection.get("iceberg_dev_connection")
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

        return config
    except Exception as e:
        raise Exception(f"Could not get the variables or secrets: {e}")


def backfill_certificate_verify_uuid_mode(cfg: dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        namespace=cfg["namespace"],
        service_account_name="spark-role",
        image=cfg["docker_image"],
        startup_timeout_seconds=600,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "2", "memory": "2Gi"},
        ),
        cmds=["/bin/bash", "-c"],
        arguments=[
            f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name backfill_certificate_verify_uuid_mode \
          --conf spark.kubernetes.container.image={cfg["docker_image"]} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.driver.cores=2 \
          --conf spark.driver.memory=8g \
          --conf spark.driver.memoryOverhead=4g \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=2 \
          --conf spark.executor.memory=8g \
          --conf spark.executor.memoryOverhead=4g \
          --conf spark.kubernetes.driver.request.cores=2 \
          --conf spark.kubernetes.driver.limit.cores=4 \
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg["ENVIRONMENT"]} \
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
          local:///opt/spark/work-dir/src/gold/backfill_certificate_verify_uuid_mode.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
        ],
        name="backfill_certificate_verify_uuid_mode",
        task_id="backfill_certificate_verify_uuid_mode_1",
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

backfill_certificate_dag = DAG(
    dag_id="backfill_certificate_verify_uuid_mode_dag",
    default_args=default_args,
    schedule=None,
    tags=["one_off", "backfill", "dev"],
)

cfg = get_connection_properties(backfill_certificate_dag)
backfill_certificate_task = backfill_certificate_verify_uuid_mode(cfg=cfg)
backfill_certificate_task  # type: ignore
