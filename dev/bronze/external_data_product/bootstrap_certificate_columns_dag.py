"""
One-off, manually-triggered DAG for fccn/nau-technical#981.

Dev has no recurring gold_dag.py/silver_dag.py for external_data_product
(confirmed live via Trino information_schema on 2026-10-13: dev's
silver_local.entidades.certificates_generatedcertificate has no
verify_uuid column, and gold_local.entidades.fact_certificate_daily has
neither verify_uuid nor mode) -- unlike prod/stage, there is no scheduled
task to wire CERTIFICATE_VERIFY_UUID_MODE_ENABLED=true into, so those
columns' own guarded ALTER TABLE (in
silver_certificates_generatedcertificate.py /
gold_fact_certificate_d.py, see nau-analytics-external-data-product#9)
has never run here.

This DAG runs the two REGULAR, incremental silver and gold scripts --
unchanged, exactly as prod/stage run them on their own daily schedule --
in order (silver then gold -- gold's verify_uuid is sourced from
silver's own, also gated, column), with the flag enabled. Triggering the
ALTER TABLE guard is a side effect of that regular run, not the whole of
what it does: each script also processes any rows pending since its own
last recorded watermark (silver.audit.pipeline_run_ctrl /
gold.audit.pipeline_run_ctrl), the normal way these scripts always work,
and advances that watermark. This DAG does not backfill *historical*
rows that predate column creation -- run
backfill_certificate_verify_uuid_mode_dag once right after this one for
that (its own precondition guard will now pass).

Catch-up scope confirmed live via Trino before merging this PR (2026-10-13):
silver's watermark was 2026-03-18 17:30:04, with bronze (the append-only
source) 2,785 rows ahead of it (1,091,497 bronze rows total vs
1,088,712 already in silver) -- so triggering this DAG appends those
~2,785 pending rows to silver, which is append-only with no dedup (by
design -- see nau-analytics-external-data-product#9's backfill script
docstring for why duplicate ids across ingestions are expected and
handled downstream). Gold's own watermark (2026-03-18 17:30:46) was
already caught up with silver's *pre-run* state (0 pending rows by
ingestion_date), so gold's run in this DAG will process whatever subset
of those same ~2,785 freshly-appended silver rows survive its MERGE
logic -- gold's required dimension tables (dim_user: 628,033 rows,
dim_organization: 172 rows, dim_course_edition: 1,012 rows) are already
populated, so no missing-dimension/NULL-key risk expected.

Safe to trigger more than once -- the ALTER TABLE guard in both scripts
is itself idempotent (`ADD COLUMN IF NOT EXISTS`-equivalent), and
subsequent triggers simply process whatever (if anything) has newly
arrived in bronze since the last run, same as a normal scheduled run
would.
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


def _make_bootstrap_task(
    cfg: dict,
    task_id: str,
    spark_app_name: str,
    script_path: str,
) -> KubernetesPodOperator:
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
          --name {spark_app_name} \
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
          --conf spark.kubernetes.driverEnv.CERTIFICATE_VERIFY_UUID_MODE_ENABLED=true \
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
          local:///opt/spark/work-dir/{script_path}\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
        ],
        name=spark_app_name,
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

bootstrap_certificate_columns_dag = DAG(
    dag_id="bootstrap_certificate_columns_dag",
    default_args=default_args,
    schedule=None,
    tags=["one_off", "bootstrap", "dev"],
)

cfg = get_connection_properties(bootstrap_certificate_columns_dag)

silver_task = _make_bootstrap_task(
    cfg,
    task_id="certificates_generatedcertificate_silver_1",
    spark_app_name="bootstrap_certificates_generatedcertificate_silver",
    script_path="src/silver/silver_certificates_generatedcertificate.py",
)
gold_task = _make_bootstrap_task(
    cfg,
    task_id="fact_certificate_d_gold_1",
    spark_app_name="bootstrap_fact_certificate_d_gold",
    script_path="src/gold/gold_fact_certificate_d.py",
)

silver_task >> gold_task  # type: ignore
