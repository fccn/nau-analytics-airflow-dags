from airflow import DAG  # type: ignore
from datetime import datetime
from typing import Optional
from airflow.sdk import Variable, Connection  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
from kubernetes.client import V1ResourceRequirements


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
            "TABLES_TO_RUN": Variable.get("TABLES_TO_RUN", default_var=""),
            "CACHE_FACT_TABLES": Variable.get("CACHE_FACT_TABLES", default_var="true"),
        }
    except Exception as e:
        raise Exception(f"Could not get the variables or secrets: {e}")


def make_gold_operator(
    cfg: dict,
    name: str,
    script: str,
    executor_cores: int = 2,
    executor_instances: int = 1,
    pod_image: Optional[str] = None,
) -> KubernetesPodOperator:
    image = pod_image or cfg["docker_image"]

    driver_memory = "8g"
    executor_memory = "8g"
    # FIX 8 (NEW): Increased memoryOverhead from 2g → 4g.
    # The original 2g overhead was insufficient for large shuffle operations,
    # causing repeated OOM kills (exit code 137) on executors during the
    # fact_conclusion_rate_agg and enrollments_vs_certificates_agg writes.
    # Kubernetes total per executor = executor_memory + memoryOverhead = 12g.
    memory_overhead = "4g"

    return KubernetesPodOperator(
        namespace=cfg["namespace"],
        service_account_name="spark-role",
        image=image,
        startup_timeout_seconds=600,
        # FIX 1: Raised the Airflow submit-pod memory limit from 1Gi → 2Gi.
        # The 1Gi limit was dangerously close to what spark-submit needs to
        # bootstrap on a large cluster. If the pod was OOM-killed by Kubernetes
        # before Spark exited, Airflow would mark the task failed even if Spark
        # completed successfully. CPU raised to 2 cores to keep the submit
        # process responsive during long Spark runs.
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
          --conf spark.kubernetes.driverEnv.TABLES_TO_RUN={cfg["TABLES_TO_RUN"]} \
          --conf spark.kubernetes.driverEnv.CACHE_FACT_TABLES={cfg["CACHE_FACT_TABLES"]} \
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
    "email": ["paulo.r.monteiro@glinttglobal.com", "vitor.pina@glinttglobal.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

gold_dag = DAG(
    dag_id="gold_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["gold_table_transform", "prod"],
)

cfg = get_connection_properties(gold_dag)

# ── Task definitions ──────────────────────────────────────────────────────────
#
# Resource sizing rationale:
#
#   Total task slots = executor_instances × executor_cores
#
#   The compaction phase (rewrite_data_files) is the dominant cost for the two
#   heaviest tasks. The Python scripts now set max-concurrent-file-group-rewrites=20,
#   so those tasks need at least 20 task slots to run compaction groups in parallel.
#   Giving them more slots also helps the CTAS / partition-overwrite write phase.
#
#   Memory is NOT the bottleneck — logs showed 4.6 GiB free on every executor
#   throughout both jobs, so executor_memory stays at 8g.
#   FIX 8: memoryOverhead raised from 2g → 4g to prevent OOM kills during
#   large shuffle operations (Kubernetes limit = 8g + 4g = 12g per executor).
#
#   Tasks that are lightweight (dim tables, simple facts) keep minimal resources
#   so they don't hold Kubernetes node capacity unnecessarily.

# Light-weight dimension tables — 1 executor is sufficient.
dim_time_task = make_gold_operator(
    cfg, "dim_time_gold", "gold_dim_time.py",
    executor_cores=1, executor_instances=1,
)
dim_organization_task = make_gold_operator(
    cfg, "dim_organization_gold", "gold_dim_organization.py",
    executor_cores=1, executor_instances=1,
)
dim_course_edition_task = make_gold_operator(
    cfg, "dim_course_edition_gold", "gold_dim_course_edition.py",
    executor_cores=2, executor_instances=1,
)

# FIX 2: dim_user dropped from executor_instances=2 → 1.
# It's an SCD2 dimension that completes quickly regardless of instance count.
# The extra executor was wasted capacity that is better reserved for the
# fact and aggregation tasks below.
dim_user_task = make_gold_operator(
    cfg, "dim_user_gold", "gold_dim_user.py",
    executor_cores=2, executor_instances=1,
)

# Medium-weight fact tables — 2 executors (8 slots) is appropriate.
fact_certificate_d_task = make_gold_operator(
    cfg, "fact_certificate_d_gold", "gold_fact_certificate_d.py",
    executor_cores=2, executor_instances=2,
)
fact_student_grades_task = make_gold_operator(
    cfg, "fact_student_grades_gold", "gold_fact_student_grades.py",
    executor_cores=2, executor_instances=2,
)
fact_course_edition_daily_task = make_gold_operator(
    cfg, "fact_course_edition_daily_gold", "gold_fact_course_edition_daily.py",
    executor_cores=2, executor_instances=2,
)

# FIX 3: fact_course_enrollment_daily scaled up from executor_instances=2 → 4,
# executor_cores default (2) → 4. Total slots: 4×4 = 16.
#
# This task had 2,672 Iceberg file groups to compact with only 4 task slots
# (2 instances × 2 cores), making the compaction almost entirely serial.
# With max-concurrent-file-group-rewrites=20 set in the Python script, we need
# at least 20 slots for that to have any effect. 16 slots is a safe step up
# that fits within a typical node's capacity without over-provisioning; tune
# to 20+ (e.g. instances=5, cores=4) if compaction is still the bottleneck.
fact_course_enrollment_daily_task = make_gold_operator(
    cfg, "fact_course_enrollment_daily_gold", "gold_fact_course_enrollment_d.py",
    executor_cores=4, executor_instances=4,
)

# FIX 4: gold_reporting_agg_tables scaled from executor_instances=3 (effective 2),
# executor_cores=2 → executor_instances=6, executor_cores=4. Total slots: 6×4 = 24.
#
# The original allocation gave only 4 usable task slots (the 3rd executor appeared
# to be a late Kubernetes scheduler arrival), capping max compaction parallelism
# at 4 regardless of the max-concurrent-file-group-rewrites setting. This task
# processes 1,182+ file groups across two aggregation tables; 24 slots allows
# the compaction to run 20 groups truly in parallel, plus leaves headroom for
# the concurrent partition-overwrite write phase.
gold_reporting_agg_tables_task = make_gold_operator(
    cfg, "gold_reporting_agg_tables", "gold_reporting_agg_tables.py",
    executor_cores=4, executor_instances=6,
)

# ── Dependency graph ──────────────────────────────────────────────────────────
#
#  dim_time ──────────────────────────────────────────────────────────────────┐
#  dim_user ──────────────────────────────────────────────┐                  │
#  dim_organization ──► dim_course_edition ───────────────┤                  │
#                                                         ▼                  │
#                              fact_certificate_d ─────────────────────────┐ │
#                              fact_student_grades ──────────────────────┐ │ │
#                              fact_course_edition_daily ──────────────┐ │ │ │
#                              fact_course_enrollment_daily ──────────┐│ │ │ │
#                                                                     ▼▼ ▼ ▼ ▼
#                                                         gold_reporting_agg_tables

# Level 1: dim_course_edition needs dim_organization
dim_organization_task >> dim_course_edition_task

# Level 2: facts need dim_user + dim_course_edition
[dim_user_task, dim_course_edition_task] >> fact_certificate_d_task
[dim_user_task, dim_course_edition_task] >> fact_student_grades_task
[dim_course_edition_task, dim_time_task] >> fact_course_edition_daily_task
[dim_user_task, dim_course_edition_task] >> fact_course_enrollment_daily_task

# Level 3: aggregations need all facts
[
    fact_certificate_d_task,
    fact_student_grades_task,
    fact_course_edition_daily_task,
    fact_course_enrollment_daily_task,
] >> gold_reporting_agg_tables_task  # type: ignore
