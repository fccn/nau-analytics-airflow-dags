from airflow import DAG #type: ignore
from datetime import datetime
from airflow.sdk import Variable,Connection #type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore
def get_connection_properties(dag: DAG)->dict:
    
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
        config["S3_ENDPOINT"] =s3_conn.extra_dejson.get("s3endpoint")

        iceberg_catalog_conn = Connection.get("iceberg_stage_connection")
        config["ICEBERG_CATALOG_HOST"] =iceberg_catalog_conn.host
        config["ICEBERG_CATALOG_PORT"] = iceberg_catalog_conn.port
        config["ICEBERG_CATALOG_USER"] = iceberg_catalog_conn.login
        config["ICEBERG_CATALOG_PASSWORD"] =iceberg_catalog_conn.password        
       
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

def course_overviews_courseoverview_ingestion(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image=cfg["docker_image"],
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode client \
          --name course_overviews_courseoverview-ingestion \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.driver.host=course-overviews-courseoverview-ingestion-driver \
          --conf spark.driver.port=7078 \
          --conf spark.blockManager.port=7079 \
          --conf spark.kubernetes.driver.service.name=course-overviews-courseoverview-ingestion-driver \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/bronze_course_overviews_courseoverview_ingestion.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    env_vars={
        "ENVIRONMENT": cfg["ENVIRONMENT"],
        "MYSQL_DATABASE": cfg["database"],
        "MYSQL_HOST": cfg["host"],
        "MYSQL_PORT": str(cfg["port"]),
        "MYSQL_USER": cfg["user"],
        "MYSQL_SECRET": cfg["secret"],
        "S3_ACCESS_KEY": cfg["S3_ACCESS_KEY"],
        "S3_SECRET_KEY": cfg["S3_SECRET_KEY"],
        "S3_ENDPOINT": cfg["S3_ENDPOINT"],
        "ICEBERG_CATALOG_HOST": cfg["ICEBERG_CATALOG_HOST"],
        "ICEBERG_CATALOG_PORT": str(cfg["ICEBERG_CATALOG_PORT"]),
        "ICEBERG_CATALOG_USER": cfg["ICEBERG_CATALOG_USER"],
        "ICEBERG_CATALOG_PASSWORD": cfg["ICEBERG_CATALOG_PASSWORD"],
        "BRONZE_ICEBERG_DATABASE_CATALOG_NAME": cfg["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_NAME": cfg["BRONZE_ICEBERG_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_WAREHOUSE": cfg["BRONZE_ICEBERG_CATALOG_WAREHOUSE"],
        "SILVER_ICEBERG_DATABASE_CATALOG_NAME": cfg["SILVER_ICEBERG_DATABASE_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_NAME": cfg["SILVER_ICEBERG_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_WAREHOUSE": cfg["SILVER_ICEBERG_CATALOG_WAREHOUSE"],
        "GOLD_ICEBERG_DATABASE_CATALOG_NAME": cfg["GOLD_ICEBERG_DATABASE_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_NAME": cfg["GOLD_ICEBERG_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_WAREHOUSE": cfg["GOLD_ICEBERG_CATALOG_WAREHOUSE"],
    },
    name="course_overviews_courseoverview_ingestion",
    task_id="course_overviews_courseoverview_ingestion_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def certificates_generatedcertificate_ingestion(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image={cfg['docker_image']},
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode client \
          --name certificates_generatedcertificate_ingestion-ingestion \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/bronze_certificates_generatedcertificate_ingestion.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
        env_vars={
        "ENVIRONMENT": cfg["ENVIRONMENT"],
        "MYSQL_DATABASE": cfg["database"],
        "MYSQL_HOST": cfg["host"],
        "MYSQL_PORT": str(cfg["port"]),
        "MYSQL_USER": cfg["user"],
        "MYSQL_SECRET": cfg["secret"],
        "S3_ACCESS_KEY": cfg["S3_ACCESS_KEY"],
        "S3_SECRET_KEY": cfg["S3_SECRET_KEY"],
        "S3_ENDPOINT": cfg["S3_ENDPOINT"],
        "ICEBERG_CATALOG_HOST": cfg["ICEBERG_CATALOG_HOST"],
        "ICEBERG_CATALOG_PORT": str(cfg["ICEBERG_CATALOG_PORT"]),
        "ICEBERG_CATALOG_USER": cfg["ICEBERG_CATALOG_USER"],
        "ICEBERG_CATALOG_PASSWORD": cfg["ICEBERG_CATALOG_PASSWORD"],
        "BRONZE_ICEBERG_DATABASE_CATALOG_NAME": cfg["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_NAME": cfg["BRONZE_ICEBERG_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_WAREHOUSE": cfg["BRONZE_ICEBERG_CATALOG_WAREHOUSE"],
        "SILVER_ICEBERG_DATABASE_CATALOG_NAME": cfg["SILVER_ICEBERG_DATABASE_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_NAME": cfg["SILVER_ICEBERG_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_WAREHOUSE": cfg["SILVER_ICEBERG_CATALOG_WAREHOUSE"],
        "GOLD_ICEBERG_DATABASE_CATALOG_NAME": cfg["GOLD_ICEBERG_DATABASE_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_NAME": cfg["GOLD_ICEBERG_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_WAREHOUSE": cfg["GOLD_ICEBERG_CATALOG_WAREHOUSE"],
    },
    name="certificates_generatedcertificate_ingestion",
    task_id="certificates_generatedcertificate_ingestion_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def grades_persistentcoursegrade_ingestion(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image={cfg['docker_image']},
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode client \
          --name grades_persistentcoursegrade-ingestion \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/bronze_grades_persistentcoursegrade_ingestion.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
        env_vars={
        "ENVIRONMENT": cfg["ENVIRONMENT"],
        "MYSQL_DATABASE": cfg["database"],
        "MYSQL_HOST": cfg["host"],
        "MYSQL_PORT": str(cfg["port"]),
        "MYSQL_USER": cfg["user"],
        "MYSQL_SECRET": cfg["secret"],
        "S3_ACCESS_KEY": cfg["S3_ACCESS_KEY"],
        "S3_SECRET_KEY": cfg["S3_SECRET_KEY"],
        "S3_ENDPOINT": cfg["S3_ENDPOINT"],
        "ICEBERG_CATALOG_HOST": cfg["ICEBERG_CATALOG_HOST"],
        "ICEBERG_CATALOG_PORT": str(cfg["ICEBERG_CATALOG_PORT"]),
        "ICEBERG_CATALOG_USER": cfg["ICEBERG_CATALOG_USER"],
        "ICEBERG_CATALOG_PASSWORD": cfg["ICEBERG_CATALOG_PASSWORD"],
        "BRONZE_ICEBERG_DATABASE_CATALOG_NAME": cfg["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_NAME": cfg["BRONZE_ICEBERG_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_WAREHOUSE": cfg["BRONZE_ICEBERG_CATALOG_WAREHOUSE"],
        "SILVER_ICEBERG_DATABASE_CATALOG_NAME": cfg["SILVER_ICEBERG_DATABASE_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_NAME": cfg["SILVER_ICEBERG_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_WAREHOUSE": cfg["SILVER_ICEBERG_CATALOG_WAREHOUSE"],
        "GOLD_ICEBERG_DATABASE_CATALOG_NAME": cfg["GOLD_ICEBERG_DATABASE_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_NAME": cfg["GOLD_ICEBERG_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_WAREHOUSE": cfg["GOLD_ICEBERG_CATALOG_WAREHOUSE"],
    },
    name="grades_persistentcoursegrade_ingestion",
    task_id="grades_persistentcoursegrade_ingestion_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )



def auth_user_ingestion(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image={cfg['docker_image']},
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode client \
          --name auth_user_ingestion \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/bronze_auth_user_ingestion.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
        env_vars={
        "ENVIRONMENT": cfg["ENVIRONMENT"],
        "MYSQL_DATABASE": cfg["database"],
        "MYSQL_HOST": cfg["host"],
        "MYSQL_PORT": str(cfg["port"]),
        "MYSQL_USER": cfg["user"],
        "MYSQL_SECRET": cfg["secret"],
        "S3_ACCESS_KEY": cfg["S3_ACCESS_KEY"],
        "S3_SECRET_KEY": cfg["S3_SECRET_KEY"],
        "S3_ENDPOINT": cfg["S3_ENDPOINT"],
        "ICEBERG_CATALOG_HOST": cfg["ICEBERG_CATALOG_HOST"],
        "ICEBERG_CATALOG_PORT": str(cfg["ICEBERG_CATALOG_PORT"]),
        "ICEBERG_CATALOG_USER": cfg["ICEBERG_CATALOG_USER"],
        "ICEBERG_CATALOG_PASSWORD": cfg["ICEBERG_CATALOG_PASSWORD"],
        "BRONZE_ICEBERG_DATABASE_CATALOG_NAME": cfg["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_NAME": cfg["BRONZE_ICEBERG_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_WAREHOUSE": cfg["BRONZE_ICEBERG_CATALOG_WAREHOUSE"],
        "SILVER_ICEBERG_DATABASE_CATALOG_NAME": cfg["SILVER_ICEBERG_DATABASE_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_NAME": cfg["SILVER_ICEBERG_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_WAREHOUSE": cfg["SILVER_ICEBERG_CATALOG_WAREHOUSE"],
        "GOLD_ICEBERG_DATABASE_CATALOG_NAME": cfg["GOLD_ICEBERG_DATABASE_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_NAME": cfg["GOLD_ICEBERG_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_WAREHOUSE": cfg["GOLD_ICEBERG_CATALOG_WAREHOUSE"],
    },
    name="auth_user_ingestion",
    task_id="auth_user_ingestion_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def bronze_auth_userprofile_ingestion(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image={cfg['docker_image']},
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode client \
          --name bronze_auth_userprofile_ingestion \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driver.service.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/python/bronze_auth_userprofile_ingestion.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
        env_vars={
        "ENVIRONMENT": cfg["ENVIRONMENT"],
        "MYSQL_DATABASE": cfg["database"],
        "MYSQL_HOST": cfg["host"],
        "MYSQL_PORT": str(cfg["port"]),
        "MYSQL_USER": cfg["user"],
        "MYSQL_SECRET": cfg["secret"],
        "S3_ACCESS_KEY": cfg["S3_ACCESS_KEY"],
        "S3_SECRET_KEY": cfg["S3_SECRET_KEY"],
        "S3_ENDPOINT": cfg["S3_ENDPOINT"],
        "ICEBERG_CATALOG_HOST": cfg["ICEBERG_CATALOG_HOST"],
        "ICEBERG_CATALOG_PORT": str(cfg["ICEBERG_CATALOG_PORT"]),
        "ICEBERG_CATALOG_USER": cfg["ICEBERG_CATALOG_USER"],
        "ICEBERG_CATALOG_PASSWORD": cfg["ICEBERG_CATALOG_PASSWORD"],
        "BRONZE_ICEBERG_DATABASE_CATALOG_NAME": cfg["BRONZE_ICEBERG_DATABASE_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_NAME": cfg["BRONZE_ICEBERG_CATALOG_NAME"],
        "BRONZE_ICEBERG_CATALOG_WAREHOUSE": cfg["BRONZE_ICEBERG_CATALOG_WAREHOUSE"],
        "SILVER_ICEBERG_DATABASE_CATALOG_NAME": cfg["SILVER_ICEBERG_DATABASE_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_NAME": cfg["SILVER_ICEBERG_CATALOG_NAME"],
        "SILVER_ICEBERG_CATALOG_WAREHOUSE": cfg["SILVER_ICEBERG_CATALOG_WAREHOUSE"],
        "GOLD_ICEBERG_DATABASE_CATALOG_NAME": cfg["GOLD_ICEBERG_DATABASE_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_NAME": cfg["GOLD_ICEBERG_CATALOG_NAME"],
        "GOLD_ICEBERG_CATALOG_WAREHOUSE": cfg["GOLD_ICEBERG_CATALOG_WAREHOUSE"],
    },
    name="auth_userprofile_ingestion",
    task_id="auth_userprofile_ingestion_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )


default_args = {
    "start_date":datetime(2023, 1, 1),
    "catchup":False,
    "email":[],
    'email_on_failure': False,
    'email_on_retry': False,

}

bronze_dag =  DAG(dag_id="bronze_ingestion_dag",
default_args=default_args,
schedule="0 3 * * *",  # Run at 3:00 every day
tags=["bronze_table_ingestion","stage"],
) 

cfg = get_connection_properties(bronze_dag)


course_overviews_courseoverview_ingestion_task = course_overviews_courseoverview_ingestion(cfg=cfg)
certificates_generatedcertificate_ingestion_task = certificates_generatedcertificate_ingestion(cfg=cfg)
grades_persistentcoursegrade_ingestion_task = grades_persistentcoursegrade_ingestion(cfg=cfg)

auth_user_ingestion_task = auth_user_ingestion(cfg=cfg)
auth_userprofile_ingestion_task = bronze_auth_userprofile_ingestion(cfg=cfg)

course_overviews_courseoverview_ingestion_task >> certificates_generatedcertificate_ingestion_task >> grades_persistentcoursegrade_ingestion_task >> auth_user_ingestion_task >> auth_userprofile_ingestion_task #type:ignore