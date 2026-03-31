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

def auth_user(cfg:dict) -> KubernetesPodOperator:
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
          --deploy-mode cluster \
          --name auth_user_silver \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_auth_user.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="auth_user_silver",
    task_id="auth_user_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def auth_userprofile(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image=cfg['docker_image'],
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name auth_userprofile_silver \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_auth_userprofile.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="auth_userprofile_silver",
    task_id="auth_userprofile_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def certificates_generatedcertificate(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image=cfg['docker_image'],
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name certificates_generatedcertificate_silver \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_certificates_generatedcertificate.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="certificates_generatedcertificate_silver",
    task_id="certificates_generatedcertificate_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def organizations_ho(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image=cfg['docker_image'],
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name organizations_ho_silver \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_organizations_ho.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="organizations_ho_silver",
    task_id="organizations_ho_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def grades_persistentcoursegrade(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image=cfg['docker_image'],
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name grades_persistentcoursegrade_silver \
          --conf spark.kubernetes.container.image={cfg['docker_image']} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_grades_persistentcoursegrade.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="grades_persistentcoursegrade_silver",
    task_id="grades_persistentcoursegrade_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def organizations_organization(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:d465952',
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name organizations_organization_silver \
          --conf spark.kubernetes.container.image={cfg["docker_image"]} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
          --conf spark.kubernetes.driverEnv.MYSQL_DATABASE={cfg["database"]} \
          --conf spark.kubernetes.driverEnv.ENVIRONMENT={cfg["ENVIRONMENT"]} \
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
          local:///opt/spark/work-dir/src/silver/silver_organizations_organization.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="organizations_organization_silver",
    task_id="organizations_organization_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def student_courseenrollment(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:d465952',
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name student_courseenrollment_silver \
          --conf spark.kubernetes.container.image={cfg["docker_image"]} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_student_courseenrollment.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="student_courseenrollment_silver",
    task_id="student_courseenrollment_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def student_courseenrollment_history(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:d465952',
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name student_courseenrollment_history_silver \
          --conf spark.kubernetes.container.image={cfg["docker_image"]} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_student_courseenrollment_history.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="student_courseenrollment_history_silver",
    task_id="student_courseenrollment_history_silver_1",
    get_logs=True,
    on_finish_action="delete_pod",
    dag = cfg["dag"]
    )

def course_overviews_courseoverview(cfg:dict) -> KubernetesPodOperator:
    return KubernetesPodOperator(
    namespace=cfg["namespace"],
    service_account_name='spark-role',
    # ✔ official spark image built for k8s
    image='nauedu/nau-analytics-spark-shell:d465952',
    startup_timeout_seconds=600,
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],
    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        f"""
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name course_overviews_courseoverview_silver \
          --conf spark.kubernetes.container.image={cfg["docker_image"]} \
          --conf spark.kubernetes.namespace={cfg["namespace"]} \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.executor.instances=2 \
          --conf spark.executor.cores=1 \
          --conf spark.executor.memory=8g \
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
          local:///opt/spark/work-dir/src/silver/silver_course_overviews_courseoverview.py\
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name="course_overviews_courseoverview_silver",
    task_id="course_overviews_courseoverview_silver_1",
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

bronze_dag =  DAG(dag_id="silver_dag",
default_args=default_args,
schedule="0 4 * * *",  # Run at 4:00 every day
tags=["silver_table_clean","stage"],
) 

cfg = get_connection_properties(bronze_dag)


auth_user_task = auth_user(cfg=cfg)
auth_userprofile_task = auth_userprofile(cfg=cfg)
certificates_generatedcertificate_task = certificates_generatedcertificate(cfg=cfg)
course_overviews_courseoverview_task = course_overviews_courseoverview(cfg=cfg)
grades_persistentcoursegrade_task = grades_persistentcoursegrade(cfg=cfg)
organizations_ho_silver_task = organizations_ho(cfg=cfg)
organizations_organization_task = organizations_organization(cfg=cfg)
student_courseenrollment_task = student_courseenrollment(cfg=cfg)
student_courseenrollment_history_task = student_courseenrollment_history(cfg=cfg)

auth_user_task >> auth_userprofile_task >> certificates_generatedcertificate_task >> course_overviews_courseoverview_task >> grades_persistentcoursegrade_task >> organizations_ho_silver_task >> organizations_organization_task >> student_courseenrollment_task >> student_courseenrollment_history_task #type: ignore