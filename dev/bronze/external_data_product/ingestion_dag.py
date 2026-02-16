from airflow import DAG #type: ignore
from datetime import datetime
from airflow.sdk import Variable,Connection #type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore
from tasks.bronze_course_overviews_courseoverview_ingestion import course_overviews_courseoverview_ingestion
from tasks.certificates_generatedcertificate_ingestion import certificates_generatedcertificate_ingestion
def get_connection_properties(dag: DAG)->dict:
    
    try:
        config = {}
        config["docker_image"] = Variable.get("docker_image")
        config["dag"] = dag
        config["namespace"] = "dev-nau-analytics"
        
        mysql_conn = Connection.get("sql_source_dev_connection")
        config["database"] = mysql_conn.extra_dejson.get("mysqldatabase")
        config["host"] = mysql_conn.host
        config["user"] = mysql_conn.login
        config["port"] = mysql_conn.port
        config["secret"] = mysql_conn.password
        

        s3_conn = Connection.get("s3_dev_connection")
        config["S3_ACCESS_KEY"] = s3_conn.login
        config["S3_SECRET_KEY"] = s3_conn.password
        config["S3_ENDPOINT"] =s3_conn.extra_dejson.get("s3endpoint")

        iceberg_catalog_conn = Connection.get("iceberg_dev_connection")
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
        
        return config
    except Exception:
        raise Exception(f"Could not get the variables or secrets: {Exception}")




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
tags=["bronze_table_ingestion","dev"],
) 

cfg = get_connection_properties(bronze_dag)


course_overviews_courseoverview_ingestion_task = course_overviews_courseoverview_ingestion(cfg=cfg)
certificates_generatedcertificate_ingestion_task = certificates_generatedcertificate_ingestion(cfg=cfg)


course_overviews_courseoverview_ingestion_task >> certificates_generatedcertificate_ingestion_task #type:ignore