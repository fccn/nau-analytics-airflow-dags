from airflow import DAG #type: ignore
from datetime import datetime
from .task_functions import spark_submit_task #type:ignore
from .get_connection_info import get_connection_properties #type:ignore

config_dict = get_connection_properties()
spark_submit_task_1 = spark_submit_task(cfg = config_dict,
                                        task_name="certificates_generatedcertificate_table_import",
                                        task_id="certificates_generatedcertificate_table_import_v1",
                                        python_script="get_full_tables.py")
with DAG(
    dag_id="bronze_ingestion_dag",
    start_date=datetime(2023, 1, 1),
    schedule="0 3 * * *",  # Run at 17:00 every day
    catchup=False,
    tags=["bronze_table_ingestion","dev"],
) as dag:
    spark_submit_task_1
spark_submit_task_1