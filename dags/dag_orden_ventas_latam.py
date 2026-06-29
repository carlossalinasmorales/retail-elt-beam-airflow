from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="retail_sales_beam_pipeline",
    start_date=datetime(2026, 6, 29),
    schedule="@daily",
    catchup=False,
    tags=["beam", "retail", "etl"],
) as dag:

    run_sales_pipeline = BashOperator(
        task_id="run_sales_pipeline",
        bash_command="python /opt/airflow/pipelines/sales_pipeline.py",
    )