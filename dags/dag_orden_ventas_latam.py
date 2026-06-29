from datetime import timedelta, datetime
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.dag import DAG
import shutil

def cleanup_psa_func(proc_date, **kwargs):
    path = f"/opt/airflow/data/outputs/psa/proc_date={proc_date}"
    shutil.rmtree(path, ignore_errors=True)

def cleanup_gold_func(proc_date, **kwargs):
    for base in ["gold", "errors"]:
        path = f"/opt/airflow/data/outputs/{base}/proc_date={proc_date}"
        shutil.rmtree(path, ignore_errors=True)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_sales_beam_pipeline",
    description="A simple dag for retail multy country data sources",
    start_date=datetime(2026, 6, 28),
    schedule="@daily",
    catchup=True,
    default_args=default_args,
    tags=["beam", "retail", "etl"],
) as dag:

    # Limpia la partición antes de escribir
    cleanup_psa = PythonOperator(
        task_id="cleanup_psa",
        python_callable=cleanup_psa_func,
        op_kwargs={"proc_date": "{{ ds }}"},
    )

    cleanup_gold = PythonOperator(
        task_id="cleanup_gold",
        python_callable=cleanup_gold_func,
        op_kwargs={"proc_date": "{{ ds }}"},
    )

    ingest_to_psa = BeamRunPythonPipelineOperator(
        task_id="ingest_to_psa",
        py_file="/opt/airflow/beam-jobs/job1_ingest_to_psa.py",
        runner="DirectRunner",
        pipeline_options={"proc_date": "{{ ds }}"},
    )

    transform_to_gold = BeamRunPythonPipelineOperator(
        task_id="transform_to_gold",
        py_file="/opt/airflow/beam-jobs/job2_transform_gold.py",
        runner="DirectRunner",
        pipeline_options={"proc_date": "{{ ds }}"},
    )

    cleanup_psa >> ingest_to_psa >> cleanup_gold >> transform_to_gold