from datetime import timedelta, datetime
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models.dag import DAG

with DAG(
    dag_id="retail_sales_beam_pipeline",
    description="A simple dag for retail multy country data sources",
    start_date=datetime(2026, 6, 28),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["beam", "retail", "etl"],
) as dag:

    cleanup_psa = BashOperator(
        task_id="cleanup_psa",
        bash_command="rm -rf /opt/airflow/data/outputs/psa/proc_date={{ ds }}",
    )

    cleanup_gold = BashOperator(
        task_id="cleanup_gold",
        bash_command="rm -rf /opt/airflow/data/outputs/errors/proc_date={{ ds }} /opt/airflow/data/outputs/gold/proc_date={{ ds }}",
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