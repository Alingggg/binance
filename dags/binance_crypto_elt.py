from datetime import datetime
from airflow.sdk import DAG, get_current_context, task
from airflow_project.binance import extract, transform

with DAG(
    dag_id="binance_crypto_elt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
):
    @task
    def extract_and_load_bronze():
        context = get_current_context()
        return extract.extract_and_load_bronze(
            run_id=context["run_id"],
            task_try_number=context["task_instance"].try_number,
        )

    @task
    def transform_and_load_silver(extraction_id):
        transform.transform_and_load_silver(extraction_id)

    extraction_id = extract_and_load_bronze()
    transform_and_load_silver(extraction_id)
