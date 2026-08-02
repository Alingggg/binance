from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:
    start = EmptyOperator(task_id="start")

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow"',
    )

    finish = EmptyOperator(task_id="finish")

    start >> say_hello >> finish