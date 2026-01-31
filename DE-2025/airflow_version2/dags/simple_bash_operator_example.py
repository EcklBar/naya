from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="simple_bash_operator_example",
    start_date=datetime(2024, 1, 10),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["example", "bash"],
) as dag:

    hello_task = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from Airflow BashOperator!'"
    )