from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

# DAG definition
with DAG(
    dag_id="simple_bashoperator_3_tasks",
    default_args=default_args,
    description="Simple Airflow 2 DAG with 3 BashOperator tasks",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "bash"],
) as dag:

    task_1 = BashOperator(
        task_id="task_1_echo_start",
        bash_command="echo 'Task 1: Start pipeline'",
    )

    task_2 = BashOperator(
        task_id="task_2_print_date",
        bash_command="echo 'Task 2: Current date:' && date",
    )

    task_3 = BashOperator(
        task_id="task_3_echo_end",
        bash_command="echo 'Task 3: Pipeline finished'",
    )

    # Task dependencies
    task_1 >> task_2 >> task_3
    # [task_2, task_3] << task_1