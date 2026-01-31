from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# -------- Python callables --------
def task_1():
    print("Task 1: Extract data")


def task_2():
    print("Task 2: Transform data")


def task_3():
    print("Task 3: Load data")


# -------- DAG definition --------
with DAG(
    dag_id="simple_python_operator_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["example", "python"],
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=task_1,
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=task_2,
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=task_3,
    )

    # -------- Task dependencies --------
    t1 >> t2 >> t3
    # [t2, t3] >> t1
    # [t2, t3] << t1