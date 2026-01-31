from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def produce_value():
    # Anything JSON-serializable is easiest (dict/list/str/int/etc.)
    payload = {"user": "lev", "count": 3, "items": ["a", "b", "c"]}
    print(f"Producing: {payload}")
    return payload  # automatically pushed to XCom as "return_value"


def consume_value(ti):
    data = ti.xcom_pull(task_ids="produce")  # pulls "return_value" by default
    print(f"Consumed from XCom: {data}")

    # Example: use the data
    items = data.get("items", [])
    print(f"Items count: {len(items)}")


def finish():
    print("All done")


with DAG(
    dag_id="demo_xcom_data_sharing",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
) as dag:
    produce = PythonOperator(
        task_id="produce",
        python_callable=produce_value,
    )

    consume = PythonOperator(
        task_id="consume",
        python_callable=consume_value,
    )

    done = PythonOperator(
        task_id="done",
        python_callable=finish,
    )

    produce >> consume >> done