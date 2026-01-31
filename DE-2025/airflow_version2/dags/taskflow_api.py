from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="example_taskflow_api",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["examples"],
)
def taskflow_dag():

    @task
    def extract():
        data = {"numbers": [1, 2, 3], "source": "demo"}
        return data

    @task
    def transform(data: dict):
        nums = data["numbers"]
        return {"sum": sum(nums), "count": len(nums), "source": data["source"]}

    @task
    def load(result: dict):
        print(f"Final result: {result}")

    load(transform(extract()))

taskflow_dag()