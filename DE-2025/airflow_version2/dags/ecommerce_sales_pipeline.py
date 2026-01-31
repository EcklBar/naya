from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  
import pandas as pd
import os
import time

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 8, 1),
    "email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

SOURCE_CSV = "/tmp/source_data/sales_data.csv"
STAGING_DIR = "/tmp/airflow_staging"
TARGET_DIR = "/tmp/target_data"

def extract_sales_data(**context):
    if not os.path.exists(SOURCE_CSV):
        raise FileNotFoundError(f"Source CSV not found: {SOURCE_CSV}")

    os.makedirs(STAGING_DIR, exist_ok=True)

    df = pd.read_csv(SOURCE_CSV)

    staging_path = os.path.join(
        STAGING_DIR,
        f"sales_raw_{context['ds_nodash']}.csv"
    )
    df.to_csv(staging_path, index=False)

    return staging_path

def transform_sales_data(ti, **context):
    time.sleep(2)

    raw_path = ti.xcom_pull(task_ids="extract_sales_data")
    if not raw_path or not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw staging file not found: {raw_path}")

    df = pd.read_csv(raw_path)

    # Transformations
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["total_revenue"] = df["quantity"] * df["price"]

    product_stats = (
        df.groupby("product", as_index=False)
          .agg(quantity=("quantity", "sum"),
               total_revenue=("total_revenue", "sum"))
    )

    os.makedirs(STAGING_DIR, exist_ok=True)
    transformed_path = os.path.join(
        STAGING_DIR,
        f"sales_transformed_{context['ds_nodash']}.csv"
    )
    product_stats.to_csv(transformed_path, index=False)

    return transformed_path

def load_data(ti, **context):
    transformed_path = ti.xcom_pull(task_ids="transform_sales_data")
    if not transformed_path or not os.path.exists(transformed_path):
        raise FileNotFoundError(f"Transformed file not found: {transformed_path}")

    os.makedirs(TARGET_DIR, exist_ok=True)
    output_file = os.path.join(
        TARGET_DIR,
        f"transformed_sales_data_{context['ds_nodash']}.csv"
    )

    df = pd.read_csv(transformed_path)
    df.to_csv(output_file, index=False)

with DAG(
    dag_id="ecommerce_sales_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["example"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data,
    )

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data,
    )

    load_task = PythonOperator(
        task_id="load_sales_data",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task