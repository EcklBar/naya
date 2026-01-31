from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

LOCAL_UPLOAD = "/tmp/sample.txt"
LOCAL_DOWNLOAD = "/tmp/sample_downloaded.txt"
BUCKET = "my-bucket"
KEY = "sample.txt"

def write_sample():
    os.makedirs("/tmp", exist_ok=True)
    with open(LOCAL_UPLOAD, "w") as f:
        f.write("Hello from Airflow to MinIO!")

def download_from_minio():
    hook = S3Hook(aws_conn_id="minio_conn")
    obj = hook.get_key(KEY, BUCKET)
    obj.download_file(LOCAL_DOWNLOAD)

with DAG(
    dag_id="minio_example_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "s3"],
) as dag:

    make_file = PythonOperator(
        task_id="make_file",
        python_callable=write_sample,
    )

    upload_to_minio = LocalFilesystemToS3Operator(
        task_id="upload_to_minio",
        filename=LOCAL_UPLOAD,
        dest_bucket=BUCKET,
        dest_key=KEY,
        replace=True,
        aws_conn_id="minio_conn",
    )

    download_from_minio_task = PythonOperator(
        task_id="download_from_minio",
        python_callable=download_from_minio,
    )

    make_file >> upload_to_minio >> download_from_minio_task