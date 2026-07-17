from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import csv


def fetch_api_data():
    print("Fetching data from API")


def clean_data():
    print("Cleaning raw data")


def create_csv():
    file_name = "/tmp/customers.csv"

    data = [
        ["id", "name", "city"],
        [1, "Ram", "Pune"],
        [2, "Alex", "Mumbai"],
    ]

    with open(file_name, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)

    print("CSV file created successfully")


def upload_to_s3():
    s3 = boto3.client("s3")

    bucket_name = "airflow-etl-pipeline-2026"

    file_name = "/tmp/customers.csv"

    s3.upload_file(
        file_name,
        bucket_name,
        "raw/customers.csv"
    )

    print("File uploaded to S3 successfully")


def load_s3_to_snowflake():
    print("Loading data into Snowflake")


def run_sql_transformation():
    print("Running SQL transformation")


def create_clean_table():
    print("Creating final clean table")


with DAG(
    dag_id="production_etl_pipeline",
    description="API to S3 to Snowflake ETL pipeline",
    start_date=datetime(2026, 7, 17),
    schedule="@daily",
    catchup=False,
    tags=["etl", "s3", "snowflake"],
) as dag:

    fetch_api = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_api_data,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    csv_file = PythonOperator(
        task_id="create_csv",
        python_callable=create_csv,
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    snowflake = PythonOperator(
        task_id="load_s3_to_snowflake",
        python_callable=load_s3_to_snowflake,
    )

    transform = PythonOperator(
        task_id="run_sql_transformation",
        python_callable=run_sql_transformation,
    )

    create_table = PythonOperator(
        task_id="create_clean_table",
        python_callable=create_clean_table,
    )

    (
        fetch_api
        >> clean
        >> csv_file
        >> upload
        >> snowflake
        >> transform
        >> create_table
    )