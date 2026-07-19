from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator

from datetime import datetime

import boto3
import csv
import json

from scripts.api_extract import extract_users


RAW_JSON = "/tmp/raw_users.json"
CLEAN_JSON = "/tmp/clean_users.json"
CSV_FILE = "/tmp/customers.csv"

BUCKET_NAME = "airflow-etl-pipeline-2026"
S3_KEY = "raw/customers.csv"


def fetch_api_data():
    extract_users()


def clean_data():
    with open(RAW_JSON, "r") as file:
        users = json.load(file)

    cleaned_users = []

    for user in users:
        cleaned_users.append(
            {
                "id": user["id"],
                "name": user["name"],
                "email": user["email"],
                "city": user["address"]["city"],
            }
        )

    with open(CLEAN_JSON, "w") as file:
        json.dump(cleaned_users, file, indent=4)

    print(f"Cleaned {len(cleaned_users)} records")


def create_csv():
    with open(CLEAN_JSON, "r") as file:
        users = json.load(file)

    with open(CSV_FILE, "w", newline="") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "id",
                "name",
                "email",
                "city",
            ],
        )

        writer.writeheader()
        writer.writerows(users)

    print("CSV created successfully")


def upload_to_s3():

    s3 = boto3.client("s3")

    s3.upload_file(
        CSV_FILE,
        BUCKET_NAME,
        S3_KEY,
    )

    print(
        f"Uploaded {CSV_FILE} to s3://{BUCKET_NAME}/{S3_KEY}"
    )


with DAG(
    dag_id="production_etl_pipeline",
    description="API to S3 to Snowflake ETL Pipeline",
    start_date=datetime(2026, 7, 17),
    schedule="@daily",
    catchup=False,
    tags=[
        "etl",
        "aws",
        "snowflake"
    ],
) as dag:


    fetch_api = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_api_data,
    )


    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )


    csv_task = PythonOperator(
        task_id="create_csv",
        python_callable=create_csv,
    )


    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )


    load_to_snowflake = SnowflakeSqlApiOperator(

        task_id="load_to_snowflake",

        snowflake_conn_id="snowflake_conn",

        hook_params={
            "authenticator": "snowflake"
        },

        sql="""

        CREATE TABLE IF NOT EXISTS CUSTOMERS (

            ID NUMBER,

            NAME STRING,

            EMAIL STRING,

            CITY STRING

        );


        TRUNCATE TABLE CUSTOMERS;


        COPY INTO CUSTOMERS

        FROM @airflow_s3_stage/customers.csv

        FILE_FORMAT = (

            TYPE = CSV

            SKIP_HEADER = 1

            FIELD_OPTIONALLY_ENCLOSED_BY = '"'

        );

        """,
    )


    fetch_api >> clean >> csv_task >> upload >> load_to_snowflake