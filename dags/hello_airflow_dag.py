from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_airflow():
    print("Airflow ETL pipeline started successfully")


with DAG(
    dag_id="hello_airflow_pipeline",
    description="First production style Airflow DAG",
    start_date=datetime(2026, 7, 17),
    schedule="@daily",
    catchup=False,
    tags=["etl", "training"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_airflow,
    )

    hello_task