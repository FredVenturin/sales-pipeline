from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from generate_data import run as generate_run
from load_bronze import run as load_bronze_run

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sales_pipeline',
    default_args=default_args,
    description='Sales data pipeline: Bronze → Silver → Gold',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['sales', 'etl', 'medallion'],
) as dag:

    # Task 1 — generate new sales data and save to bronze CSV
    task_generate = PythonOperator(
        task_id='generate_data',
        python_callable=generate_run,
    )

    # Task 2 — load bronze CSV into PostgreSQL
    task_load_bronze = PythonOperator(
        task_id='load_bronze',
        python_callable=load_bronze_run,
    )

    # Task 3 — run dbt staging models (Silver)
    task_dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging',
    )

    # Task 4 — run dbt marts models (Gold) and run quality tests
    task_dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts && dbt test',
    )

    # Define execution order: Bronze → Silver → Gold
    task_generate >> task_load_bronze >> task_dbt_staging >> task_dbt_marts