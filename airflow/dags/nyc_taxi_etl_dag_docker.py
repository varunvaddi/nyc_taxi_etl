from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi_etl_pipeline_docker', #NEW DAG NAME
    default_args=default_args,
    description='Dockerized ETL pipeline for NYC Taxi data using dbt',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['taxi', 'etl', 'dbt', 'snowflake', 'docker'],
) as dag:

    # Task 1: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/nyc_taxi_pipeline && dbt run --profiles-dir .',
    )
    # Task 2: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt/nyc_taxi_pipeline && dbt test --profiles-dir .',
    )

    # Task 3: Generate dbt docs
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt/nyc_taxi_pipeline && dbt docs generate --profiles-dir .',
    )

    # Define task dependencies
    dbt_run >> dbt_test >> dbt_docs