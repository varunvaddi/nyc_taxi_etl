FROM apache/airflow:2.11.0-python3.11

USER root
RUN apt-get update && apt-get install -y git

USER airflow
RUN pip install --no-cache-dir dbt-snowflake==1.10.4
