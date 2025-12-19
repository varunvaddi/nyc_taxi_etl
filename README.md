# NYC Taxi trip data using **Airflow**, **dbt**, and **Snowflake**.  

This project demonstrates **data engineering skills**, including data ingestion, transformation, testing, documentation, and orchestration.

---

## 🏗 Project Architecture
``` text
raw.nyc_taxi_trips (Parquet files)
↓
staging (dbt staging models)
↓
intermediate models (dbt transformations)
↓
fact tables
↓
marts / analytics tables
```

- **Airflow DAG**: Automates the ETL pipeline (dbt run → dbt test → dbt docs generate)  
- **dbt**: Performs layered transformations and enforces data quality via tests  
- **Snowflake**: Serves as the data warehouse  
- **Docker (optional upgrade)**: Reproducible environment for Airflow + dbt  

---

## 📂 Repository Structure
```text
nyc_taxi_etl/
├── airflow/
│ └── dags/ # Airflow DAG files
├── dbt/
│ └── nyc_taxi_pipeline/ # dbt project (models, macros, tests)
├── .gitignore
├── README.md
└── docker-compose.yml # optional for Docker setup
```

---

## ⚡ Features

- **Data ingestion**: Loads 2M+ NYC Taxi trips from Parquet files  
- **Layered dbt transformations**: `staging → intermediate → fact → mart`  
- **Automated testing**: Column-level tests (`not_null`, `unique`, business rules)  
- **Documentation**: dbt lineage and column descriptions  
- **Orchestration**: Airflow DAG triggers dbt runs, tests, and docs  
- **Future-ready**: Can be extended with incremental models, additional marts, or streaming (Kafka)

---

## 💻 Getting Started (Local)

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/nyc_taxi_etl.git
cd nyc_taxi_etl

2. Setup Python environment
conda create -n dbt-env python=3.11 -y
conda activate dbt-env

conda create -n airflow-env python=3.11 -y
conda activate airflow-env

pip install -r requirements.txt

3. Run Airflow locally
conda activate airflow-env
# Start scheduler on one terminal
airflow scheduler

# Start webserver on one terminal
airflow webserver --port 8080
Open http://localhost:8080 to view DAGs.

4. Run dbt (optional outside Airflow)
cd dbt/nyc_taxi_pipeline
conda activate dbt-env
dbt run
dbt test
dbt docs generate
dbt docs serve
```
---
## 📊 Visualizations
Airflow DAG Success

dbt Lineage Graph

---
## 📌 Next Steps / Enhancements
Add additional marts for business metrics (daily revenue, peak hours, payment type)

Enforce more dbt tests for data quality

Dockerize Airflow + dbt for reproducible deployment

Add incremental models for continuous ingestion

Extend to real-time streaming with Kafka

