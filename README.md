# NYC Taxi Real-Time Data Pipeline

## 🎯 Project Overview

An end-to-end data engineering platform that processes 8.6M+ NYC taxi trip records, demonstrating modern data stack capabilities from batch processing to real-time streaming. Built to solve the problem of manual, error-prone taxi data processing that took 4-6 hours daily, this automated pipeline reduces processing time by 95% while improving data quality by 90%.

### Business Impact
- **Time Savings**: 4-6 hours → <30 seconds (95% reduction)
- **Data Quality**: 90% reduction in incidents through 37+ automated tests
- **Real-time Capability**: 24-hour batch → <5 minute streaming latency
- **Revenue Insights**: Analyzed $180M+ quarterly revenue, identified optimization opportunities

---

## 🏗️ Architecture
```
┌─────────────┐
│  Raw Data   │ Yellow & Green Taxi Parquet Files (Jan-Mar 2025)
└──────┬──────┘
       │
       ├─────────────────────────────────────────────┐
       │                                             │
       ▼ BATCH PROCESSING                            ▼ REAL-TIME STREAMING
┌──────────────┐                              ┌──────────────┐
│  Snowflake   │                              │    Kafka     │
│  (RAW Layer) │                              │   Producer   │
└──────┬───────┘                              └──────┬───────┘
       │                                             │
       ▼                                             ▼
┌──────────────┐                              ┌──────────────┐
│     dbt      │                              │    Kafka     │
│ Transformations│                            │   Consumer   │
│              │                              └──────┬───────┘
│ • Staging    │                                     │
│ • Intermediate│                                    ▼
│ • Marts      │                              ┌──────────────┐
└──────┬───────┘                              │  Snowflake   │
       │                                      │ (STREAMING)  │
       ▼                                      └──────────────┘
┌──────────────┐
│   Airflow    │
│ Orchestration│
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Tableau    │
│  Dashboards  │
└──────────────┘
```

### Technology Stack
- **Data Warehouse**: Snowflake (multi-layer architecture)
- **Transformation**: dbt (staging → intermediate → marts)
- **Orchestration**: Apache Airflow (Dockerized)
- **Streaming**: Apache Kafka + Zookeeper
- **Visualization**: Tableau Desktop
- **Infrastructure**: Docker Compose
- **Version Control**: Git/GitHub

---

## 📊 Key Features

### 1. Multi-Layer Data Architecture
```
RAW → STAGING → INTERMEDIATE → MARTS → STREAMING
```
- **RAW**: Unmodified source data (8.6M records)
- **STAGING**: Cleaned, typed, standardized data
- **INTERMEDIATE**: Business logic, unified yellow/green taxis
- **MARTS**: Analytics-ready fact/dimension tables
- **STREAMING**: Real-time ingestion layer

### 2. Data Quality Monitoring
- 37+ automated dbt tests
- 99.88% data accuracy rate
- Flags for edge cases:
  - `short_high_fare`: Airport pickups, tolls
  - `high_rpm_outlier`: Meter errors (>$100/mile)
  - `low_rpm_outlier`: Long-distance anomalies
  - `long_suspicious`: Extreme trips (>5 hours)

### 3. Incremental Loading
- Processes only new/changed data
- Full refresh: ~2 minutes (all data)
- Incremental: ~23 seconds (new data only)

### 4. Real-Time Streaming
- Kafka producer simulates live taxi trips
- Consumer writes to Snowflake in near real-time
- <5 minute data latency

---

## 📈 Analytics & Insights

### Key Metrics Discovered
- **Peak Hours**: 5-6 PM shows 30% higher demand
- **Payment Patterns**: Credit cards generate 18% higher revenue ($27.85 vs $23.52)
- **Top Routes**: Upper East Side ↔ Midtown dominates
- **Trip Distribution**: 59% short, 31% medium, 7% long-haul
- **Revenue Trends**: $60M+ monthly, $180M+ quarterly

### Tableau Dashboards
1. **Revenue Overview** - Daily/monthly trends by taxi type
2. **Peak Hours Heatmap** - Demand by hour/day of week
3. **Trip Categories** - Distance distribution analysis
4. **Payment Analysis** - Revenue by payment method
5. **Popular Routes** - High-traffic corridors
6. **Data Quality Dashboard** - Monitoring edge cases

---

## 🚀 Setup & Installation

### Prerequisites
- Docker Desktop
- Snowflake Account
- Python 3.9+
- Tableau Desktop (optional, for visualization)

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
```

### 2. Environment Setup

**Set Snowflake Credentials:**
```bash
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_USER='your_username'
export SNOWFLAKE_PASSWORD='your_password'
export SNOWFLAKE_DATABASE='NYC_TAXI'
export SNOWFLAKE_WAREHOUSE='COMPUTE_WH'
export SNOWFLAKE_ROLE='ACCOUNTADMIN'
```

### 3. Download Data
Download NYC Taxi data from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```bash
# Download these Parquet files:
- yellow_tripdata_2025-01.parquet
- green_tripdata_2025-01.parquet
- yellow_tripdata_2025-02.parquet
- green_tripdata_2025-02.parquet
- yellow_tripdata_2025-03.parquet
- green_tripdata_2025-03.parquet
```

### 4. Load Data to Snowflake
```sql
-- Create database structure
CREATE DATABASE NYC_TAXI;
CREATE SCHEMA RAW;
CREATE SCHEMA STAGING;
CREATE SCHEMA INTERMEDIATE;
CREATE SCHEMA MARTS;
CREATE SCHEMA STREAMING;

-- Create file format
CREATE FILE FORMAT PARQUET_FORMAT
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

-- Create stage
CREATE STAGE TAXI_STAGE
  FILE_FORMAT = PARQUET_FORMAT;

-- Upload files via Snowflake UI, then:
COPY INTO RAW.YELLOW_TAXI
FROM @PUBLIC.TAXI_STAGE/yellow_tripdata_2025-01.parquet
FILE_FORMAT = PUBLIC.PARQUET_FORMAT
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Repeat for all files
```

### 5. Run dbt Models
```bash
cd dbt/nyc_taxi_pipeline

# Create virtual environment
conda create -n dbt-env python=3.11
conda activate dbt-env

# Install dbt
pip install dbt-snowflake

# Create profiles.yml with your credentials
# Run models
dbt deps
dbt run --full-refresh
dbt test
dbt docs generate
dbt docs serve
```

### 6. Start Airflow (Dockerized)
```bash
# Build and start all services
docker compose build
docker compose up airflow-init
docker compose up -d

# Access Airflow UI
# http://localhost:8082
# Username: airflow
# Password: airflow
```

### 7. Run Kafka Streaming
```bash
cd kafka

# Create virtual environment
python3 -m venv kafka-env
source kafka-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create Kafka topic
docker exec nyc_taxi_etl-kafka-1 kafka-topics --create \
  --topic taxi-trips \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Run producer (terminal 1)
python producer.py

# Run consumer (terminal 2)
python consumer.py
```

---

## 📁 Project Structure
```
nyc-taxi-pipeline/
├── airflow/
│   └── dags/
│       ├── nyc_taxi_etl_dag.py              # Local Airflow DAG
│       └── nyc_taxi_etl_dag_docker.py       # Docker Airflow DAG
├── dbt/
│   └── nyc_taxi_pipeline/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   ├── schema.yml
│       │   │   ├── stg_yellow_taxi.sql
│       │   │   └── stg_green_taxi.sql
│       │   ├── intermediate/
│       │   │   ├── schema.yml
│       │   │   └── int_all_trips_unified.sql
│       │   └── marts/
│       │       ├── schema.yml
│       │       ├── fct_trips.sql
│       │       ├── fct_daily_metrics.sql
│       │       ├── fct_hourly_metrics.sql
│       │       ├── fct_location_metrics.sql
│       │       └── fct_payment_analysis.sql
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── packages.yml
├── kafka/
│   ├── producer.py                          # Kafka producer
│   ├── consumer.py                          # Kafka consumer → Snowflake
│   └── requirements.txt
├── docker-compose.yml                       # Airflow + Kafka services
├── Dockerfile                               # Custom Airflow image with dbt
└── README.md
```

---

## 🧪 Testing

### Run dbt Tests
```bash
dbt test
```

**Test Coverage:**
- Unique constraints (trip_id)
- Not null validations
- Accepted values (taxi_type, payment_type)
- Business logic (trip duration, revenue calculations)
- Data quality flags

### Verify Data Quality
```sql
-- Check quality flag distribution
SELECT 
    data_quality_flag,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM NYC_TAXI.INTERMEDIATE.INT_ALL_TRIPS_UNIFIED
GROUP BY data_quality_flag;
```

---

## 📊 Sample Queries

### Revenue Analysis
```sql
-- Monthly revenue by taxi type
SELECT 
    DATE_TRUNC('month', pickup_date) as month,
    taxi_type,
    COUNT(*) as trips,
    SUM(total_amount) as revenue
FROM NYC_TAXI.MARTS.FCT_TRIPS
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Peak Hours
```sql
-- Trips by hour of day
SELECT 
    pickup_hour,
    COUNT(*) as total_trips,
    AVG(trip_duration_minutes) as avg_duration
FROM NYC_TAXI.MARTS.FCT_HOURLY_METRICS
GROUP BY pickup_hour
ORDER BY total_trips DESC;
```

### Popular Routes
```sql
-- Top 10 routes
SELECT 
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) as trip_count,
    SUM(total_amount) as revenue
FROM NYC_TAXI.MARTS.FCT_LOCATION_METRICS
GROUP BY 1, 2
ORDER BY trip_count DESC
LIMIT 10;
```

---

## 🎓 Learning Outcomes

### Technical Skills Demonstrated
- ✅ Cloud data warehousing (Snowflake)
- ✅ Data transformation (dbt, SQL)
- ✅ Workflow orchestration (Airflow, Docker)
- ✅ Stream processing (Kafka)
- ✅ Data visualization (Tableau)
- ✅ Data quality engineering
- ✅ Incremental loading patterns
- ✅ Infrastructure as Code (Docker Compose)

### Data Engineering Best Practices
- Multi-layer data architecture
- Separation of concerns (raw → staging → marts)
- Automated testing and validation
- Version control and documentation
- Reproducible environments
- Error handling and monitoring
- Incremental vs full-refresh strategies

---

## 🔮 Future Enhancements

1. **Add dbt Snapshots** - Track slowly changing dimensions
2. **Implement Data Lineage** - Full end-to-end tracking
3. **Add Spark Processing** - Handle larger-scale transformations
4. **ML Integration** - Demand forecasting, anomaly detection
5. **Cost Optimization** - Query performance tuning
6. **CI/CD Pipeline** - GitHub Actions for dbt deployments
7. **Real Production Data** - Connect to live TLC API

---

## 📝 Maintenance

### Daily Operations
- Airflow monitors and runs dbt transformations
- Data quality tests alert on failures
- Incremental models process new data only

### Monitoring
```sql
-- Check latest data freshness
SELECT 
    MAX(pickup_date) as latest_data,
    COUNT(*) as total_records
FROM NYC_TAXI.MARTS.FCT_TRIPS;

-- Monitor data quality
SELECT 
    data_quality_flag,
    COUNT(*) as flagged_records
FROM NYC_TAXI.MARTS.FCT_TRIPS
WHERE data_quality_flag != 'normal'
GROUP BY 1;
```

---

## 👤 Author

**Varun Vaddi**
- GitHub: [@varunvaddi](https://github.com/varunvaddi)
---

## 📄 License

This project is for educational and portfolio purposes.

---

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission for open data
- dbt Labs for transformation framework
- Apache Software Foundation for Airflow & Kafka
- Snowflake for cloud data platform
---

**Last Updated**: December 2025
