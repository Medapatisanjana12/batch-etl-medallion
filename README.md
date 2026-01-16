# Batch ETL Medallion Pipeline 

An end-to-end **batch ETL pipeline** built using a modern data stack.  
This project processes raw event data, transforms it using Apache Spark, orchestrates workflows with Apache Airflow, stores analytics-ready data in PostgreSQL, and visualizes insights using Streamlit.

---

## Architecture Overview

The pipeline follows the **Medallion Architecture**:

- **Bronze Layer** – Raw event data (CSV/JSON) stored in MinIO
- **Silver Layer** – Cleaned and enriched data in partitioned Parquet format
- **Gold Layer** – Aggregated, business-level metrics ready for analytics
---

### Tech Stack
- **Apache Spark** – Distributed data processing
- **Apache Airflow** – Workflow orchestration
- **MinIO** – S3-compatible data lake
- **PostgreSQL** – Data warehouse
- **Streamlit** – Data visualization
- **Docker & Docker Compose** – Containerization

---


## ETL Workflow

1. **Data Generation**
   - Synthetic user event data is generated using Python.
   - Data includes events such as page views, clicks, and purchases.

2. **Bronze Layer**
   - Raw data is uploaded to MinIO (S3-compatible storage).

3. **Spark ETL**
   - Spark reads raw data from Bronze.
   - Performs data cleaning, type casting, and enrichment.
   - Writes transformed data to:
     - Silver layer (partitioned Parquet)
     - Gold layer (aggregated metrics)

4. **Airflow Orchestration**
   - DAG schedules Spark ETL daily.
   - Ensures idempotent runs.
   - Includes data quality checks.

5. **PostgreSQL Loading**
   - Gold data is loaded into PostgreSQL tables.

6. **Visualization**
   - Streamlit dashboard connects to PostgreSQL.
   - Displays key metrics and tables.

---

##  How to Run the Project

### 1️ Prerequisites
- Docker
- Docker Compose
- Git

---

### 2️ Clone the Repository
```bash
git clone <your-github-repo-url>
cd batch-etl-medallion
```
### 3.Start All Services
```
docker-compose up -d
```
### 4. Generate Raw Event Data
```
python data-generator/generate_events.py
```

### 5.Run Spark ETL (Manual Test)
```
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.access.key=minioadmin \
--conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
/opt/spark/work-dir/spark/jobs/etl_job.py
```
### 6. Airflow UI
- Open: http://localhost:8081
- Login:
```
Username: admin
Password: admin
```
- Enable and trigger DAG: batch_etl_medallion
---
### 7. MinIO UI
- Open: http://localhost:9001
- Access Key: minioadmin
- Secret Key: minioadmin123

---
### 8️. PostgreSQL Verification
```
docker exec -it postgres psql -U postgres -d analytics

SELECT COUNT(*) FROM daily_metrics;
SELECT * FROM daily_metrics LIMIT 10;
```
----
### 9.Streamlit Dashboard
- Open: http://localhost:8501
- View key metrics and aggregated data.

---
### Key Learnings
- Implemented a full medallion data lake architecture
- Built a containerized Spark + Airflow pipeline
- Integrated Spark with S3-compatible storage (MinIO)
- Handled real-world issues like:
- Dependency management
- Networking in Docker
- Orchestration failures
- Designed an end-to-end, production-style ETL system
---
## Author
**Sanjana Medapati**
