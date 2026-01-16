from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# ---------------- DEFAULT ARGS ----------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------- DAG ----------------
with DAG(
    dag_id="batch_etl_medallion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # ---------------- SPARK ETL ----------------
    spark_etl = SparkSubmitOperator(
        task_id="spark_etl_bronze_to_gold",
        application="/opt/spark/work-dir/spark/jobs/etl_job.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
    )

    # ---------------- DATA QUALITY CHECK ----------------
    def data_quality_check():
        conn = psycopg2.connect(
            host="postgres",
            database="analytics",
            user="postgres",
            password="postgres",
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM daily_metrics;")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError("Data quality check failed: No data found")
        conn.close()

    dq_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    # ---------------- LOAD TO POSTGRES ----------------
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            year INT,
            month INT,
            day INT,
            daily_active_users INT,
            total_events INT
        );
        """,
    )

    spark_etl >> create_table >> dq_check
