from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth,
    countDistinct, count
)

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder \
    .appName("Bronze-Silver-Gold-ETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------- MINIO CONFIG ----------------
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}"
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.access.key", ACCESS_KEY
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.secret.key", SECRET_KEY
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.path.style.access", "true"
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

# ---------------- PATHS ----------------
BRONZE_PATH = "s3a://bronze/raw/user_events.csv"
SILVER_PATH = "s3a://silver/events"
GOLD_PATH = "s3a://gold/daily_metrics"

# ---------------- BRONZE → SILVER ----------------
print("Reading Bronze data...")
bronze_df = spark.read.option("header", "true").csv(BRONZE_PATH)

silver_df = bronze_df \
    .withColumn("user_id", col("user_id").cast("int")) \
    .withColumn("event_timestamp", to_timestamp("event_timestamp")) \
    .dropna(subset=["user_id", "event_type", "event_timestamp"]) \
    .withColumn("year", year("event_timestamp")) \
    .withColumn("month", month("event_timestamp")) \
    .withColumn("day", dayofmonth("event_timestamp"))

print("Writing Silver data...")
silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

# ---------------- SILVER → GOLD ----------------
print("Reading Silver data...")
silver_read_df = spark.read.parquet(SILVER_PATH)

gold_df = silver_read_df.groupBy("year", "month", "day") \
    .agg(
        countDistinct("user_id").alias("daily_active_users"),
        count("*").alias("total_events")
    )

print("Writing Gold data...")
gold_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(GOLD_PATH)

print("ETL Job Completed Successfully")

spark.stop()
