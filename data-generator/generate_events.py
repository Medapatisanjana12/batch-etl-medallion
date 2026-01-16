import csv
import random
import uuid
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error

# ---------------- CONFIG ----------------
MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
BUCKET_NAME = "bronze"

OUTPUT_FILE = "user_events.csv"
TOTAL_EVENTS = 1_000_000

EVENT_TYPES = ["page_view", "click", "purchase"]
PRODUCT_IDS = [f"P{i}" for i in range(1, 101)]

# ---------------- MINIO CLIENT ----------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

# ---------------- DATA GENERATION ----------------
def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 50000),
        "event_type": random.choice(EVENT_TYPES),
        "product_id": random.choice(PRODUCT_IDS),
        "event_timestamp": (
            datetime.utcnow() - timedelta(
                minutes=random.randint(0, 60 * 24 * 30)
            )
        ).isoformat()
    }

def generate_csv():
    print("Generating CSV file...")
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "event_id",
                "user_id",
                "event_type",
                "product_id",
                "event_timestamp"
            ]
        )
        writer.writeheader()

        for i in range(TOTAL_EVENTS):
            writer.writerow(generate_event())
            if i % 100_000 == 0 and i != 0:
                print(f"{i} events generated...")

    print("CSV generation completed.")

# ---------------- UPLOAD TO MINIO ----------------
def upload_to_minio():
    print("Uploading file to MinIO bronze bucket...")

    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/{OUTPUT_FILE}",
        OUTPUT_FILE
    )

    print("Upload completed successfully.")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    try:
        generate_csv()
        upload_to_minio()
    except S3Error as e:
        print("MinIO error:", e)
