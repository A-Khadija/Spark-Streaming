import pandas as pd
import json
import os
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# ================= CONFIG =================
KAFKA_TOPIC = "user_behavior"
BROKER = "kafka:9092"

CSV_FILES = [
    "./data/2019-Nov.csv",
    "./data/2019-Oct.csv",
]

CHUNK_SIZE = 20000  # aggressive for speed

# ================= PRODUCER =================
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
    batch_size=128 * 1024,
    compression_type="lz4",
    acks=1,
)

print(" Kafka Producer started")

def process_file(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"Processing {file_path}")

    csv_stream = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
    total = 0

    for chunk in csv_stream:
        chunk = chunk.where(pd.notnull(chunk), None)
        processing_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for row in chunk.itertuples(index=False):
            message = {
                "event_id": str(uuid.uuid4()),
                "event_time": pd.to_datetime(row.event_time).strftime("%Y-%m-%d %H:%M:%S"),
                "processing_time": processing_time,
                "event_type": row.event_type,
                "product_id": int(row.product_id),
                "category_id": int(row.category_id),
                "category_code": row.category_code,
                "brand": row.brand,
                "price": float(row.price) if row.price else 0.0,
                "user_id": int(row.user_id),
                "user_session": row.user_session,
            }

            producer.send(KAFKA_TOPIC, message)
            total += 1

        producer.flush()
        print(f"➡ {file_path}: {total:,} rows sent")

    print(f"Finished {file_path}")

# ================= MAIN =================
try:
    for file in CSV_FILES:
        process_file(file)

except KeyboardInterrupt:
    print("Producer interrupted")

finally:
    producer.close()
    print("Producer closed")
