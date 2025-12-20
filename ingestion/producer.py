import pandas as pd
import json
import os
<<<<<<< HEAD
from datetime import datetime, timezone
from kafka import KafkaProducer

# =====================================================
# CONFIGURATION
# =====================================================
=======
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# ================= CONFIG =================
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
KAFKA_TOPIC = "user_behavior"
BROKER = "kafka:9092"

CSV_FILES = [
    "./data/2019-Oct.csv",
    "./data/2019-Nov.csv",
]

<<<<<<< HEAD
# Optional limit for demo/testing
DEMO_LIMIT_ROWS = None

# Simulated ingestion rate (events per second)
EVENT_RATE = 100  # 100 events/sec → realistic streaming

# =====================================================
# INITIALIZE KAFKA PRODUCER
# =====================================================
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"✅ Kafka Producer connected to {BROKER}")

=======
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
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459

# =====================================================
# PROCESS CSV FILE
# =====================================================
def process_file(file_path):
    if not os.path.exists(file_path):
<<<<<<< HEAD
        print(f"❌ File not found: {file_path}")
        return

    print(f"📄 Processing file: {file_path}")

    chunk_size = 5000
    total_processed = 0

    base_event_time = None
    time_offset = None

    csv_stream = pd.read_csv(file_path, chunksize=chunk_size)

    for chunk in csv_stream:
        chunk = chunk.astype(object)
=======
        print(f"File not found: {file_path}")
        return

    print(f"Processing {file_path}")

    csv_stream = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
    total = 0

    for chunk in csv_stream:
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
        chunk = chunk.where(pd.notnull(chunk), None)
        processing_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

<<<<<<< HEAD
        for _, row in chunk.iterrows():

            # -------------------------------
            # REAL-TIME SIMULATION LOGIC
            # -------------------------------
            original_event_time = pd.to_datetime(row["event_time"], utc=True)

            if base_event_time is None:
                base_event_time = original_event_time
                time_offset = pd.Timestamp.now(tz="UTC") - base_event_time

            simulated_event_time = datetime.utcnow()

            # -------------------------------
            # BUILD MESSAGE
            # -------------------------------
            message = {
                "event_time": simulated_event_time.strftime("%Y-%m-%d %H:%M:%S"),
                "processing_time": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "event_type": row["event_type"],
                "product_id": int(row["product_id"]),
                "category_id": int(row["category_id"]),
                "category_code": row["category_code"],
                "brand": row["brand"],
                "price": float(row["price"]) if row["price"] is not None else 0.0,
                "user_id": int(row["user_id"]),
                "user_session": row["user_session"],
            }

            # -------------------------------
            # SEND TO KAFKA
            # -------------------------------
            producer.send(KAFKA_TOPIC, message)
            total_processed += 1

            if total_processed % 1000 == 0:
                print(
                    f"[{file_path}] Sent {total_processed} events | "
                    f"Last event: {message['event_type']} @ {message['event_time']}"
                )

            # Control ingestion speed
            time.sleep(1 / EVENT_RATE)

            if DEMO_LIMIT_ROWS and total_processed >= DEMO_LIMIT_ROWS:
                print(f"⚠️ Demo limit reached: {DEMO_LIMIT_ROWS}")
                producer.flush()
                return
=======
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
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459

        producer.flush()
        print(f"➡ {file_path}: {total:,} rows sent")

<<<<<<< HEAD
    print(f"✅ Finished processing {file_path} | Total events: {total_processed}")


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    try:
        for csv_file in CSV_FILES:
            process_file(csv_file)

        print("🎉 All files processed successfully.")

    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")

    finally:
        producer.close()
        print("🔌 Kafka Producer closed.")
=======
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
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
