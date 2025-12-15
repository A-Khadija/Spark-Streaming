import pandas as pd
import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from datetime import datetime, timezone

# --- CONFIGURATION ---
KAFKA_TOPIC = "user_behavior"
BROKER = "kafka:9092"

# List your 4 files here.
# Make sure these paths are correct relative to where you run the script.
CSV_FILES = [
    "./data/2019-Nov.csv",
    "./data/2019-Oct.csv",
]

DEMO_LIMIT_ROWS = None

# --- INITIALIZE PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f" Starting Producer connected to {BROKER}")


def process_file(file_path):
    if not os.path.exists(file_path):
        print(f" File not found: {file_path}. Skipping.")
        return

    print(f" Reading file: {file_path}...")

    # Chunk size: 5000 is a good balance for memory
    chunk_size = 5000
    total_processed = 0

    # read_csv iterator
    csv_stream = pd.read_csv(
        file_path,
        chunksize=chunk_size,
    )

    for chunk in csv_stream:

        chunk = chunk.astype(object)

        chunk = chunk.where(pd.notnull(chunk), None)

        for index, row in chunk.iterrows():

            original_time = pd.to_datetime(row["event_time"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            processing_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            message = {
                "event_time": original_time,
                "processing_time": processing_time,
                "event_type": row["event_type"],
                "product_id": int(row["product_id"]),
                "category_id": int(row["category_id"]),
                "category_code": row["category_code"],
                "brand": row["brand"],
                "price": float(row["price"]) if row["price"] is not None else 0.0,
                "user_id": int(row["user_id"]),
                "user_session": row["user_session"],
            }

            # 3. Send to Kafka
            producer.send(KAFKA_TOPIC, message)

            total_processed += 1

            # Logging progress
            if total_processed % 1000 == 0:
                print(
                    f"[{file_path}]  Sent {total_processed} events. Last: {message['event_type']} @ {processing_time}"
                )

         
            time.sleep(0.01)

            # Check demo limit
            if DEMO_LIMIT_ROWS and total_processed >= DEMO_LIMIT_ROWS:
                print(f" Reached demo limit of {DEMO_LIMIT_ROWS} for this file.")
                return

        # Flush after every chunk to ensure data is sent
        producer.flush()

    print(f" Finished processing {file_path}")


# --- MAIN LOOP ---
try:
    for csv_file in CSV_FILES:
        process_file(csv_file)
    print("All files processed.")

except KeyboardInterrupt:
    print("\n Stopped by user.")
finally:
    producer.close()
