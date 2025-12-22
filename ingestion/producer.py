import pandas as pd
import json
import os
import uuid
import multiprocessing
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ================= CONFIG =================
KAFKA_TOPIC = "user_behavior"
BROKER = "kafka:9092"
CSV_FILES = ["./data/2019-Nov.csv", "./data/2019-Oct.csv"]
CHUNK_SIZE = 10000 

# Unique segments to avoid "cloning"
WINDOWS = [
    {"name": "July/Aug", "offset_days": -112, "start_row": 1, "total_rows": 10_000_000},
    {"name": "Sept/Oct", "offset_days": -56,  "start_row": 10_000_001, "total_rows": 20_000_000},
    {"name": "Nov/Dec",  "offset_days": 0,    "start_row": 20_000_001, "total_rows": 30_000_000}
]

# Calculate the base shift for 2025
original_end_date = datetime(2019, 11, 30)
base_offset = ((datetime.now() - original_end_date).days // 7) * 7

# ================= TURBO WORKER =================
def process_turbo(file_path, window):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="lz4",
        linger_ms=50,
        batch_size=512 * 1024,
        buffer_memory=128 * 1024 * 1024
    )

    final_offset = base_offset + window['offset_days']
    print(f"[PID {os.getpid()}] Starting {window['name']} on {os.path.basename(file_path)}")
    
    try:
        csv_stream = pd.read_csv(
            file_path, 
            chunksize=CHUNK_SIZE,
            skiprows=range(1, window['start_row']), 
            nrows=window['total_rows'],
            usecols=['event_time', 'event_type', 'product_id', 'category_code', 'brand', 'price', 'user_id', 'user_session']
        )

        total_sent = 0
        for chunk in csv_stream:
            # Random sampling for variety
            chunk = chunk.sample(frac=0.1)
            chunk = chunk.where(pd.notnull(chunk), None)

            # Vectorized Time Shift
            times = pd.to_datetime(chunk['event_time']) + timedelta(days=final_offset)
            chunk['event_time'] = times.dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Batch convert to dictionary
            records = chunk.to_dict('records')
            
            for record in records:
                record['event_id'] = str(uuid.uuid4())
                producer.send(KAFKA_TOPIC, record)
            
            total_sent += len(records)
            if total_sent % 50000 == 0:
                print(f" {window['name']} [{os.path.basename(file_path)}]: {total_sent:,} sampled rows sent")

        producer.flush()
    except Exception as e:
        print(f" Error in {window['name']}: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    for window in WINDOWS:
        print(f"\n--- INITIATING {window['name']} WINDOW ---")
        processes = []
        for file in CSV_FILES:
            p = multiprocessing.Process(target=process_turbo, args=(file, window))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
    
    print("\n PIPELINE COMPLETE. 6 Months of Unique Data Injected.")