# import pandas as pd
# import json
# import os
# import uuid
# from datetime import datetime, timezone
# from kafka import KafkaProducer

# # ================= CONFIG =================
# KAFKA_TOPIC = "user_behavior"
# BROKER = "kafka:9092"

# CSV_FILES = [
#     "./data/2019-Nov.csv",
#     "./data/2019-Oct.csv",
# ]

# CHUNK_SIZE = 20000  # aggressive for speed

# # ================= PRODUCER =================
# producer = KafkaProducer(
#     bootstrap_servers=BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     linger_ms=50,
#     batch_size=128 * 1024,
#     compression_type="lz4",
#     acks=1,
# )

# print(" Kafka Producer started")

# def process_file(file_path):
#     if not os.path.exists(file_path):
#         print(f"File not found: {file_path}")
#         return

#     print(f"Processing {file_path}")

#     csv_stream = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
#     total = 0

#     for chunk in csv_stream:
#         chunk = chunk.where(pd.notnull(chunk), None)
#         processing_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

#         for row in chunk.itertuples(index=False):
#             message = {
#                 "event_id": str(uuid.uuid4()),
#                 "event_time": pd.to_datetime(row.event_time).strftime("%Y-%m-%d %H:%M:%S"),
#                 "processing_time": processing_time,
#                 "event_type": row.event_type,
#                 "product_id": int(row.product_id),
#                 "category_id": int(row.category_id),
#                 "category_code": row.category_code,
#                 "brand": row.brand,
#                 "price": float(row.price) if row.price else 0.0,
#                 "user_id": int(row.user_id),
#                 "user_session": row.user_session,
#             }

#             producer.send(KAFKA_TOPIC, message)
#             total += 1

#         producer.flush()
#         print(f"➡ {file_path}: {total:,} rows sent")

#     print(f"Finished {file_path}")

# # ================= MAIN =================
# try:
#     for file in CSV_FILES:
#         process_file(file)

# except KeyboardInterrupt:
#     print("Producer interrupted")

# finally:
#     producer.close()
#     print("Producer closed")
import pandas as pd
import json
import uuid
import os
import multiprocessing
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ================= CONFIG =================
KAFKA_TOPIC = "user_behavior"
BROKER = "kafka:9092"
CSV_FILES = ["./data/2019-Nov.csv", "./data/2019-Oct.csv"]
CHUNK_SIZE = 10000  

# ================= WINDOW DEFINITIONS =================
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
    # Optimized Producer for high-throughput
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="lz4",
        linger_ms=50,             # Batch messages every 50ms
        batch_size=512 * 1024,    # 512KB batch size
        buffer_memory=128 * 1024 * 1024  # 128MB memory buffer
    )

    final_offset = base_offset + window['offset_days']
    print(f"[PID {os.getpid()}] Starting {window['name']} on {os.path.basename(file_path)}")
    try:
        # total_rows is your "scan area". frac=0.1 means we send 10% of that area.
        csv_stream = pd.read_csv(
            file_path, 
            chunksize=CHUNK_SIZE,
            skiprows=range(1, window['start_row']), 
            nrows=window['total_rows'],
            # We explicitly define columns here to save memory
            usecols=['event_time', 'event_type', 'product_id', 'category_code', 'brand', 'price', 'user_id', 'user_session']
        )

        total_sent = 0
        for chunk in csv_stream:
            # 1. SAMPLING: Picks random rows throughout the chunk
            # This ensures we hit multiple hours/days in the source file
            chunk = chunk.sample(frac=0.1)
            chunk = chunk.where(pd.notnull(chunk), None)

            # 2. SPEED BOOST 2: Vectorized Time Shift
            # Done on the chunk level for speed
            times = pd.to_datetime(chunk['event_time']) + timedelta(days=final_offset)
            chunk['event_time'] = times.dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # 3. SPEED BOOST 3: Batch convert to dictionary
            records = chunk.to_dict('records')
            
            for record in records:
                record['event_id'] = str(uuid.uuid4())
                producer.send(KAFKA_TOPIC, record)
            
            total_sent += len(records)
            if total_sent % 50000 == 0:
                print(f" {window['name']} [{os.path.basename(file_path)}]: {total_sent:,} sampled rows sent")

        # 4. Final flush for the window
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
            if os.path.exists(file):
                p = multiprocessing.Process(target=process_turbo, args=(file, window))
                processes.append(p)
                p.start()
        
        for p in processes:
            p.join()
    
    print("\n PIPELINE COMPLETE. 6 Months of Unique Data Injected.")