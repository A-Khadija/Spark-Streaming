# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     from_json,
#     col,
#     split,
#     dayofweek,
#     to_timestamp,
#     window,
#     sum as spark_sum,
# )
# from pyspark.sql.types import StructType, StructField, StringType

# import redis
# from inference.predictor import predict_cart_event

# # =====================================================
# # CONFIG
# # =====================================================
# KAFKA_BOOTSTRAP = "kafka:9092"
# TOPIC = "user_behavior"

# POSTGRES_URL = "jdbc:postgresql://postgres:5432/ecommerce"
# POSTGRES_PROPERTIES = {
#     "user": "user",
#     "password": "password",
#     "driver": "org.postgresql.Driver",
#     "batchsize": "10000",
#     "rewriteBatchedInserts": "true",
# }

# REDIS_HOST = "redis"
# REDIS_PORT = 6379

# # =====================================================
# # SCHEMA (CANONICAL)
# # =====================================================
# schema = StructType([
#     StructField("event_id", StringType()),
#     StructField("event_time", StringType()),
#     StructField("event_type", StringType()),
#     StructField("product_id", StringType()),
#     StructField("category_code", StringType()),
#     StructField("brand", StringType()),
#     StructField("price", StringType()),
#     StructField("user_id", StringType()),
#     StructField("user_session", StringType()),
# ])

# # =====================================================
# # POSTGRES WRITERS
# # =====================================================
# def write_raw_events(df, batch_id):
#     (
#         df.select(
#             "event_time",
#             "event_type",
#             "product_id",
#             "category_code",
#             "brand",
#             "price",
#             "user_id",
#             "user_session",
#             "category_level1",
#             "category_level2",
#             "event_weekday",
#         )
#         .write
#         .jdbc(
#             url=POSTGRES_URL,
#             table="raw_events",
#             mode="append",
#             properties=POSTGRES_PROPERTIES,
#         )
#     )

# def write_sales_per_minute(df, batch_id):
#     (
#         df.write
#         .jdbc(
#             url=POSTGRES_URL,
#             table="analytics.sales_per_minute",
#             mode="append",
#             properties=POSTGRES_PROPERTIES,
#         )
#     )

# # =====================================================
# # REDIS + ML (SAFE)
# # =====================================================
# def redis_and_ml_sink(df, batch_id):
#     r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

#     cart_events = (
#         df.filter(col("event_type") == "cart")
#           .select("user_id", "user_session", "price", "category_level1")
#           .limit(100)  # safety guard
#           .collect()
#     )

#     for row in cart_events:
#         event = row.asDict()

#         # lightweight real-time signal
#         r.hincrby(f"user:{event['user_id']}", "cart_count", 1)

#         # ML inference
#         predict_cart_event(event)

# # =====================================================
# # MAIN STREAM
# # =====================================================
# def process_stream():
#     spark = (
#         SparkSession.builder
#         .appName("EcommerceStreamingFinal")
#         .config(
#             "spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
#             "org.postgresql:postgresql:42.6.0",
#         )
#         .getOrCreate()
#     )

#     spark.sparkContext.setLogLevel("WARN")

#     # ---------------- Kafka Source ----------------
#     raw_stream = (
#         spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#         .option("subscribe", TOPIC)
#         .option("startingOffsets", "earliest")
#         .option("failOnDataLoss", "false")
#         .load()
#     )

#     json_stream = raw_stream.select(
#         from_json(col("value").cast("string"), schema).alias("data")
#     ).select("data.*")

#     # ---------------- Transform ----------------
#     transformed = (
#         json_stream
#         .withColumn("event_time", to_timestamp(col("event_time")))
#         .withColumn("user_id", col("user_id").cast("int"))
#         .withColumn("product_id", col("product_id").cast("int"))
#         .withColumn("price", col("price").cast("double"))
#         .withColumn("category_level1", split(col("category_code"), "\.").getItem(0))
#         .withColumn("category_level2", split(col("category_code"), "\.").getItem(1))
#         .withColumn("event_weekday", dayofweek(col("event_time")))
#     )

#     # ---------------- Raw archive ----------------
#     transformed.writeStream.foreachBatch(write_raw_events) \
#         .option("checkpointLocation", "/tmp/checkpoints/raw") \
#         .trigger(processingTime="30 seconds") \
#         .start()

#     # ---------------- Sales per minute ----------------
#     sales_per_minute = (
#         transformed
#         .filter(col("event_type") == "purchase")
#         .groupBy(window(col("event_time"), "1 minute"))
#         .agg(spark_sum("price").alias("total_sales"))
#         .select(col("window.start").alias("minute"), "total_sales")
#     )

#     sales_per_minute.writeStream.foreachBatch(write_sales_per_minute) \
#         .option("checkpointLocation", "/tmp/checkpoints/sales") \
#         .trigger(processingTime="30 seconds") \
#         .start()

#     # ---------------- Redis + ML ----------------
#     transformed.writeStream.foreachBatch(redis_and_ml_sink) \
#         .option("checkpointLocation", "/tmp/checkpoints/ml") \
#         .trigger(processingTime="30 seconds") \
#         .start()

#     spark.streams.awaitAnyTermination()

# # =====================================================
# # ENTRY POINT
# # =====================================================
# if __name__ == "__main__":
#     process_stream()
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    split,
    dayofweek,
    to_timestamp,
    window,
    sum as spark_sum,
)
from pyspark.sql.types import StructType, StructField, StringType
import redis
from inference.predictor import predict_cart_event

<<<<<<< HEAD
=======
import redis
from inference.predictor import predict_cart_event

>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
# =====================================================
# CONFIG
# =====================================================
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "user_behavior"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/ecommerce"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
    "rewriteBatchedInserts": "true",
}

<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
REDIS_HOST = "redis"
REDIS_PORT = 6379

# =====================================================
<<<<<<< HEAD
# SCHEMA (CANONICAL)
# =====================================================
=======
# SCHEMA
# =====================================================
schema = StructType(
    [
        StructField("event_time", StringType()),
        StructField("processing_time", StringType()),
        StructField("event_type", StringType()),
        StructField("product_id", IntegerType()),
        StructField("category_id", IntegerType()),
        StructField("category_code", StringType()),
        StructField("brand", StringType()),
        StructField("price", FloatType()),
        StructField("user_id", IntegerType()),
        StructField("user_session", StringType()),
    ]
)

=======
# ================= SCHEMA =================
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("user_id", StringType()),
    StructField("user_session", StringType()),
])
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459

# =====================================================
<<<<<<< HEAD
# POSTGRES WRITERS
# =====================================================
def write_raw_events(df, batch_id):
    count = df.count()
    if count > 0:
        print(f"DEBUG [Batch {batch_id}]: Writing {count} raw events to Postgres...")
        (
            df.select(
                "event_time", "event_type", "product_id", "category_code",
                "brand", "price", "user_id", "user_session",
                "category_level1", "category_level2", "event_weekday",
            )
            .write
            .jdbc(url=POSTGRES_URL, table="raw_events", mode="append", properties=POSTGRES_PROPERTIES)
        )
        print(f"DEBUG [Batch {batch_id}]: Raw events write complete.")

def write_sales_per_minute(df, batch_id):
    if df.isEmpty():
        return

    count = df.count()
    print(f"DEBUG [Batch {batch_id}]: UPSERTing {count} sales windows to Postgres...")
    
    staging_table = f"staging_sales_{batch_id}"
    df.write.jdbc(url=POSTGRES_URL, table=staging_table, mode="overwrite", properties=POSTGRES_PROPERTIES)

    upsert_sql = f"""
    INSERT INTO analytics.sales_per_minute (minute, total_sales)
    SELECT minute, total_sales FROM {staging_table}
    ON CONFLICT (minute) 
    DO UPDATE SET total_sales = EXCLUDED.total_sales;
    DROP TABLE {staging_table};
    """
    
    try:
        conn = psycopg2.connect("host=postgres dbname=ecommerce user=user password=password")
        cur = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        cur.close()
        conn.close()
        print(f"DEBUG [Batch {batch_id}]: Sales UPSERT complete.")
    except Exception as e:
        print(f"ERROR [Batch {batch_id}]: Upsert failed: {e}")
# =====================================================
# REDIS + ML (SAFE)
# =====================================================
def redis_and_ml_sink(df, batch_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    cart_events = (
        df.filter(col("event_type") == "cart")
          .select("user_id", "user_session", "price", "category_level1", "category_level2", "category_code", "event_weekday")
          .limit(100)
          .collect()
    )

    if cart_events:
        print(f"DEBUG [Batch {batch_id}]: Processing {len(cart_events)} cart events for ML Inference...")
        for row in cart_events:
            event = row.asDict()
            r.hincrby(f"user:{event['user_id']}", "cart_count", 1)
            predict_cart_event(event)
        print(f"DEBUG [Batch {batch_id}]: ML Inference/Redis updates complete.")
# =====================================================
# MAIN STREAM
# =====================================================
=======
# POSTGRES WRITER (ARCHIVE)
# =====================================================
def write_to_postgres(df, batch_id):
    (
        df.repartition(8)
          .write
          .jdbc(
              url=POSTGRES_URL,
              table="raw_events",
              mode="append",
              properties=POSTGRES_PROPERTIES,
          )
    )
<<<<<<< HEAD
    print(f"Batch {batch_id}: written to PostgreSQL")


# =====================================================
# REDIS WRITERS (REAL-TIME METRICS)
# =====================================================
def write_category_stats(df, batch_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pipe = r.pipeline()

    # reset du ZSET à chaque batch (outputMode complete)
    pipe.delete("category_stats")

    for row in df.collect():
        if row["category_code"] is not None:
            pipe.zadd("category_stats", {row["category_code"]: int(row["count"])})

    pipe.execute()
    print(f"Batch {batch_id}: category_stats ZSET updated")


def write_user_activity(df, batch_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pipe = r.pipeline()

    for row in df.collect():
        if row["user_session"]:
            pipe.set(
                f"user_activity:{row['user_session']}",
                row["activity_count"],
            )

    pipe.execute()
    print(f"Batch {batch_id}: user activity written to Redis")


# =====================================================
# ML PREDICTION STREAM
# =====================================================
def predict_batch(df, batch_id):
    """
    Perform real-time ML inference ONLY on cart events
    """
    cart_events = df.filter(col("event_type") == "cart").collect()

    for row in cart_events:
        event = row.asDict()
        predict_cart_event(event)

    print(f"Batch {batch_id}: ML predictions generated")


# =====================================================
# MAIN STREAM
# =====================================================
=======
    print(f"Batch {batch_id} written to Postgres")

# ================= MAIN =================
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
def process_stream():
    spark = (
        SparkSession.builder
        .appName("EcommerceStreamingFinal")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
<<<<<<< HEAD
    print("INFO: Spark Stream starting... Listening for Kafka messages...")
    # ---------------- Kafka Source ----------------
=======

<<<<<<< HEAD
    # -------------------------------
    # READ FROM KAFKA
    # -------------------------------
    raw_stream = (
        spark.readStream.format("kafka")
=======
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
    raw_stream = (
        spark.readStream
        .format("kafka")
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

<<<<<<< HEAD
    # -------------------------------
    # PARSE JSON
    # -------------------------------
=======
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

<<<<<<< HEAD
    # ---------------- Transform + WATERMARK ----------------
=======
<<<<<<< HEAD
    # -------------------------------
    # FEATURE ENGINEERING
    # -------------------------------
    transformed = (
        json_stream.withColumn("event_time", to_timestamp(col("event_time")))
=======
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
    transformed = (
        json_stream
        .withColumn("event_time", to_timestamp(col("event_time")))
<<<<<<< HEAD
        .withWatermark("event_time", "2 minutes")  # Allow 2 min of lateness
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("price", col("price").cast("double"))
=======
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
        .withColumn("processing_time", to_timestamp(col("processing_time")))
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
        .withColumn("category_level1", split(col("category_code"), "\.").getItem(0))
        .withColumn("category_level2", split(col("category_code"), "\.").getItem(1))
        .withColumn("event_weekday", dayofweek(col("event_time")))
    )

<<<<<<< HEAD
    # ---------------- Raw archive ----------------
    transformed.writeStream.foreachBatch(write_raw_events) \
        .option("checkpointLocation", "/tmp/checkpoints/raw") \
        .trigger(processingTime="30 seconds") \
=======
<<<<<<< HEAD
    # -------------------------------
    # ARCHIVE STREAM → POSTGRES
    # -------------------------------
    transformed.writeStream.foreachBatch(write_to_postgres).option(
        "checkpointLocation", "/tmp/checkpoints/postgres"
    ).trigger(processingTime="10 seconds").start()

    # -------------------------------
    # REAL-TIME METRICS → REDIS
    # -------------------------------
    category_counts = transformed.groupBy("category_code").count()
    user_activity = transformed.groupBy("user_session").agg(
        count("*").alias("activity_count")
    )

    category_counts.writeStream.outputMode("complete").foreachBatch(
        write_category_stats
    ).option("checkpointLocation", "/tmp/checkpoints/category").trigger(
        processingTime="10 seconds"
    ).start()

    user_activity.writeStream.outputMode("complete").foreachBatch(
        write_user_activity
    ).option("checkpointLocation", "/tmp/checkpoints/activity").trigger(
        processingTime="10 seconds"
    ).start()

    # -------------------------------
    # REAL-TIME ML PREDICTION
    # -------------------------------
    transformed.writeStream.foreachBatch(predict_batch).option(
        "checkpointLocation", "/tmp/checkpoints/prediction"
    ).trigger(processingTime="10 seconds").start()

    print("🚀 Spark Streaming with ML inference started")
    spark.streams.awaitAnyTermination()


# =====================================================
# ENTRY POINT
# =====================================================
=======
    (
        transformed.writeStream
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/postgres")
        .trigger(processingTime="30 seconds")
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
        .start()

    # ---------------- Sales per minute ----------------
    sales_per_minute = (
        transformed
        .filter(col("event_type") == "purchase")
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(spark_sum("price").alias("total_sales"))
        .select(col("window.start").alias("minute"), "total_sales")
    )

    # Changed outputMode to "update" to support streaming aggregations
    sales_per_minute.writeStream.foreachBatch(write_sales_per_minute) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/sales") \
        .trigger(processingTime="30 seconds") \
        .start()

    # ---------------- Redis + ML ----------------
    transformed.writeStream.foreachBatch(redis_and_ml_sink) \
        .option("checkpointLocation", "/tmp/checkpoints/ml") \
        .trigger(processingTime="30 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

<<<<<<< HEAD
# =====================================================
# ENTRY POINT
# =====================================================
=======
>>>>>>> beba52005e31ac16ad1fb5737a86aefeb8436459
>>>>>>> 568f93bee2e22fd33692c4cb888755cae63169e9
if __name__ == "__main__":
    process_stream()