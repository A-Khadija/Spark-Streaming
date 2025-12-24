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
import psycopg2
from inference.predictor import predict_cart_event

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
    "batchsize": "2000",
    "rewriteBatchedInserts": "true",
}

REDIS_HOST = "redis"
REDIS_PORT = 6379

# =====================================================
# SCHEMA
# =====================================================
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

# =====================================================
# POSTGRES WRITERS (DEBUG ENABLED)
# =====================================================
def write_raw_events(df, batch_id):
    count = df.count()
    if count > 0:
        print(f"DEBUG [Batch {batch_id}]: Writing {count} raw events to Postgres...")
        (
            df.select(
                col("event_time"),
                col("event_type"),
                col("product_id").cast("int"),  # <--- FORCE CAST HERE
                col("category_code"),
                col("brand"),
                col("price").cast("double"),
                col("user_id").cast("int"),     # <--- FORCE CAST HERE
                col("user_session"),
                col("category_level1"),
                col("category_level2"),
                col("event_weekday")
            )
            .coalesce(1)
            .write
            .jdbc(
                url=POSTGRES_URL, 
                table="raw_events", 
                mode="append", 
                properties=POSTGRES_PROPERTIES
            )
        )
        print(f"DEBUG [Batch {batch_id}]: Raw events write complete.")

def write_sales_per_minute(df, batch_id):
    if df.isEmpty():
        return
    
    count = df.count()
    print(f"DEBUG [Batch {batch_id}]: UPSERTing {count} sales windows...")
    
    staging_table = f"staging_sales_{batch_id}"
    df.write.jdbc(url=POSTGRES_URL, table=staging_table, mode="overwrite", properties=POSTGRES_PROPERTIES)
    df.coalesce(1).write.jdbc(url=POSTGRES_URL, table=staging_table, mode="overwrite", properties=POSTGRES_PROPERTIES)
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
    except Exception as e:
        print(f"ERROR: Upsert failed: {e}")

# =====================================================
# REDIS + ML
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
        print(f"DEBUG [Batch {batch_id}]: ML Inference on {len(cart_events)} events...")
        for row in cart_events:
            event = row.asDict()
            r.hincrby(f"user:{event['user_id']}", "cart_count", 1)
            predict_cart_event(event)
# =====================================================
# UNIFIED SINK (Combines all tasks into one batch)
# =====================================================
def unified_foreach_sink(df, batch_id):
    # Cache to avoid re-reading Kafka data for each step
    df.cache()
    
    # 1. Raw Events
    write_raw_events(df, batch_id)
    
    # 2. Sales Aggregates
    sales_batch = (
        df.filter(col("event_type") == "purchase")
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(spark_sum("price").alias("total_sales"))
        .select(col("window.start").alias("minute"), "total_sales")
    )
    write_sales_per_minute(sales_batch, batch_id)
    
    # 3. Redis + ML
    redis_and_ml_sink(df, batch_id)
    
    # Crucial: Free the memory after the batch finishes
    df.unpersist()
# =====================================================
# MAIN STREAM
# =====================================================
def process_stream():
    spark = (
        SparkSession.builder
        .appName("EcommerceStreamingFinal")
        .config("spark.sql.shuffle.partitions", "4")  # Reduced from 200
        .config("spark.default.parallelism", "4")
        .config("spark.memory.fraction", "0.8")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Spark Stream starting...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)  # <-- ADD THIS: Limits rows per batch
        .option("failOnDataLoss", "false")
        .load()
    )

    json_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    transformed = (
        json_stream
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withWatermark("event_time", "5 minutes")
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("category_level1", split(col("category_code"), "\.").getItem(0))
        .withColumn("category_level2", split(col("category_code"), "\.").getItem(1))
        .withColumn("event_weekday", dayofweek(col("event_time")))
    )

    query = (
        transformed.writeStream
        .foreachBatch(unified_foreach_sink)
        .option("checkpointLocation", "/tmp/checkpoints/unified_v2")
        .trigger(processingTime="60 seconds") # Give JVM time to recover between batches
        .start()
    )

    query.awaitTermination()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()