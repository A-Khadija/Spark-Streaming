from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    count,
    split,
    dayofweek,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

import redis
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
}

REDIS_HOST = "redis"
REDIS_PORT = 6379

# =====================================================
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


# =====================================================
# POSTGRES WRITER (ARCHIVE)
# =====================================================
def write_to_postgres(df, batch_id):
    (
        df.select(
            "event_time",
            "processing_time",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session",
            "category_level1",
            "category_level2",
            "event_weekday",
        ).write.jdbc(
            url=POSTGRES_URL,
            table="raw_events",
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )
    )
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
def process_stream():
    spark = (
        SparkSession.builder.appName("EcommerceStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------
    # READ FROM KAFKA
    # -------------------------------
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # -------------------------------
    # PARSE JSON
    # -------------------------------
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # -------------------------------
    # FEATURE ENGINEERING
    # -------------------------------
    transformed = (
        json_stream.withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("processing_time", to_timestamp(col("processing_time")))
        .withColumn("category_level1", split(col("category_code"), "\.").getItem(0))
        .withColumn("category_level2", split(col("category_code"), "\.").getItem(1))
        .withColumn("event_weekday", dayofweek(col("event_time")))
    )

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
if __name__ == "__main__":
    process_stream()
