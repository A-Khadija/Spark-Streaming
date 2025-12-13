import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    struct,
    count,
    window,
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

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "kafka:9092"  # Connects to the internal Docker network listener
TOPIC = "user_behavior"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/ecommerce"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
}
REDIS_HOST = "redis"
REDIS_PORT = 6379


# --- SCHEMA DEFINITION ---
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


def write_features_to_redis(batch_df, batch_id):
    """
    Writes Calculated Features to Redis.
    1. Category Counts -> "category_stats:{category}"
    2. User Activity Count -> "user_activity:{user_session}"
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

    print(f"🔥 Writing Batch {batch_id} to Redis...")
    pipe = r.pipeline()

    for row in batch_df.toLocalIterator():
        # Check which type of aggregation this row is
        if "category_code" in row:
            # It's the Category Count Stream
            cat = row["category_code"]
            cnt = row["count"]
            if cat:
                pipe.set(f"category_stats:{cat}", cnt)

        elif "user_session" in row:
            # It's the User Activity Stream (Feature for ML)
            session = row["user_session"]
            act_count = row["activity_count"]
            if session:
                # We store this so the API can look it up later: "How active is this user?"
                pipe.set(f"user_activity:{session}", act_count)

    pipe.execute()


def process_stream():
    spark = (
        SparkSession.builder.appName("EcommerceStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Parse JSON
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # --- TRANSFORMATIONS (Feature Engineering) ---

    transformed_stream = (
        json_stream.withColumn(
            "category_level1", split(col("category_code"), "\.").getItem(0)
        )
        .withColumn("category_level2", split(col("category_code"), "\.").getItem(1))
        .withColumn("event_weekday", dayofweek(to_timestamp(col("event_time"))))
    )

    # --- BRANCH A: BATCH STORAGE (POSTGRES) ---
    # Write the FULL transformed data (with new columns) to Postgres
    postgres_query = (
        transformed_stream.writeStream.foreachBatch(
            lambda df, epoch_id: df.write.jdbc(
                url=POSTGRES_URL,
                table="raw_events",
                mode="append",
                properties=POSTGRES_PROPERTIES,
            )
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- BRANCH B: REAL-TIME ANALYTICS (REDIS) ---

    # 1. Dashboard Metric: Top Categories
    category_counts = transformed_stream.groupBy("category_code").count()

    # 2. ML Feature: User Activity Count (Velocity)
    # "How many actions has this user done in this session?"
    user_activity = transformed_stream.groupBy("user_session").agg(
        count("*").alias("activity_count")
    )

    # Write Category Counts to Redis
    query_cats = (
        category_counts.writeStream.outputMode("complete")
        .foreachBatch(write_features_to_redis)
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Write User Activity to Redis
    query_activity = (
        user_activity.writeStream.outputMode("complete")
        .foreachBatch(write_features_to_redis)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Streaming Started! Writing Features to Redis & History to Postgres...")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    process_stream()
