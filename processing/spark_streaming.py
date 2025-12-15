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

# ================= CONFIG =================
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

# ================= SCHEMA =================
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


# ================= POSTGRES WRITER =================
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

    print(f" Batch {batch_id}: written to PostgreSQL")


# ================= REDIS WRITERS =================
def write_category_stats(df, batch_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pipe = r.pipeline()

    for row in df.collect():
        if row["category_code"]:
            pipe.set(f"category_stats:{row['category_code']}", row["count"])

    pipe.execute()
    print(f" Batch {batch_id}: category stats written to Redis")


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
    print(f" Batch {batch_id}: user activity written to Redis")


# ================= MAIN STREAM =================
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

    # -------- READ FROM KAFKA --------
    raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")  
    .option("failOnDataLoss", "false")   
    .load()
)


    # -------- PARSE JSON --------
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # -------- FEATURE ENGINEERING --------
    transformed = (
        json_stream.withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "processing_time",
            to_timestamp(col("processing_time"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "category_level1",
            split(col("category_code"), "\.").getItem(0),
        )
        .withColumn(
            "category_level2",
            split(col("category_code"), "\.").getItem(1),
        )
        .withColumn(
            "event_weekday",
            dayofweek(col("event_time")),
        )
    )

    # -------- POSTGRES STREAM --------
    transformed.writeStream.foreachBatch(write_to_postgres).option(
        "checkpointLocation", "/tmp/checkpoints/postgres"
    ).trigger(processingTime="10 seconds").start()

    # -------- REDIS STREAMS --------
    category_counts = transformed.groupBy("category_code").count()
    user_activity = transformed.groupBy("user_session").agg(
        count("*").alias("activity_count")
    )

    category_counts.writeStream.outputMode("complete").foreachBatch(
        write_category_stats
    ).option("checkpointLocation", "/tmp/checkpoints/category").trigger(
        processingTime="15 seconds"
    ).start()

    user_activity.writeStream.outputMode("complete").foreachBatch(
        write_user_activity
    ).option("checkpointLocation", "/tmp/checkpoints/activity").trigger(
        processingTime="5 seconds"
    ).start()

    print(" Spark Streaming started successfully")
    spark.streams.awaitAnyTermination()


# ================= ENTRY POINT =================
if __name__ == "__main__":
    process_stream()
