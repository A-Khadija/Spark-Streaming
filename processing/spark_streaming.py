from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    split,
    trim,
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

# ================= CONFIG =================
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

# ================= SCHEMA =================
schema = StructType([
    StructField("event_id", StringType()),
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
])

# ================= POSTGRES WRITER =================
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
    print(f"Batch {batch_id} written to Postgres")

# ================= MAIN =================
def process_stream():
    spark = (
        SparkSession.builder
        .appName("EcommerceStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    transformed = (
        json_stream
        .drop("category_id")
        .filter((col("category_code").isNotNull()) & (trim(col("category_code")) != ""))
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("processing_time", to_timestamp(col("processing_time")))
        .withColumn("category_level1", split(col("category_code"), "\.").getItem(0))
        .withColumn("category_level2", split(col("category_code"), "\.").getItem(1))
        .withColumn("event_weekday", dayofweek(col("event_time")))
    )

    (
        transformed.writeStream
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/postgres")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Spark Streaming started")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
