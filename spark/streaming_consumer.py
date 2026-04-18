"""
PySpark Structured Streaming Consumer
Reads from Kafka topic raw_orders, applies schema + deduplication,
writes micro-batches to PostgreSQL raw.orders via JDBC.

Run:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    spark/streaming_consumer.py
"""

import logging
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Schema matching the producer payload ───────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",        StringType(),  True),
    StructField("user_id",         StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("product_name",    StringType(),  True),
    StructField("category",        StringType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("unit_price",      DoubleType(),  True),
    StructField("amount",          DoubleType(),  True),
    StructField("status",          StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("payment_method",  StringType(),  True),
    StructField("event_ts",        StringType(),  True),
])

# ── Postgres JDBC config ───────────────────────────────────────────────────
JDBC_URL = (
    f"jdbc:postgresql://"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'order_analytics')}"
)

JDBC_PROPS = {
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "driver":   "org.postgresql.Driver",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "OrderStreamingPipeline"))
        .config("spark.sql.shuffle.partitions", "4")      # keep low for local mode
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "raw_orders"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)   # process up to 1000 msgs per micro-batch
        .load()
    )


def parse_and_transform(raw_df: DataFrame) -> DataFrame:
    """
    1. Deserialise JSON from Kafka value bytes
    2. Cast types
    3. Add watermark for late-arriving event deduplication (10 min window)
    4. Add ingestion metadata
    """
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(
            from_json(col("json_str"), ORDER_SCHEMA).alias("data"),
            col("kafka_ts")
        )
        .select("data.*", "kafka_ts")
    )

    transformed = (
        parsed
        .withColumn("event_ts",    to_timestamp(col("event_ts")))
        .withColumn("amount",      col("amount").cast("decimal(10,2)"))
        .withColumn("unit_price",  col("unit_price").cast("decimal(10,2)"))
        .withColumn("ingested_at", current_timestamp())
        # Flag high-value orders (useful for downstream dbt models)
        .withColumn("is_high_value", expr("amount > 5000"))
        .drop("kafka_ts")
    )

    # Watermark: handle events arriving up to 10 minutes late
    watermarked = transformed.withWatermark("event_ts", "10 minutes")

    return watermarked


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink: called for every micro-batch.
    Deduplicates within the batch on order_id before writing.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping.")
        return

    # Deduplicate within micro-batch (handles producer retries)
    deduped = batch_df.dropDuplicates(["order_id"])

    record_count = deduped.count()
    logger.info(f"Batch {batch_id}: writing {record_count} records to raw.orders")

    (
        deduped.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "raw.orders")
        .option("user",   JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )

    logger.info(f"Batch {batch_id}: done.")


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark logs

    logger.info("Spark session created. Reading from Kafka...")

    raw_df = read_kafka_stream(spark)
    transformed_df = parse_and_transform(raw_df)

    query = (
        transformed_df.writeStream
        .foreachBatch(write_batch_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "./spark_checkpoints/raw_orders")
        .trigger(processingTime="30 seconds")   # micro-batch every 30 sec
        .start()
    )

    logger.info("Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    run()