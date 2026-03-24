"""
Spark Structured Streaming job: Kafka → Delta Lake.

Reads financial transaction JSON from Kafka, applies a rich set of inline
transformations (UDFs, conditionals, hashing, null-handling, geo-features,
risk classification, PII masking), and writes to a Delta table.

──────────────────────────────────────────────────────────────────────
SPARK CONCEPTS EXERCISED IN THIS PIPELINE
──────────────────────────────────────────────────────────────────────
  1. Structured Streaming     — readStream / writeStream with Kafka
  2. from_json + schema       — parse raw bytes into typed columns
  3. withColumn               — add/replace columns declaratively
  4. when / otherwise         — SQL CASE-WHEN as column expressions
  5. UDFs (Python)            — classify_risk, mask_account, etc.
  6. Built-in functions       — year, month, dayofweek, sha2, log,
                                coalesce, regexp_replace, concat_ws
  7. Null handling            — coalesce, na.fill, isNull checks
  8. .transform()             — composable DataFrame → DataFrame chains
  9. Delta Lake sink          — append with merge-schema + partitioning
 10. Checkpointing            — exactly-once delivery guarantee
──────────────────────────────────────────────────────────────────────

Run inside Docker:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \\
        /opt/spark-jobs/streaming_job.py

Run locally:
    python spark_jobs/streaming_job.py --local
"""

import argparse
import logging
import sys

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Import the reusable transforms module
from transforms import apply_all_transforms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("streaming_job")

# ── Transaction JSON schema ───────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("sender_account_id", StringType(), True),
    StructField("receiver_account_id", StringType(), True),
    StructField("sender_account", StringType(), True),
    StructField("receiver_account", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("sender_name", StringType(), True),
    StructField("receiver_name", StringType(), True),
    StructField("sender_bank_code", StringType(), True),
    StructField("receiver_bank_code", StringType(), True),
    StructField("sender_country", StringType(), True),
    StructField("receiver_country", StringType(), True),
    StructField("fee", DoubleType(), True),
    StructField("exchange_rate", DoubleType(), True),
    StructField("reference_id", StringType(), True),
    StructField("memo", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("is_flagged", BooleanType(), True),
    StructField("category", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_fingerprint", StringType(), True),
    StructField("session_id", StringType(), True),
])


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(cfg: dict, local: bool) -> SparkSession:
    """Build a SparkSession with Delta Lake support."""
    builder = (
        SparkSession.builder
        .appName(cfg["spark"]["app_name"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.sql.streaming.schemaInference", "false")
    )

    if local:
        builder = builder.master("local[*]")
    else:
        builder = builder.master(cfg["spark"]["master"])

    return builder.getOrCreate()


def run_streaming_job(cfg: dict, local: bool):
    """Main streaming pipeline: Kafka source → transform → Delta sink.

    The transformation chain uses .transform() to compose reusable
    DataFrame→DataFrame functions from the transforms module:

        raw → parse → null_handling → time_dims → amount_features
            → risk_features → geo_features → anonymize → session
            → write to Delta
    """
    spark = create_spark_session(cfg, local)
    spark.sparkContext.setLogLevel("WARN")

    kafka_cfg = cfg["kafka"]
    spark_cfg = cfg["spark"]

    bootstrap = (
        kafka_cfg["bootstrap_servers"] if local
        else kafka_cfg["internal_bootstrap_servers"]
    )
    checkpoint = (
        spark_cfg["local_checkpoint"] if local
        else spark_cfg["checkpoint_location"]
    )
    delta_path = (
        spark_cfg["local_delta_output"] if local
        else spark_cfg["delta_output_path"]
    )

    logger.info(f"Kafka bootstrap: {bootstrap}")
    logger.info(f"Delta output:    {delta_path}")
    logger.info(f"Checkpoint:      {checkpoint}")

    # ── 1. Read from Kafka (Structured Streaming source) ──────
    #
    # Spark concept: readStream creates an unbounded DataFrame.
    # Each micro-batch pulls up to maxOffsetsPerTrigger records.
    # Columns: key (binary), value (binary), topic, partition,
    #          offset, timestamp, timestampType
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", kafka_cfg["topic"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", spark_cfg["max_offsets_per_trigger"])
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload (from_json + schema) ────────────
    #
    # Spark concept: from_json() deserializes a JSON string column
    # into a nested StructType.  We then flatten with "data.*"
    # to promote nested fields to top-level columns.
    parsed_df = (
        raw_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            col("topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "kafka_key", "data.*",
            "topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        )
    )

    # ── 3. Apply all inline transforms ────────────────────────
    #
    # Spark concept: .transform(fn) lets you compose DataFrame
    # transformations as reusable functions.  apply_all_transforms
    # chains 7 transforms that add ~25 derived columns covering:
    #
    #   null_handling      → coalesce, na.fill defaults
    #   time_dimensions    → year, month, day, hour, weekend, quarter
    #   amount_features    → bucket, net_amount, fee_pct, log_amount
    #   risk_features      → UDF risk_tier, risk_level, needs_review
    #   geo_features       → cross_border, corridor, intra_bank
    #   anonymized_columns → sha2 hashing, account masking, IP anonymize
    #   session_features   → device_short_id, memo_length, row_id
    #
    # Each transform is defined in spark_jobs/transforms.py with
    # detailed docstrings explaining the Spark concept it uses.
    enriched_df = parsed_df.transform(apply_all_transforms)

    # ── 4. Write to Delta Lake ────────────────────────────────
    #
    # Spark concepts:
    #   - writeStream + format("delta") → streaming Delta sink
    #   - append mode → new rows only (no updates/deletes)
    #   - checkpointLocation → WAL for exactly-once guarantees
    #   - mergeSchema → auto-evolve schema if new columns appear
    #   - partitionBy → physical data layout for partition pruning
    #   - trigger(processingTime=...) → micro-batch interval
    query = (
        enriched_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("txn_year", "txn_month", "currency")
        .trigger(processingTime=spark_cfg["trigger_interval"])
        .start(delta_path)
    )

    logger.info("Streaming query started — waiting for termination...")
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser(description="Kafka → Delta Lake streaming job")
    parser.add_argument(
        "--config",
        default="config/app_config.yaml",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run in local mode (no Spark cluster required)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_streaming_job(cfg, args.local)


if __name__ == "__main__":
    main()
