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
import time as _time

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

# ── Metrics integration (graceful if unavailable) ─────────────
try:
    from metrics_bridge import update_spark_metrics
    _HAS_METRICS = True
    logger.info("Dashboard metrics integration enabled (JSON bridge)")
except Exception as exc:
    _HAS_METRICS = False
    logger.warning("Dashboard metrics unavailable: %s", exc)

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

    # ── 3 + 4. foreachBatch with instrumentation ────────────────
    #
    # We use foreachBatch so we can measure the time spent on:
    #   (a) apply_all_transforms — the enrichment / UDF phase
    #   (b) the Delta write itself
    # and report both to the metrics module for the dashboard.

    def _process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            logger.info(f"Batch {batch_id}: empty, skipping")
            return

        # ── Transform phase ───────────────────────────────────
        t0 = _time.perf_counter()
        enriched = batch_df.transform(apply_all_transforms)
        # Force materialisation so transform time is real
        row_count = enriched.count()
        t_transform = (_time.perf_counter() - t0) * 1000  # ms

        # ── Delta write phase ─────────────────────────────────
        t1 = _time.perf_counter()
        (
            enriched.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("txn_year", "txn_month", "currency")
            .save(delta_path)
        )
        t_write = (_time.perf_counter() - t1) * 1000  # ms
        t_batch = t_transform + t_write

        logger.info(
            f"Batch {batch_id}: {row_count} rows | "
            f"transform {t_transform:.0f} ms | "
            f"delta_write {t_write:.0f} ms | "
            f"total {t_batch:.0f} ms"
        )

        if _HAS_METRICS:
            try:
                update_spark_metrics(
                    status="running",
                    batch_duration_ms=t_batch,
                    transform_time_ms=t_transform,
                    delta_write_time_ms=t_write,
                    rows_processed=row_count,
                )
            except Exception as exc:
                logger.warning(f"Metrics update failed: {exc}")

    query = (
        parsed_df.writeStream
        .foreachBatch(_process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=spark_cfg["trigger_interval"])
        .start()
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
