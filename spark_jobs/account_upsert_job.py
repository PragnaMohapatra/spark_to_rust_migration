"""
Account Upsert Streaming Job: Kafka → Delta Lake MERGE.

Reads account-update events from Kafka and upserts them into a Delta
"accounts" table using foreachBatch + DeltaTable.merge().

──────────────────────────────────────────────────────────────────────
SPARK / DELTA CONCEPTS EXERCISED
──────────────────────────────────────────────────────────────────────
  1. foreachBatch             — apply arbitrary logic per micro-batch
  2. DeltaTable.merge()       — SQL MERGE (upsert) on Delta Lake
  3. whenMatchedUpdate        — update existing rows on key match
  4. whenNotMatchedInsert     — insert new rows when key not found
  5. merge condition          — join predicate on account_id
  6. Accumulative updates     — total_sent += incoming, etc.
  7. Conditional expressions  — greatest(), coalesce() in merge
  8. Schema evolution         — mergeSchema on first write
  9. Partition pruning        — partition by country for fast lookups
 10. Idempotent processing    — checkpoint ensures exactly-once
──────────────────────────────────────────────────────────────────────

Run inside Docker:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \\
        /opt/spark-jobs/account_upsert_job.py

Run locally:
    python spark_jobs/account_upsert_job.py --local
"""

import argparse
import logging
import sys
import time as _time

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("account_upsert")

# ── Metrics integration (graceful if unavailable) ─────────────
try:
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
    from dashboard.metrics import update_spark_metrics
    _HAS_METRICS = True
except Exception:
    _HAS_METRICS = False

# ── Account update event schema ───────────────────────────────

ACCOUNT_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("account_number", StringType(), True),
    StructField("holder_name", StringType(), True),
    StructField("bank_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("total_sent", DoubleType(), True),
    StructField("total_received", DoubleType(), True),
    StructField("transaction_count", IntegerType(), True),
    StructField("risk_rating", DoubleType(), True),
    StructField("kyc_level", StringType(), True),
    StructField("last_transaction_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("created_at", StringType(), True),
])


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(cfg: dict, local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(cfg["spark"]["app_name"] + "_AccountUpsert")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    if local:
        builder = builder.master("local[*]")
    else:
        builder = builder.master(cfg["spark"]["master"])
    return builder.getOrCreate()


def upsert_to_delta(micro_batch_df: DataFrame, batch_id: int,
                    delta_path: str, spark: SparkSession):
    """Upsert a micro-batch of account updates into the accounts Delta table.

    Spark / Delta concepts:
      - DeltaTable.forPath()      — open an existing Delta table
      - .alias("target")/.alias("source") — table aliases for merge
      - .merge(source, condition)  — start a MERGE operation
      - whenMatchedUpdate          — UPDATE SET for existing accounts
      - whenNotMatchedInsertAll    — INSERT * for new accounts

    The merge logic:
      - If account_id already exists in the table (MATCHED):
        - Accumulates total_sent and total_received (running totals)
        - Increments transaction_count
        - Takes the latest balance, risk_rating, kyc_level, status
        - Updates last_transaction_time and updated_at
      - If account_id is new (NOT MATCHED):
        - Inserts the full row as-is
    """
    if micro_batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping")
        return

    row_count = micro_batch_df.count()
    logger.info(f"Batch {batch_id}: upserting {row_count} account updates")

    # Deduplicate within the micro-batch: keep the latest update per account
    # (multiple transactions for the same account can arrive in one batch)
    deduped = (
        micro_batch_df
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("account_id").orderBy(F.col("updated_at").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Aggregate within batch: sum total_sent/received and count per account
    # so we accumulate correctly even with multiple events per account
    aggregated = (
        micro_batch_df
        .groupBy("account_id")
        .agg(
            F.sum("total_sent").alias("batch_total_sent"),
            F.sum("total_received").alias("batch_total_received"),
            F.count("*").alias("batch_txn_count"),
            F.max("updated_at").alias("latest_updated_at"),
            F.max("last_transaction_time").alias("latest_txn_time"),
        )
    )

    # Join aggregated stats back to the deduped row (for non-additive fields)
    source = (
        deduped
        .drop("total_sent", "total_received", "transaction_count",
              "updated_at", "last_transaction_time")
        .join(aggregated, on="account_id", how="inner")
        .withColumnRenamed("batch_total_sent", "total_sent")
        .withColumnRenamed("batch_total_received", "total_received")
        .withColumnRenamed("batch_txn_count", "transaction_count")
        .withColumnRenamed("latest_updated_at", "updated_at")
        .withColumnRenamed("latest_txn_time", "last_transaction_time")
    )

    try:
        # Try to open existing Delta table
        delta_table = DeltaTable.forPath(spark, delta_path)
    except Exception:
        # First batch: table doesn't exist yet — write it directly
        logger.info(f"Batch {batch_id}: creating accounts table at {delta_path}")
        (
            source.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("country")
            .save(delta_path)
        )
        return

    # ── MERGE (upsert) ────────────────────────────────────────
    #
    # This is the core Delta Lake concept: a single atomic operation
    # that can INSERT new rows and UPDATE existing rows based on a
    # match condition (like SQL MERGE INTO).
    (
        delta_table.alias("target")
        .merge(
            source.alias("source"),
            condition="target.account_id = source.account_id",
        )
        # WHEN MATCHED → update accumulative fields + latest snapshot
        .whenMatchedUpdate(set={
            # Accumulate lifetime totals (running sum)
            "total_sent": F.coalesce("target.total_sent", F.lit(0.0))
                          + F.coalesce("source.total_sent", F.lit(0.0)),
            "total_received": F.coalesce("target.total_received", F.lit(0.0))
                              + F.coalesce("source.total_received", F.lit(0.0)),
            "transaction_count": F.coalesce("target.transaction_count", F.lit(0))
                                 + F.coalesce("source.transaction_count", F.lit(0)),

            # Take the latest snapshot values
            "balance": "source.balance",
            "account_status": "source.account_status",
            "risk_rating": "source.risk_rating",
            "kyc_level": "source.kyc_level",

            # Update timestamps — take the most recent
            "last_transaction_time": F.greatest(
                F.coalesce("target.last_transaction_time", F.lit("1970-01-01")),
                F.coalesce("source.last_transaction_time", F.lit("1970-01-01")),
            ),
            "updated_at": "source.updated_at",

            # Immutable fields kept as-is (account_number, holder_name, etc.)
            # are not listed here, so they remain unchanged
        })
        # WHEN NOT MATCHED → insert the new account
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(f"Batch {batch_id}: merge complete")


def run_account_upsert(cfg: dict, local: bool):
    """Main account upsert pipeline: Kafka → foreachBatch → Delta MERGE."""
    spark = create_spark_session(cfg, local)
    spark.sparkContext.setLogLevel("WARN")

    kafka_cfg = cfg["kafka"]
    accounts_cfg = cfg["accounts"]

    bootstrap = (
        kafka_cfg["bootstrap_servers"] if local
        else kafka_cfg["internal_bootstrap_servers"]
    )
    checkpoint = (
        accounts_cfg["local_checkpoint"] if local
        else accounts_cfg["checkpoint_location"]
    )
    delta_path = (
        accounts_cfg["local_delta_output"] if local
        else accounts_cfg["delta_output_path"]
    )

    logger.info(f"Kafka bootstrap:  {bootstrap}")
    logger.info(f"Account topic:    {kafka_cfg['account_topic']}")
    logger.info(f"Accounts Delta:   {delta_path}")
    logger.info(f"Checkpoint:       {checkpoint}")

    # ── Read account update events from Kafka ─────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", kafka_cfg["account_topic"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON → typed columns ────────────────────────────
    parsed_df = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"), ACCOUNT_SCHEMA
            ).alias("data"),
        )
        .select("data.*")
        .filter(F.col("account_id").isNotNull())
    )

    # ── Write with foreachBatch → Delta MERGE (instrumented) ──
    #
    # foreachBatch gives us access to each micro-batch as a static
    # DataFrame, which lets us call DeltaTable.merge() — something
    # not possible with a plain writeStream.format("delta").
    # We wrap upsert_to_delta with timing instrumentation.

    def _instrumented_upsert(df, bid):
        if df.rdd.isEmpty():
            logger.info(f"Batch {bid}: empty, skipping")
            return
        row_count = df.count()
        t0 = _time.perf_counter()
        upsert_to_delta(df, bid, delta_path, spark)
        t_batch = (_time.perf_counter() - t0) * 1000  # ms

        logger.info(
            f"Batch {bid}: {row_count} rows | "
            f"upsert {t_batch:.0f} ms"
        )

        if _HAS_METRICS:
            try:
                update_spark_metrics(
                    status="running",
                    batch_duration_ms=t_batch,
                    delta_write_time_ms=t_batch,
                    rows_processed=row_count,
                )
            except Exception as exc:
                logger.warning(f"Metrics update failed: {exc}")

    query = (
        parsed_df.writeStream
        .foreachBatch(_instrumented_upsert)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=cfg["spark"]["trigger_interval"])
        .start()
    )

    logger.info("Account upsert stream started — waiting for termination...")
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser(
        description="Kafka → Delta Lake account upsert (MERGE) job"
    )
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_account_upsert(cfg, args.local)


if __name__ == "__main__":
    main()
