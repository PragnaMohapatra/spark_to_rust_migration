"""
Spark Batch job: re-process Kafka topic into Delta Lake.

Useful for back-filling or reprocessing the full topic as a one-shot batch.

Run locally:
    python spark_jobs/batch_job.py --local
"""

import argparse
import logging

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, when, year, month
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("batch_job")

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
    builder = (
        SparkSession.builder
        .appName(cfg["spark"]["app_name"] + "_Batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
    )
    if local:
        builder = builder.master("local[*]")
    else:
        builder = builder.master(cfg["spark"]["master"])
    return builder.getOrCreate()


def run_batch_job(cfg: dict, local: bool):
    spark = create_spark_session(cfg, local)
    spark.sparkContext.setLogLevel("WARN")

    kafka_cfg = cfg["kafka"]
    spark_cfg = cfg["spark"]

    bootstrap = (
        kafka_cfg["bootstrap_servers"] if local
        else kafka_cfg["internal_bootstrap_servers"]
    )
    delta_path = (
        spark_cfg["local_delta_output"] if local
        else spark_cfg["delta_output_path"]
    )

    logger.info(f"Reading full Kafka topic '{kafka_cfg['topic']}' in batch mode")

    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", kafka_cfg["topic"])
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
        )
        .select("kafka_key", "data.*")
    )

    enriched_df = (
        parsed_df
        .withColumn("transaction_date", to_date(col("timestamp")))
        .withColumn("txn_year", year(col("timestamp")))
        .withColumn("txn_month", month(col("timestamp")))
        .withColumn(
            "amount_bucket",
            when(col("amount") < 100, "MICRO")
            .when(col("amount") < 10000, "SMALL")
            .when(col("amount") < 100000, "MEDIUM")
            .otherwise("LARGE"),
        )
    )

    record_count = enriched_df.count()
    logger.info(f"Batch contains {record_count:,} records")

    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("txn_year", "txn_month", "currency")
        .save(delta_path)
    )

    logger.info(f"Batch write complete → {delta_path}")


def main():
    parser = argparse.ArgumentParser(description="Kafka → Delta Lake batch job")
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_batch_job(cfg, args.local)


if __name__ == "__main__":
    main()
