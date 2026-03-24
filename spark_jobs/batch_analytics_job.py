"""
Spark Batch Analytics job: read Delta table → run advanced analytics.

This job reads the enriched Delta table (produced by streaming_job.py)
and demonstrates advanced Spark operations you'll encounter in
production pipelines:

──────────────────────────────────────────────────────────────────────
SPARK CONCEPTS EXERCISED
──────────────────────────────────────────────────────────────────────
  1. Window functions     — row_number, rank, dense_rank, lag, lead,
                            running totals, moving averages
  2. Aggregations         — groupBy + agg with multiple metrics
  3. Pivot tables         — long → wide reshaping
  4. Joins                — self-joins, broadcast joins
  5. Sorting & TopN       — orderBy, limit, row_number partitioned
  6. Explode & Arrays     — collect_list, array, explode
  7. Caching              — .cache() / .unpersist() for reuse
  8. SQL interface        — spark.sql() on temp views
  9. Repartition          — control parallelism and file output
 10. Write modes          — overwrite, append, partitionBy
──────────────────────────────────────────────────────────────────────

Run locally:
    python spark_jobs/batch_analytics_job.py --local
"""

import argparse
import logging

import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("batch_analytics")


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(cfg: dict, local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(cfg["spark"]["app_name"] + "_Analytics")
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


# ─────────────────────────────────────────────────────────────
# 1. WINDOW FUNCTIONS — ordered computations within partitions
# ─────────────────────────────────────────────────────────────

def window_analytics(df):
    """Demonstrate Spark window functions.

    Window functions compute a value for each row using a "window"
    of related rows (defined by partitionBy + orderBy), without
    collapsing the row count like groupBy does.

    Concepts:
      - Window.partitionBy().orderBy() — define the window frame
      - row_number()   — sequential position in partition
      - rank()         — same value → same rank, gaps after ties
      - dense_rank()   — same as rank but no gaps
      - lag() / lead() — access previous/next row's value
      - sum().over()   — running total within the window
      - avg().over()   — moving average (with rowsBetween)
    """
    logger.info("── Window Functions ──────────────────────────")

    # Window: partition by currency, order by amount descending
    currency_window = (
        Window
        .partitionBy("currency")
        .orderBy(F.col("amount").desc())
    )

    # Window for running totals: partition by sender, order by timestamp
    sender_window = (
        Window
        .partitionBy("sender_account")
        .orderBy("timestamp")
    )

    # Window for moving average: 5-row sliding window
    moving_avg_window = (
        Window
        .partitionBy("currency")
        .orderBy("timestamp")
        .rowsBetween(-2, 2)  # current row ± 2 = 5-row window
    )

    windowed = (
        df
        # Rank transactions by amount within each currency
        .withColumn("rank_by_amount", F.row_number().over(currency_window))
        .withColumn("rank_with_ties", F.rank().over(currency_window))
        .withColumn("dense_rank_amount", F.dense_rank().over(currency_window))

        # Previous and next transaction amount for the same sender
        .withColumn("prev_amount", F.lag("amount", 1).over(sender_window))
        .withColumn("next_amount", F.lead("amount", 1).over(sender_window))

        # Change from previous transaction
        .withColumn(
            "amount_delta",
            F.col("amount") - F.coalesce(F.lag("amount", 1).over(sender_window), F.col("amount")),
        )

        # Running total of amount per sender
        .withColumn(
            "running_total",
            F.sum("amount").over(
                Window.partitionBy("sender_account").orderBy("timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )

        # 5-row moving average of amount per currency
        .withColumn("moving_avg_amount", F.round(F.avg("amount").over(moving_avg_window), 2))

        # Percentile rank within currency
        .withColumn(
            "pct_rank",
            F.round(F.percent_rank().over(currency_window), 4),
        )
    )

    # Show top-3 transactions per currency
    top3 = windowed.filter(F.col("rank_by_amount") <= 3)
    logger.info(f"Top-3 per currency: {top3.count()} rows")
    top3.select(
        "currency", "rank_by_amount", "amount", "transaction_type",
        "moving_avg_amount", "pct_rank",
    ).show(30, truncate=False)

    return windowed


# ─────────────────────────────────────────────────────────────
# 2. AGGREGATIONS — groupBy + multi-metric summaries
# ─────────────────────────────────────────────────────────────

def aggregation_analytics(df):
    """Demonstrate groupBy aggregations.

    Concepts:
      - groupBy(col)            — partition rows into groups
      - .agg(F.sum(), F.avg())  — compute multiple metrics at once
      - F.countDistinct()       — count unique values
      - F.approx_count_distinct — probabilistic unique count (faster)
      - F.percentile_approx     — approximate percentiles
      - F.collect_set / list    — gather values into an array
      - F.first / F.last        — first/last in group
    """
    logger.info("── Aggregations ─────────────────────────────")

    # Multi-metric summary by currency
    currency_stats = (
        df.groupBy("currency")
        .agg(
            F.count("*").alias("txn_count"),
            F.countDistinct("sender_account").alias("unique_senders"),
            F.approx_count_distinct("receiver_account", rsd=0.05).alias("approx_unique_receivers"),
            F.sum("amount").alias("total_volume"),
            F.avg("amount").alias("avg_amount"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.stddev("amount").alias("stddev_amount"),
            F.avg("fee").alias("avg_fee"),
            F.avg("risk_score").alias("avg_risk"),
            F.sum(F.when(F.col("is_flagged"), 1).otherwise(0)).alias("flagged_count"),
            F.percentile_approx("amount", 0.5).alias("median_amount"),
            F.percentile_approx("amount", 0.95).alias("p95_amount"),
            F.percentile_approx("amount", 0.99).alias("p99_amount"),
        )
        .orderBy(F.col("total_volume").desc())
    )

    logger.info("Currency statistics:")
    currency_stats.show(truncate=False)

    # Multi-dimensional groupBy: currency × transaction_type
    cross_stats = (
        df.groupBy("currency", "transaction_type")
        .agg(
            F.count("*").alias("count"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("currency", F.col("total_amount").desc())
    )

    logger.info("Currency × Transaction Type:")
    cross_stats.show(50, truncate=False)

    # Hourly volume (requires txn_hour from transforms)
    if "txn_hour" in df.columns:
        hourly = (
            df.groupBy("txn_hour")
            .agg(
                F.count("*").alias("txn_count"),
                F.round(F.sum("amount"), 2).alias("volume"),
            )
            .orderBy("txn_hour")
        )
        logger.info("Hourly volume:")
        hourly.show(24, truncate=False)

    return currency_stats


# ─────────────────────────────────────────────────────────────
# 3. PIVOT TABLES — reshape long → wide
# ─────────────────────────────────────────────────────────────

def pivot_analytics(df):
    """Demonstrate pivot tables.

    Concepts:
      - .pivot(col) — turn distinct values of a column into columns
      - Use after groupBy to create cross-tabulations
      - Specify values= to limit pivot columns (performance)
    """
    logger.info("── Pivot Tables ─────────────────────────────")

    # Pivot: rows=currency, columns=transaction_type, values=count
    pivot_count = (
        df.groupBy("currency")
        .pivot("transaction_type", ["WIRE", "ACH", "CARD", "P2P", "CHECK"])
        .agg(F.count("*"))
        .na.fill(0)
        .orderBy("currency")
    )

    logger.info("Transaction count pivot (currency × type):")
    pivot_count.show(truncate=False)

    # Pivot: rows=currency, columns=status, values=sum(amount)
    pivot_volume = (
        df.groupBy("currency")
        .pivot("status", ["COMPLETED", "PENDING", "FAILED", "REVERSED"])
        .agg(F.round(F.sum("amount"), 2))
        .na.fill(0.0)
        .orderBy("currency")
    )

    logger.info("Volume pivot (currency × status):")
    pivot_volume.show(truncate=False)

    # Pivot: rows=channel, columns=category, values=avg(amount)
    pivot_channel = (
        df.groupBy("channel")
        .pivot("category", ["RETAIL", "CORPORATE", "GOVERNMENT", "INTERBANK"])
        .agg(F.round(F.avg("amount"), 2))
        .na.fill(0.0)
        .orderBy("channel")
    )

    logger.info("Avg amount pivot (channel × category):")
    pivot_channel.show(truncate=False)

    return pivot_count


# ─────────────────────────────────────────────────────────────
# 4. SELF-JOINS — correlate rows within the same DataFrame
# ─────────────────────────────────────────────────────────────

def join_analytics(df):
    """Demonstrate joins including self-join and broadcast join.

    Concepts:
      - df.alias("a").join(df.alias("b"), ...) — self-join
      - F.broadcast(small_df)    — hint Spark to broadcast a small DF
      - join types: inner, left, right, full, semi, anti
    """
    logger.info("── Join Analytics ───────────────────────────")

    # Build a "high risk senders" lookup table
    high_risk_senders = (
        df.filter(F.col("risk_score") >= 0.7)
        .groupBy("sender_account")
        .agg(
            F.count("*").alias("high_risk_txn_count"),
            F.round(F.avg("risk_score"), 4).alias("avg_risk_score"),
        )
        .filter(F.col("high_risk_txn_count") >= 2)
    )

    logger.info(f"High-risk senders (≥2 risky txns): {high_risk_senders.count()}")

    # Broadcast join: tag all transactions from high-risk senders
    # broadcast() tells Spark to replicate the small DF to all executors
    # instead of shuffling the large DF — much faster for small lookups
    tagged = (
        df.join(
            F.broadcast(high_risk_senders),
            on="sender_account",
            how="left",
        )
        .withColumn(
            "sender_is_repeat_offender",
            F.col("high_risk_txn_count").isNotNull(),
        )
    )

    offender_count = tagged.filter(F.col("sender_is_repeat_offender")).count()
    logger.info(f"Transactions from repeat high-risk senders: {offender_count}")

    # Semi-join: keep only rows where sender also appears as receiver somewhere
    # (detects circular / round-trip money patterns)
    receivers = df.select(F.col("receiver_account").alias("account")).distinct()
    circular = df.join(
        receivers,
        df["sender_account"] == receivers["account"],
        how="left_semi",
    )
    logger.info(f"Circular-pattern transactions (semi-join): {circular.count()}")

    # Anti-join: senders who NEVER appear as receivers (one-way senders)
    one_way = df.join(
        receivers,
        df["sender_account"] == receivers["account"],
        how="left_anti",
    )
    logger.info(f"One-way sender transactions (anti-join): {one_way.count()}")

    return tagged


# ─────────────────────────────────────────────────────────────
# 5. COLLECT & EXPLODE — arrays inside DataFrames
# ─────────────────────────────────────────────────────────────

def array_analytics(df):
    """Demonstrate collect_list / explode pattern.

    Concepts:
      - F.collect_list(col)  — gather column values into an array per group
      - F.collect_set(col)   — same but deduplicated
      - F.size(array_col)    — length of array
      - F.explode(array_col) — one row per array element (inverse of collect)
      - F.array_contains     — check if value is in array
      - F.array_distinct     — deduplicate an array column
    """
    logger.info("── Array / Explode ──────────────────────────")

    # Collect all currencies used by each sender
    sender_currencies = (
        df.groupBy("sender_account")
        .agg(
            F.collect_set("currency").alias("currencies_used"),
            F.collect_list("transaction_type").alias("txn_types_list"),
            F.count("*").alias("txn_count"),
        )
        .withColumn("num_currencies", F.size("currencies_used"))
        .withColumn("num_txn_types", F.size(F.array_distinct("txn_types_list")))
        .filter(F.col("num_currencies") > 1)  # multi-currency senders only
        .orderBy(F.col("num_currencies").desc())
    )

    logger.info(f"Multi-currency senders: {sender_currencies.count()}")
    sender_currencies.select(
        "sender_account", "currencies_used", "num_currencies", "txn_count",
    ).show(20, truncate=False)

    # Explode: turn the array back into rows (one per currency per sender)
    exploded = (
        sender_currencies
        .select("sender_account", F.explode("currencies_used").alias("currency"))
    )
    logger.info(f"Exploded rows: {exploded.count()}")
    exploded.show(20, truncate=False)

    return sender_currencies


# ─────────────────────────────────────────────────────────────
# 6. SQL INTERFACE — register temp view and query with SQL
# ─────────────────────────────────────────────────────────────

def sql_analytics(spark, df):
    """Demonstrate Spark SQL interface.

    Concepts:
      - createOrReplaceTempView — register DF as a SQL table
      - spark.sql("SELECT ...") — run SQL and get a DataFrame back
      - Subqueries, CTEs, HAVING, CASE WHEN all work
    """
    logger.info("── Spark SQL Interface ──────────────────────")

    df.createOrReplaceTempView("transactions")

    # Top corridors by volume (using SQL syntax)
    corridor_sql = spark.sql("""
        SELECT
            sender_country,
            receiver_country,
            COUNT(*)                    AS txn_count,
            ROUND(SUM(amount), 2)       AS total_volume,
            ROUND(AVG(amount), 2)       AS avg_amount,
            ROUND(AVG(risk_score), 4)   AS avg_risk
        FROM transactions
        WHERE sender_country != receiver_country
        GROUP BY sender_country, receiver_country
        HAVING COUNT(*) >= 5
        ORDER BY total_volume DESC
        LIMIT 20
    """)

    logger.info("Top cross-border corridors (SQL):")
    corridor_sql.show(truncate=False)

    # CTE example: percentage of flagged transactions by channel
    flagged_sql = spark.sql("""
        WITH channel_totals AS (
            SELECT
                channel,
                COUNT(*) AS total,
                SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged
            FROM transactions
            GROUP BY channel
        )
        SELECT
            channel,
            total,
            flagged,
            ROUND(flagged * 100.0 / total, 2) AS flagged_pct
        FROM channel_totals
        ORDER BY flagged_pct DESC
    """)

    logger.info("Flagged % by channel (SQL with CTE):")
    flagged_sql.show(truncate=False)

    return corridor_sql


# ─────────────────────────────────────────────────────────────
# 7. OUTPUT — write analytics results
# ─────────────────────────────────────────────────────────────

def write_results(df, output_path: str, name: str, partition_cols=None):
    """Write a DataFrame to Delta or Parquet.

    Concepts:
      - .repartition(n) — control output file count
      - .coalesce(1)    — merge to single file (small results)
      - write.mode("overwrite") — replace existing data
    """
    path = f"{output_path}/{name}"
    writer = df.coalesce(1).write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.format("parquet").save(path)
    logger.info(f"Wrote {name} → {path}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run_analytics(cfg: dict, local: bool):
    spark = create_spark_session(cfg, local)
    spark.sparkContext.setLogLevel("WARN")

    delta_path = (
        cfg["spark"]["local_delta_output"] if local
        else cfg["spark"]["delta_output_path"]
    )
    output_path = delta_path.rstrip("/") + "_analytics"

    logger.info(f"Reading Delta table from: {delta_path}")
    df = spark.read.format("delta").load(delta_path)
    row_count = df.count()
    logger.info(f"Loaded {row_count:,} rows, {len(df.columns)} columns")

    # Cache the base DataFrame — it's reused by every analytics function.
    # .cache() tells Spark to keep it in memory after the first action.
    df.cache()
    logger.info("DataFrame cached in memory")

    # Run each analytics module
    windowed   = window_analytics(df)
    agg_stats  = aggregation_analytics(df)
    pivot_data = pivot_analytics(df)
    tagged     = join_analytics(df)
    arrays     = array_analytics(df)
    corridors  = sql_analytics(spark, df)

    # Write select results to parquet
    write_results(agg_stats, output_path, "currency_stats")
    write_results(pivot_data, output_path, "pivot_type_by_currency")
    write_results(corridors, output_path, "top_corridors")

    # Release cached DF
    df.unpersist()
    logger.info("Analytics complete.")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Batch analytics on Delta table")
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_analytics(cfg, args.local)


if __name__ == "__main__":
    main()
