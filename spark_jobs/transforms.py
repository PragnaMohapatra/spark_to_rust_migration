"""
Reusable Spark transformation library for financial transaction data.

Demonstrates key Spark concepts:
  1. Column expressions (withColumn, select, expr)
  2. UDFs — User-Defined Functions (Python callables → Spark columns)
  3. When/otherwise — conditional logic
  4. Regex extraction — parsing structured strings
  5. Math & rounding — derived numeric columns
  6. Null handling — coalesce, na.fill
  7. String manipulation — concat, upper, substring
  8. Timestamp arithmetic — datediff, hour extraction
  9. Hashing — SHA-256 for anonymization
  10. Monotonically increasing ID — row numbering
"""

import math
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


# ─────────────────────────────────────────────────────────────
# 1. UDFs  —  Python functions registered as Spark column ops
# ─────────────────────────────────────────────────────────────

@F.udf(returnType=StringType())
def classify_risk(risk_score: Optional[float]) -> str:
    """Classify risk_score into named tiers.

    Spark concept: UDF — wraps arbitrary Python logic so it can run
    row-by-row inside Spark executors.  Useful when built-in functions
    can't express the logic, but slower than native expressions.
    """
    if risk_score is None:
        return "UNKNOWN"
    if risk_score >= 0.8:
        return "CRITICAL"
    if risk_score >= 0.6:
        return "HIGH"
    if risk_score >= 0.4:
        return "MEDIUM"
    if risk_score >= 0.2:
        return "LOW"
    return "NEGLIGIBLE"


@F.udf(returnType=StringType())
def mask_account(account: Optional[str]) -> str:
    """Mask all but the last 4 digits of an account number.

    Spark concept: UDF for PII masking — shows how to process
    string columns with arbitrary Python string ops.
    """
    if account is None or len(account) < 4:
        return "****"
    return "*" * (len(account) - 4) + account[-4:]


@F.udf(returnType=DoubleType())
def compute_net_amount(amount: Optional[float], fee: Optional[float],
                       exchange_rate: Optional[float]) -> Optional[float]:
    """Net amount after fee, converted at exchange rate.

    Spark concept: multi-column UDF — takes several columns as
    input and returns a derived value.
    """
    if amount is None:
        return None
    f = fee if fee is not None else 0.0
    rate = exchange_rate if exchange_rate is not None else 1.0
    return round((amount - f) * rate, 2)


@F.udf(returnType=IntegerType())
def ip_to_int(ip: Optional[str]) -> Optional[int]:
    """Convert dotted IPv4 string to a 32-bit integer.

    Spark concept: UDF for type conversion — useful for range joins
    or geo-IP lookups against integer IP ranges.
    """
    if ip is None:
        return None
    try:
        parts = ip.split(".")
        return (int(parts[0]) << 24) + (int(parts[1]) << 16) + \
               (int(parts[2]) << 8) + int(parts[3])
    except (IndexError, ValueError):
        return None


@F.udf(returnType=DoubleType())
def log_amount(amount: Optional[float]) -> Optional[float]:
    """Natural log of amount (for distribution normalization).

    Spark concept: math UDF — when built-in F.log() isn't enough
    (e.g. you need custom handling of zero/negative).
    """
    if amount is None or amount <= 0:
        return None
    return round(math.log(amount), 4)


# ─────────────────────────────────────────────────────────────
# 2. Transform functions  —  DataFrame → DataFrame
# ─────────────────────────────────────────────────────────────

def add_time_dimensions(df: DataFrame) -> DataFrame:
    """Extract date parts from the transaction timestamp.

    Spark concepts demonstrated:
      - to_date / to_timestamp — string → date/timestamp casting
      - year, month, dayofweek, hour — temporal extraction
      - date_format — custom date string formatting
      - when/otherwise — conditional column (is_weekend)
    """
    return (
        df
        .withColumn("transaction_date", F.to_date(F.col("timestamp")))
        .withColumn("txn_year", F.year("timestamp"))
        .withColumn("txn_month", F.month("timestamp"))
        .withColumn("txn_day", F.dayofmonth("timestamp"))
        .withColumn("txn_hour", F.hour("timestamp"))
        .withColumn("txn_dayofweek", F.dayofweek("timestamp"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("timestamp").isin(1, 7), True).otherwise(False),
        )
        .withColumn("txn_quarter", F.quarter("timestamp"))
        .withColumn("date_str", F.date_format("timestamp", "yyyy-MM-dd"))
    )


def add_amount_features(df: DataFrame) -> DataFrame:
    """Derive amount-based features using built-in math + when/otherwise.

    Spark concepts demonstrated:
      - when/otherwise — multi-branch conditional (amount_bucket)
      - Arithmetic expressions — col * col, col - col
      - F.round — decimal precision control
      - F.log — built-in log (compare to UDF version)
      - F.abs — absolute value
      - F.greatest / F.least — row-level min/max across columns
    """
    return (
        df
        # Bucket by amount range
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount") < 100, "MICRO")
            .when(F.col("amount") < 1_000, "SMALL")
            .when(F.col("amount") < 10_000, "MEDIUM")
            .when(F.col("amount") < 100_000, "LARGE")
            .otherwise("WHALE"),
        )
        # Net amount after fee
        .withColumn(
            "net_amount",
            F.round(F.col("amount") - F.coalesce(F.col("fee"), F.lit(0.0)), 2),
        )
        # Fee as percentage of amount
        .withColumn(
            "fee_pct",
            F.when(F.col("amount") > 0,
                   F.round(F.col("fee") / F.col("amount") * 100, 4))
            .otherwise(F.lit(None)),
        )
        # Amount in USD equivalent (amount * exchange_rate)
        .withColumn(
            "amount_usd",
            F.round(
                F.col("amount") * F.coalesce(F.col("exchange_rate"), F.lit(1.0)), 2
            ),
        )
        # Log-scaled amount for distribution analysis
        .withColumn(
            "log_amount",
            F.when(F.col("amount") > 0, F.round(F.log(F.col("amount")), 4))
            .otherwise(F.lit(None)),
        )
    )


def add_risk_features(df: DataFrame) -> DataFrame:
    """Compute risk-related derived columns.

    Spark concepts demonstrated:
      - UDF invocation — classify_risk(col) used as a column expression
      - F.when chained — multi-level conditions
      - Boolean column creation — direct comparison → bool
      - F.coalesce — null-safe fallback
    """
    return (
        df
        # Risk tier via UDF (see classify_risk above)
        .withColumn("risk_tier", classify_risk(F.col("risk_score")))
        # Numeric risk level for ordering
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 0.8, 5)
            .when(F.col("risk_score") >= 0.6, 4)
            .when(F.col("risk_score") >= 0.4, 3)
            .when(F.col("risk_score") >= 0.2, 2)
            .when(F.col("risk_score") >= 0.0, 1)
            .otherwise(0),
        )
        # Composite flag: high risk OR already flagged OR large amount
        .withColumn(
            "needs_review",
            (
                (F.col("risk_score") >= 0.7)
                | (F.col("is_flagged") == True)
                | (F.col("amount") >= 100_000)
            ),
        )
    )


def add_geo_features(df: DataFrame) -> DataFrame:
    """Derive geography-based columns.

    Spark concepts demonstrated:
      - String comparison — col == col (cross-border detection)
      - F.concat_ws — join multiple columns with separator
      - F.upper, F.lower, F.trim — string transformations
      - F.when with compound conditions — AND / OR logic
    """
    return (
        df
        # Is this a cross-border transaction?
        .withColumn(
            "is_cross_border",
            F.col("sender_country") != F.col("receiver_country"),
        )
        # Corridor string: "US→GB"
        .withColumn(
            "corridor",
            F.concat_ws(
                "→",
                F.upper(F.col("sender_country")),
                F.upper(F.col("receiver_country")),
            ),
        )
        # Same-bank transfer detection
        .withColumn(
            "is_intra_bank",
            F.col("sender_bank_code") == F.col("receiver_bank_code"),
        )
    )


def add_anonymized_columns(df: DataFrame) -> DataFrame:
    """Hash/mask PII columns for analytics-safe output.

    Spark concepts demonstrated:
      - F.sha2 — built-in SHA-256 hashing (no UDF needed)
      - UDF for masking — mask_account applied per-row
      - F.regexp_replace — regex-based string transformation
      - Column aliasing with .alias()
    """
    return (
        df
        # Hash sender/receiver names → non-reversible pseudonyms
        .withColumn("sender_hash", F.sha2(F.col("sender_name"), 256))
        .withColumn("receiver_hash", F.sha2(F.col("receiver_name"), 256))
        # Mask account numbers (UDF)
        .withColumn("sender_account_masked", mask_account(F.col("sender_account")))
        .withColumn("receiver_account_masked", mask_account(F.col("receiver_account")))
        # Anonymize IP: replace last octet with "xxx"
        .withColumn(
            "ip_anonymized",
            F.regexp_replace(F.col("ip_address"), r"\.\d+$", ".xxx"),
        )
    )


def add_session_features(df: DataFrame) -> DataFrame:
    """Session and channel analysis columns.

    Spark concepts demonstrated:
      - F.length — string length
      - F.substring — extract part of a string
      - F.monotonically_increasing_id — unique row ID (not sequential!)
      - F.lit — add a constant column
    """
    return (
        df
        # First 8 chars of device fingerprint as short ID
        .withColumn("device_short_id", F.substring(F.col("device_fingerprint"), 1, 8))
        # Session prefix (first segment of UUID)
        .withColumn(
            "session_prefix",
            F.substring(F.col("session_id"), 1, 8),
        )
        # Memo length — useful for text analysis
        .withColumn("memo_length", F.length(F.col("memo")))
        # Has memo flag
        .withColumn(
            "has_memo",
            (F.col("memo").isNotNull()) & (F.length(F.col("memo")) > 0),
        )
        # Assign a unique row ID using sha2 hash of transaction_id
        .withColumn("row_id", F.sha2(F.col("transaction_id"), 256))
    )


def add_null_handling(df: DataFrame) -> DataFrame:
    """Demonstrate null-safe operations.

    Spark concepts demonstrated:
      - F.coalesce — first non-null among columns
      - na.fill — replace nulls with defaults (dict of col→value)
      - F.when(col.isNull(), ...) — explicit null check
      - F.nanvl — replace NaN with a value (for DoubleType)
    """
    return (
        df
        .withColumn(
            "currency_safe",
            F.coalesce(F.col("currency"), F.lit("USD")),
        )
        .withColumn(
            "risk_score_safe",
            F.coalesce(F.col("risk_score"), F.lit(0.0)),
        )
        .withColumn(
            "memo_clean",
            F.when(F.col("memo").isNull(), F.lit("(no memo)"))
            .otherwise(F.trim(F.col("memo"))),
        )
        .na.fill({
            "fee": 0.0,
            "exchange_rate": 1.0,
            "sender_country": "XX",
            "receiver_country": "XX",
        })
    )


# ─────────────────────────────────────────────────────────────
# 3. Pipeline combinator — apply all transforms in order
# ─────────────────────────────────────────────────────────────

def apply_all_transforms(df: DataFrame) -> DataFrame:
    """Chain every enrichment transform.  Order matters where
    downstream columns depend on upstream (e.g. amount_bucket
    must exist before risk features reference amount).
    """
    return (
        df
        .transform(add_null_handling)
        .transform(add_time_dimensions)
        .transform(add_amount_features)
        .transform(add_risk_features)
        .transform(add_geo_features)
        .transform(add_anonymized_columns)
        .transform(add_session_features)
    )
