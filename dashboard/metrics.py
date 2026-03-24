"""
Shared metrics store for pipeline instrumentation.

Provides a thread-safe, DuckDB-backed metrics collector.  The data
generator writes metrics here; the Streamlit dashboard reads them for
real-time progress and performance monitoring.

Tables:
  - generator_state:   scalar counters (status, bytes_sent, throughput, …)
  - generator_timings: rolling time-series (batch_time_ms, publish_latency_ms)
  - spark_state:       scalar Spark counters
  - spark_timings:     rolling Spark time-series
  - delta_tables:      per-table statistics (version, row_count, size_bytes)
  - pipeline_state:    end-to-end latency

The public API is unchanged from the previous JSON-backed version so
callers (dashboard/app.py, data_generator/generator.py) require no changes.
"""

import json
import threading
import time
from pathlib import Path
from typing import Optional

import duckdb
import math

METRICS_DIR = Path(__file__).resolve().parent.parent / "logs"
DB_PATH = METRICS_DIR / "pipeline_metrics.duckdb"
SPARK_JSON_PATH = METRICS_DIR / "spark_metrics.json"

_lock = threading.Lock()
_local = threading.local()

# Maximum rolling time-series rows kept per category
_MAX_TIMINGS = 200


# ── Connection management ─────────────────────────────────────


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Return a per-thread DuckDB connection (thread-safe)."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(DB_PATH))
        _local.conn = conn
        _init_tables(conn)
    return conn


def _init_tables(conn: duckdb.DuckDBPyConnection):
    """Create tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS generator_state (
            id              INTEGER DEFAULT 1 PRIMARY KEY,
            status          VARCHAR DEFAULT 'idle',
            start_time      DOUBLE,
            target_bytes    BIGINT  DEFAULT 0,
            bytes_sent      BIGINT  DEFAULT 0,
            records_sent    BIGINT  DEFAULT 0,
            batches_completed INTEGER DEFAULT 0,
            elapsed_seconds DOUBLE  DEFAULT 0.0,
            throughput_mb_per_sec DOUBLE DEFAULT 0.0,
            avg_batch_time_ms     DOUBLE DEFAULT 0.0,
            avg_publish_latency_ms DOUBLE DEFAULT 0.0,
            errors          INTEGER DEFAULT 0,
            last_update     DOUBLE
        )
    """)
    conn.execute("""
        INSERT OR IGNORE INTO generator_state (id) VALUES (1)
    """)

    conn.execute("CREATE SEQUENCE IF NOT EXISTS gen_timing_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS generator_timings (
            id        INTEGER PRIMARY KEY DEFAULT(nextval('gen_timing_seq')),
            category  VARCHAR NOT NULL,
            value_ms  DOUBLE  NOT NULL,
            ts        DOUBLE  NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS spark_state (
            id              INTEGER DEFAULT 1 PRIMARY KEY,
            status          VARCHAR DEFAULT 'idle',
            micro_batches_completed INTEGER DEFAULT 0,
            total_rows_processed    BIGINT  DEFAULT 0,
            avg_batch_duration_ms   DOUBLE  DEFAULT 0.0,
            avg_transform_time_ms   DOUBLE  DEFAULT 0.0,
            avg_delta_write_time_ms DOUBLE  DEFAULT 0.0,
            last_batch_rows INTEGER DEFAULT 0,
            first_update    DOUBLE,
            last_update     DOUBLE
        )
    """)
    conn.execute("INSERT OR IGNORE INTO spark_state (id) VALUES (1)")

    conn.execute("CREATE SEQUENCE IF NOT EXISTS spark_timing_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spark_timings (
            id        INTEGER PRIMARY KEY DEFAULT(nextval('spark_timing_seq')),
            category  VARCHAR NOT NULL,
            value_ms  DOUBLE  NOT NULL,
            ts        DOUBLE  NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS delta_tables (
            table_name VARCHAR PRIMARY KEY,
            version    INTEGER DEFAULT 0,
            num_files  INTEGER DEFAULT 0,
            row_count  BIGINT  DEFAULT 0,
            size_bytes BIGINT  DEFAULT 0,
            last_update DOUBLE
        )
    """)
    # Seed the two known tables
    conn.execute("""
        INSERT OR IGNORE INTO delta_tables (table_name) VALUES ('transactions')
    """)
    conn.execute("""
        INSERT OR IGNORE INTO delta_tables (table_name) VALUES ('accounts')
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            id                    INTEGER DEFAULT 1 PRIMARY KEY,
            end_to_end_latency_ms DOUBLE DEFAULT 0.0,
            last_update           DOUBLE
        )
    """)
    conn.execute("INSERT OR IGNORE INTO pipeline_state (id) VALUES (1)")


def _trim_timings(conn: duckdb.DuckDBPyConnection, table: str, category: str):
    """Keep only the most recent _MAX_TIMINGS rows per category."""
    conn.execute(f"""
        DELETE FROM {table}
        WHERE category = ? AND id NOT IN (
            SELECT id FROM {table}
            WHERE category = ?
            ORDER BY id DESC
            LIMIT ?
        )
    """, [category, category, _MAX_TIMINGS])


def _timings_list(conn: duckdb.DuckDBPyConnection, table: str, category: str) -> list:
    """Return rolling timing values as a plain list."""
    rows = conn.execute(f"""
        SELECT value_ms FROM {table}
        WHERE category = ?
        ORDER BY id DESC
        LIMIT ?
    """, [category, _MAX_TIMINGS]).fetchall()
    return [r[0] for r in reversed(rows)]


def _timings_avg(conn: duckdb.DuckDBPyConnection, table: str, category: str) -> float:
    """Return the average of the rolling window for a category."""
    row = conn.execute(f"""
        SELECT AVG(value_ms) FROM (
            SELECT value_ms FROM {table}
            WHERE category = ?
            ORDER BY id DESC
            LIMIT ?
        )
    """, [category, _MAX_TIMINGS]).fetchone()
    return round(row[0], 2) if row[0] is not None else 0.0


# ── Public API (unchanged signatures) ────────────────────────


def reset_metrics():
    """Reset all metrics to defaults."""
    with _lock:
        conn = _get_conn()
        conn.execute("DELETE FROM generator_timings")
        conn.execute("DELETE FROM spark_timings")
        conn.execute("""
            UPDATE generator_state SET
                status='idle', start_time=NULL, target_bytes=0, bytes_sent=0,
                records_sent=0, batches_completed=0, elapsed_seconds=0.0,
                throughput_mb_per_sec=0.0, avg_batch_time_ms=0.0,
                avg_publish_latency_ms=0.0, errors=0, last_update=NULL
            WHERE id=1
        """)
        conn.execute("""
            UPDATE spark_state SET
                status='idle', micro_batches_completed=0, total_rows_processed=0,
                avg_batch_duration_ms=0.0, avg_transform_time_ms=0.0,
                avg_delta_write_time_ms=0.0, last_batch_rows=0,
                first_update=NULL, last_update=NULL
            WHERE id=1
        """)
        conn.execute("""
            UPDATE delta_tables SET version=0, num_files=0, row_count=0,
                size_bytes=0, last_update=NULL
        """)
        conn.execute("""
            UPDATE pipeline_state SET end_to_end_latency_ms=0.0, last_update=NULL
            WHERE id=1
        """)
        # Also delete the Spark JSON sidecar (written by container)
        try:
            SPARK_JSON_PATH.unlink(missing_ok=True)
        except Exception:
            pass


def _nan_to_none(v):
    """Convert numpy NaN (returned by DuckDB for NULL) to Python None."""
    if v is None:
        return None
    try:
        if math.isnan(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _read_spark_json() -> dict:
    """Read Spark metrics from the JSON file written by the container."""
    try:
        if SPARK_JSON_PATH.exists():
            return json.loads(SPARK_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {
        "status": "idle",
        "micro_batches_completed": 0,
        "total_rows_processed": 0,
        "last_batch_rows": 0,
        "first_update": None,
        "last_update": None,
        "batch_durations_ms": [],
        "transform_times_ms": [],
        "delta_write_times_ms": [],
    }


def get_metrics() -> dict:
    """Read the current metrics snapshot (same dict shape as before)."""
    with _lock:
        conn = _get_conn()

        g = conn.execute("SELECT * FROM generator_state WHERE id=1").fetchdf().iloc[0]
        p = conn.execute("SELECT * FROM pipeline_state WHERE id=1").fetchdf().iloc[0]

        # Delta tables
        dt_rows = conn.execute("SELECT * FROM delta_tables").fetchdf()
        delta = {}
        for _, row in dt_rows.iterrows():
            delta[f"{row['table_name']}_table"] = {
                "version": int(row["version"]),
                "num_files": int(row["num_files"]),
                "row_count": int(row["row_count"]),
                "size_bytes": int(row["size_bytes"]),
                "last_update": _nan_to_none(row["last_update"]),
            }

        # ── Spark metrics: read from JSON file (written by container) ──
        spark_data = _read_spark_json()
        bd = spark_data.get("batch_durations_ms", [])
        tt = spark_data.get("transform_times_ms", [])
        dw = spark_data.get("delta_write_times_ms", [])

        return {
            "generator": {
                "status": g["status"],
                "start_time": _nan_to_none(g["start_time"]),
                "target_bytes": int(g["target_bytes"]),
                "bytes_sent": int(g["bytes_sent"]),
                "records_sent": int(g["records_sent"]),
                "batches_completed": int(g["batches_completed"]),
                "elapsed_seconds": float(g["elapsed_seconds"]),
                "throughput_mb_per_sec": float(g["throughput_mb_per_sec"]),
                "avg_batch_time_ms": float(g["avg_batch_time_ms"]),
                "avg_publish_latency_ms": float(g["avg_publish_latency_ms"]),
                "batch_timings_ms": _timings_list(conn, "generator_timings", "batch_time"),
                "publish_latencies_ms": _timings_list(conn, "generator_timings", "publish_latency"),
                "errors": int(g["errors"]),
                "last_update": _nan_to_none(g["last_update"]),
            },
            "spark": {
                "status": spark_data.get("status", "idle"),
                "micro_batches_completed": spark_data.get("micro_batches_completed", 0),
                "total_rows_processed": spark_data.get("total_rows_processed", 0),
                "avg_batch_duration_ms": sum(bd) / len(bd) if bd else 0.0,
                "avg_transform_time_ms": sum(tt) / len(tt) if tt else 0.0,
                "avg_delta_write_time_ms": sum(dw) / len(dw) if dw else 0.0,
                "batch_durations_ms": bd,
                "transform_times_ms": tt,
                "delta_write_times_ms": dw,
                "last_batch_rows": spark_data.get("last_batch_rows", 0),
                "first_update": spark_data.get("first_update"),
                "last_update": spark_data.get("last_update"),
            },
            "delta": delta,
            "pipeline": {
                "end_to_end_latency_ms": float(p["end_to_end_latency_ms"]),
                "last_update": _nan_to_none(p["last_update"]),
            },
        }


def update_generator_metrics(
    *,
    status: Optional[str] = None,
    start_time: Optional[float] = None,
    target_bytes: Optional[int] = None,
    bytes_sent: Optional[int] = None,
    records_sent: Optional[int] = None,
    batches_completed: Optional[int] = None,
    elapsed_seconds: Optional[float] = None,
    throughput_mb_per_sec: Optional[float] = None,
    batch_time_ms: Optional[float] = None,
    publish_latency_ms: Optional[float] = None,
    errors: Optional[int] = None,
):
    """Update generator metrics (called from data_generator threads)."""
    with _lock:
        conn = _get_conn()
        now = time.time()

        # Build SET clause dynamically for scalar fields
        sets, params = [], []
        for col, val in [
            ("status", status),
            ("start_time", start_time),
            ("target_bytes", target_bytes),
            ("bytes_sent", bytes_sent),
            ("records_sent", records_sent),
            ("batches_completed", batches_completed),
            ("elapsed_seconds", elapsed_seconds),
            ("throughput_mb_per_sec", throughput_mb_per_sec),
            ("errors", errors),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                params.append(val)

        # Append rolling timing rows
        if batch_time_ms is not None:
            conn.execute(
                "INSERT INTO generator_timings (category, value_ms, ts) VALUES (?, ?, ?)",
                ["batch_time", round(batch_time_ms, 2), now],
            )
            _trim_timings(conn, "generator_timings", "batch_time")
            avg = _timings_avg(conn, "generator_timings", "batch_time")
            sets.append("avg_batch_time_ms = ?")
            params.append(avg)

        if publish_latency_ms is not None:
            conn.execute(
                "INSERT INTO generator_timings (category, value_ms, ts) VALUES (?, ?, ?)",
                ["publish_latency", round(publish_latency_ms, 2), now],
            )
            _trim_timings(conn, "generator_timings", "publish_latency")
            avg = _timings_avg(conn, "generator_timings", "publish_latency")
            sets.append("avg_publish_latency_ms = ?")
            params.append(avg)

        sets.append("last_update = ?")
        params.append(now)

        conn.execute(
            f"UPDATE generator_state SET {', '.join(sets)} WHERE id = 1",
            params,
        )


def update_spark_metrics(
    *,
    status: Optional[str] = None,
    batch_duration_ms: Optional[float] = None,
    transform_time_ms: Optional[float] = None,
    delta_write_time_ms: Optional[float] = None,
    rows_processed: Optional[int] = None,
):
    """Update Spark streaming metrics."""
    with _lock:
        conn = _get_conn()
        now = time.time()

        sets, params = [], []

        if status is not None:
            sets.append("status = ?")
            params.append(status)

        if batch_duration_ms is not None:
            conn.execute(
                "INSERT INTO spark_timings (category, value_ms, ts) VALUES (?, ?, ?)",
                ["batch_duration", round(batch_duration_ms, 2), now],
            )
            _trim_timings(conn, "spark_timings", "batch_duration")
            avg = _timings_avg(conn, "spark_timings", "batch_duration")
            sets.append("micro_batches_completed = micro_batches_completed + 1")
            sets.append("avg_batch_duration_ms = ?")
            params.append(avg)

        if transform_time_ms is not None:
            conn.execute(
                "INSERT INTO spark_timings (category, value_ms, ts) VALUES (?, ?, ?)",
                ["transform_time", round(transform_time_ms, 2), now],
            )
            _trim_timings(conn, "spark_timings", "transform_time")
            avg = _timings_avg(conn, "spark_timings", "transform_time")
            sets.append("avg_transform_time_ms = ?")
            params.append(avg)

        if delta_write_time_ms is not None:
            conn.execute(
                "INSERT INTO spark_timings (category, value_ms, ts) VALUES (?, ?, ?)",
                ["delta_write_time", round(delta_write_time_ms, 2), now],
            )
            _trim_timings(conn, "spark_timings", "delta_write_time")
            avg = _timings_avg(conn, "spark_timings", "delta_write_time")
            sets.append("avg_delta_write_time_ms = ?")
            params.append(avg)

        if rows_processed is not None:
            sets.append("total_rows_processed = total_rows_processed + ?")
            params.append(rows_processed)
            sets.append("last_batch_rows = ?")
            params.append(rows_processed)

        if sets:
            # Set first_update on the very first metrics call
            sets.append("first_update = COALESCE(first_update, ?)")
            params.append(now)
            sets.append("last_update = ?")
            params.append(now)
            conn.execute(
                f"UPDATE spark_state SET {', '.join(sets)} WHERE id = 1",
                params,
            )


def update_delta_metrics(
    table_name: str,
    *,
    version: Optional[int] = None,
    num_files: Optional[int] = None,
    row_count: Optional[int] = None,
    size_bytes: Optional[int] = None,
):
    """Update Delta table statistics."""
    with _lock:
        conn = _get_conn()
        now = time.time()

        # Ensure row exists
        conn.execute(
            "INSERT OR IGNORE INTO delta_tables (table_name) VALUES (?)",
            [table_name],
        )

        sets, params = [], []
        for col, val in [
            ("version", version),
            ("num_files", num_files),
            ("row_count", row_count),
            ("size_bytes", size_bytes),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                params.append(val)

        if sets:
            sets.append("last_update = ?")
            params.append(now)
            params.append(table_name)
            conn.execute(
                f"UPDATE delta_tables SET {', '.join(sets)} WHERE table_name = ?",
                params,
            )


def update_pipeline_latency(latency_ms: float):
    """Update end-to-end pipeline latency."""
    with _lock:
        conn = _get_conn()
        now = time.time()
        conn.execute(
            "UPDATE pipeline_state SET end_to_end_latency_ms = ?, last_update = ? WHERE id = 1",
            [round(latency_ms, 2), now],
        )
