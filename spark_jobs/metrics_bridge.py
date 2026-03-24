"""
Lightweight JSON-file metrics bridge for Spark jobs running inside Docker.

DuckDB cannot be shared across process boundaries (host Streamlit ↔ container
Spark) due to exclusive file locking.  This module writes a simple JSON file
to the shared ``/opt/logs`` volume.  The Streamlit dashboard polls this file
and merges the data into its own DuckDB instance on the host side.

File: ``logs/spark_metrics.json``   (host)
      ``/opt/logs/spark_metrics.json`` (container)
"""

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_METRICS_DIR = Path(__file__).resolve().parent.parent / "logs"
_METRICS_FILE = _METRICS_DIR / "spark_metrics.json"

# Rolling window size for per-batch timings
_MAX_TIMINGS = 200


def _load() -> dict:
    """Load current metrics from the JSON file, or return defaults."""
    try:
        if _METRICS_FILE.exists():
            return json.loads(_METRICS_FILE.read_text(encoding="utf-8"))
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
        # Per-batch detailed metrics (from Spark REST API)
        "batch_details": [],
        # Cumulative executor totals
        "executor": {},
    }


def _save(data: dict):
    """Atomically write metrics JSON (write-to-temp then rename)."""
    _METRICS_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(_METRICS_DIR), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        # Atomic rename (works on Linux; on Windows it replaces)
        os.replace(tmp, str(_METRICS_FILE))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def update_spark_metrics(
    *,
    status: Optional[str] = None,
    batch_duration_ms: Optional[float] = None,
    transform_time_ms: Optional[float] = None,
    delta_write_time_ms: Optional[float] = None,
    rows_processed: Optional[int] = None,
    batch_detail: Optional[dict] = None,
    executor_metrics: Optional[dict] = None,
):
    """Append a micro-batch measurement to the shared JSON file."""
    data = _load()
    now = time.time()

    if status is not None:
        data["status"] = status

    if data["first_update"] is None:
        data["first_update"] = now
    data["last_update"] = now

    if batch_duration_ms is not None:
        data["batch_durations_ms"].append(round(batch_duration_ms, 2))
        data["batch_durations_ms"] = data["batch_durations_ms"][-_MAX_TIMINGS:]
        data["micro_batches_completed"] = data.get("micro_batches_completed", 0) + 1

    if transform_time_ms is not None:
        data["transform_times_ms"].append(round(transform_time_ms, 2))
        data["transform_times_ms"] = data["transform_times_ms"][-_MAX_TIMINGS:]

    if delta_write_time_ms is not None:
        data["delta_write_times_ms"].append(round(delta_write_time_ms, 2))
        data["delta_write_times_ms"] = data["delta_write_times_ms"][-_MAX_TIMINGS:]

    if rows_processed is not None:
        data["total_rows_processed"] = data.get("total_rows_processed", 0) + rows_processed
        data["last_batch_rows"] = rows_processed

    # Per-batch detailed metrics (CPU, GC, shuffle, I/O, memory)
    if batch_detail is not None:
        details = data.get("batch_details", [])
        details.append(batch_detail)
        data["batch_details"] = details[-_MAX_TIMINGS:]

    # Cumulative executor metrics
    if executor_metrics is not None:
        data["executor"] = executor_metrics

    _save(data)
    logger.debug("Spark metrics written: batches=%d rows=%d",
                 data["micro_batches_completed"], data["total_rows_processed"])
