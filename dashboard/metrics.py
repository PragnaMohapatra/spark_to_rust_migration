"""
Shared metrics store for pipeline instrumentation.

Provides a thread-safe, JSON-file-backed metrics reader.  Each pipeline
container writes its own JSON sidecar to the shared ``logs/`` volume.
The Streamlit dashboard reads those files to build the metrics dict.

Delta table statistics are computed on-demand via the ``deltalake``
Python library (no database required).

The public API is unchanged from the previous DuckDB-backed version so
callers (dashboard/app.py, data_generator/generator.py) require no changes.
"""

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

METRICS_DIR = Path(__file__).resolve().parent.parent / "logs"
DELTA_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "delta_output"
SPARK_JSON_PATH = METRICS_DIR / "spark_metrics.json"
SPARK_API_DUMP_PATH = METRICS_DIR / "spark_api_dump.json"
RUST_JSON_PATH = METRICS_DIR / "rust_metrics.json"
RUST_ACCT_JSON_PATH = METRICS_DIR / "rust_acct_metrics.json"
VALIDATION_JSON_PATH = METRICS_DIR / "validation_report.json"
GENERATOR_JSON_PATH = METRICS_DIR / "generator_metrics.json"

_lock = threading.Lock()


# ── Delta table helpers ───────────────────────────────────────

# Known Delta tables and their paths (relative to DELTA_OUTPUT_DIR)
_DELTA_TABLES = {
    "transactions": "financial_transactions",
    "accounts": "accounts",
}


def _read_delta_table_stats(table_name: str) -> dict:
    """Read Delta table stats on-demand using the deltalake library.

    Uses only metadata operations — never reads the full table into memory.
    Falls back to file-count-only stats if metadata read fails.
    """
    _DEFAULT = {"version": 0, "num_files": 0, "row_count": 0,
                "size_bytes": 0, "last_update": None}
    table_dir = DELTA_OUTPUT_DIR / _DELTA_TABLES.get(table_name, table_name)
    try:
        if not table_dir.exists():
            return _DEFAULT
        # Check for _delta_log directory — required for a valid delta table
        delta_log = table_dir / "_delta_log"
        if not delta_log.exists():
            return _DEFAULT
        from deltalake import DeltaTable
        import pyarrow as pa
        dt = DeltaTable(str(table_dir))
        version = dt.version()
        # Get row count and file sizes from metadata (avoids slow per-file stat calls)
        arro3_table = dt.get_add_actions(flatten=True)
        add_actions = pa.table(arro3_table).to_pydict()
        row_count = sum(add_actions.get("num_records", [0]))
        total_size = sum(add_actions.get("size_bytes", [0]))
        num_files = len(add_actions.get("path", []))
        return {
            "version": version,
            "num_files": num_files,
            "row_count": int(row_count),
            "size_bytes": total_size,
            "last_update": time.time(),
        }
    except Exception as exc:
        logger.debug("Could not read delta table %s: %s", table_name, exc)
        return {"version": 0, "num_files": 0, "row_count": 0,
                "size_bytes": 0, "last_update": None}


# ── Public API (unchanged signatures) ────────────────────────


def reset_metrics():
    """Reset all metrics by deleting JSON sidecar files."""
    with _lock:
        for p in [SPARK_JSON_PATH, RUST_JSON_PATH, RUST_ACCT_JSON_PATH,
                   VALIDATION_JSON_PATH, GENERATOR_JSON_PATH]:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


def _read_spark_json() -> dict:
    """Read Spark metrics from the best available source.

    Checks both spark_metrics.json (live container) and spark_api_dump.json
    (Spark REST API dump).  Returns whichever has more micro-batches so that
    a complete historical run is preferred over an incomplete live snapshot.
    """
    live = None
    try:
        if SPARK_JSON_PATH.exists():
            live = json.loads(SPARK_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass

    dump = _parse_spark_api_dump()

    # Pick the richer source
    live_batches = (live or {}).get("micro_batches_completed", 0)
    dump_batches = (dump or {}).get("micro_batches_completed", 0)

    if live and dump:
        return live if live_batches >= dump_batches else dump
    if live:
        return live
    if dump:
        return dump

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


def _parse_spark_api_dump() -> dict | None:
    """Parse spark_api_dump.json (Spark REST API response) into the
    normalised metrics dict that the dashboard expects.

    The dump contains ``stages.value`` (list of completed stage objects)
    and ``executors.value``.  Stages whose description contains
    ``"batch = <N>"`` are grouped into micro-batches and aggregated.
    """
    import re as _re

    try:
        if not SPARK_API_DUMP_PATH.exists():
            return None
        raw = SPARK_API_DUMP_PATH.read_bytes()
        data = json.loads(raw.decode("utf-8-sig"))
    except Exception:
        return None

    stages = data.get("stages", {}).get("value", [])
    if not stages:
        return None

    # Group stages by batch number ------------------------------------------
    batch_map: dict[int, list] = {}
    for stg in stages:
        desc = stg.get("description", "") or stg.get("name", "")
        m = _re.search(r"batch\s*=\s*(\d+)", desc, _re.IGNORECASE)
        if m:
            batch_id = int(m.group(1))
            batch_map.setdefault(batch_id, []).append(stg)

    if not batch_map:
        return None

    # Aggregate per-batch metrics -------------------------------------------
    batch_details = []
    batch_durations_ms: list[float] = []
    transform_times_ms: list[float] = []
    delta_write_times_ms: list[float] = []
    total_rows = 0

    for bid in sorted(batch_map):
        stg_list = batch_map[bid]
        rows = sum(s.get("inputRecords", 0) for s in stg_list)
        run_ms = sum(s.get("executorRunTime", 0) for s in stg_list)
        # executorCpuTime is in nanoseconds in Spark REST API
        cpu_ns = sum(s.get("executorCpuTime", 0) for s in stg_list)
        cpu_ms = cpu_ns / 1_000_000
        gc_ms = sum(s.get("jvmGcTime", 0) for s in stg_list)
        in_bytes = sum(s.get("inputBytes", 0) for s in stg_list)
        out_bytes = sum(s.get("outputBytes", 0) for s in stg_list)
        shuf_r = sum(s.get("shuffleReadBytes", 0) for s in stg_list)
        shuf_w = sum(s.get("shuffleWriteBytes", 0) for s in stg_list)
        tasks = sum(s.get("numCompleteTasks", 0) for s in stg_list)
        peak_mem = max((s.get("peakExecutionMemory", 0) for s in stg_list), default=0)

        # Estimate "transform" as cpu_ms; "write" as run_ms - cpu_ms (I/O wait)
        write_ms = max(0, run_ms - cpu_ms)

        total_rows += rows
        batch_durations_ms.append(float(run_ms))
        transform_times_ms.append(round(cpu_ms, 1))
        delta_write_times_ms.append(round(write_ms, 1))

        batch_details.append({
            "batch_id": bid,
            "rows": rows,
            "wall_ms": run_ms,
            "cpu_ms": round(cpu_ms, 1),
            "gc_ms": gc_ms,
            "cpu_util_pct": round(cpu_ms / max(1, run_ms) * 100, 1),
            "gc_pct": round(gc_ms / max(1, run_ms) * 100, 1),
            "input_mb": round(in_bytes / 1_048_576, 2),
            "output_mb": round(out_bytes / 1_048_576, 2),
            "shuffle_read_mb": round(shuf_r / 1_048_576, 2),
            "shuffle_write_mb": round(shuf_w / 1_048_576, 2),
            "peak_memory_mb": round(peak_mem / 1_048_576, 2),
            "tasks": tasks,
        })

    # Executor summary -------------------------------------------------------
    executors = data.get("executors", {}).get("value", [])
    exec_summary = {}
    if executors:
        active_cores = sum(e.get("totalCores", 0) for e in executors if e.get("id") != "driver")
        total_tasks = sum(e.get("totalTasks", 0) for e in executors)
        total_gc_ms = sum(e.get("totalGCTime", 0) for e in executors)
        total_dur_ms = sum(e.get("totalDuration", 0) for e in executors)
        total_input = sum(e.get("totalInputBytes", 0) for e in executors)
        total_output = sum(e.get("totalOutputBytes", 0) for e in executors)
        total_shuf_r = sum(e.get("totalShuffleRead", 0) for e in executors)
        total_shuf_w = sum(e.get("totalShuffleWrite", 0) for e in executors)
        peak_mem_exec = max((e.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0)
                             for e in executors), default=0)
        max_mem = sum(e.get("maxMemory", 0) for e in executors)

        exec_summary = {
            "active_cores": active_cores,
            "total_tasks": total_tasks,
            "total_gc_ms": total_gc_ms,
            "gc_pct": round(total_gc_ms / max(1, total_dur_ms) * 100, 1),
            "total_duration_ms": total_dur_ms,
            "peak_memory_mb": round(peak_mem_exec / 1_048_576, 2),
            "max_memory_mb": round(max_mem / 1_048_576, 2),
            "total_input_mb": round(total_input / 1_048_576, 2),
            "total_output_mb": round(total_output / 1_048_576, 2),
            "total_shuffle_read_mb": round(total_shuf_r / 1_048_576, 2),
            "total_shuffle_write_mb": round(total_shuf_w / 1_048_576, 2),
        }

    last_batch_rows = batch_details[-1]["rows"] if batch_details else 0
    return {
        "status": "completed",
        "micro_batches_completed": len(batch_details),
        "total_rows_processed": total_rows,
        "last_batch_rows": last_batch_rows,
        "first_update": None,
        "last_update": None,
        "batch_durations_ms": batch_durations_ms,
        "transform_times_ms": transform_times_ms,
        "delta_write_times_ms": delta_write_times_ms,
        "batch_details": batch_details,
        "executor": exec_summary,
    }


def _build_engine_metrics(data: dict) -> dict:
    """Build a normalized metrics dict from a raw JSON metrics file (Spark or Rust)."""
    bd = data.get("batch_durations_ms", [])
    tt = data.get("transform_times_ms", [])
    dw = data.get("delta_write_times_ms", [])
    return {
        "status": data.get("status", "idle"),
        "micro_batches_completed": data.get("micro_batches_completed", 0),
        "total_rows_processed": data.get("total_rows_processed", 0),
        "avg_batch_duration_ms": sum(bd) / len(bd) if bd else 0.0,
        "avg_transform_time_ms": sum(tt) / len(tt) if tt else 0.0,
        "avg_delta_write_time_ms": sum(dw) / len(dw) if dw else 0.0,
        "batch_durations_ms": bd,
        "transform_times_ms": tt,
        "delta_write_times_ms": dw,
        "last_batch_rows": data.get("last_batch_rows", 0),
        "first_update": data.get("first_update"),
        "last_update": data.get("last_update"),
        "batch_details": data.get("batch_details", []),
        "executor": data.get("executor", {}),
    }


def _read_rust_json() -> dict:
    """Read Rust pipeline metrics from the JSON file written by the container."""
    try:
        if RUST_JSON_PATH.exists():
            return json.loads(RUST_JSON_PATH.read_text(encoding="utf-8"))
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
        "batch_details": [],
        "executor": {},
    }


def _read_validation_json() -> dict:
    """Read the last validation report."""
    try:
        if VALIDATION_JSON_PATH.exists():
            return json.loads(VALIDATION_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _read_generator_json() -> dict:
    """Read generator metrics from the JSON sidecar file."""
    try:
        if GENERATOR_JSON_PATH.exists():
            return json.loads(GENERATOR_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {
        "status": "idle",
        "start_time": None,
        "target_bytes": 0,
        "bytes_sent": 0,
        "records_sent": 0,
        "batches_completed": 0,
        "elapsed_seconds": 0.0,
        "throughput_mb_per_sec": 0.0,
        "avg_batch_time_ms": 0.0,
        "avg_publish_latency_ms": 0.0,
        "batch_timings_ms": [],
        "publish_latencies_ms": [],
        "errors": 0,
        "last_update": None,
    }


def get_metrics() -> dict:
    """Read the current metrics snapshot (same dict shape as before)."""
    with _lock:
        # ── Delta tables: compute on-demand from deltalake ──
        delta = {}
        for table_name in _DELTA_TABLES:
            stats = _read_delta_table_stats(table_name)
            delta[f"{table_name}_table"] = stats

        # ── Generator metrics: read from JSON sidecar ──
        gen = _read_generator_json()

        # ── Spark metrics: read from JSON file (written by container) ──
        spark_data = _read_spark_json()
        bd = spark_data.get("batch_durations_ms", [])
        tt = spark_data.get("transform_times_ms", [])
        dw = spark_data.get("delta_write_times_ms", [])

        return {
            "generator": {
                "status": gen.get("status", "idle"),
                "start_time": gen.get("start_time"),
                "target_bytes": gen.get("target_bytes", 0),
                "bytes_sent": gen.get("bytes_sent", 0),
                "records_sent": gen.get("records_sent", 0),
                "batches_completed": gen.get("batches_completed", 0),
                "elapsed_seconds": gen.get("elapsed_seconds", 0.0),
                "throughput_mb_per_sec": gen.get("throughput_mb_per_sec", 0.0),
                "avg_batch_time_ms": gen.get("avg_batch_time_ms", 0.0),
                "avg_publish_latency_ms": gen.get("avg_publish_latency_ms", 0.0),
                "batch_timings_ms": gen.get("batch_timings_ms", []),
                "publish_latencies_ms": gen.get("publish_latencies_ms", []),
                "errors": gen.get("errors", 0),
                "last_update": gen.get("last_update"),
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
                "batch_details": spark_data.get("batch_details", []),
                "executor": spark_data.get("executor", {}),
            },
            "delta": delta,
            "pipeline": {
                "end_to_end_latency_ms": 0.0,
                "last_update": None,
            },
            "rust": _build_engine_metrics(_read_rust_json()),
            "validation": _read_validation_json(),
        }


# ── Legacy update functions (no-ops, kept for API compatibility) ──


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
    """No-op: generator writes its own JSON sidecar file."""
    pass


def update_spark_metrics(
    *,
    status: Optional[str] = None,
    batch_duration_ms: Optional[float] = None,
    transform_time_ms: Optional[float] = None,
    delta_write_time_ms: Optional[float] = None,
    rows_processed: Optional[int] = None,
):
    """No-op: Spark writes its own JSON via metrics_bridge."""
    pass


def update_delta_metrics(
    table_name: str,
    *,
    version: Optional[int] = None,
    num_files: Optional[int] = None,
    row_count: Optional[int] = None,
    size_bytes: Optional[int] = None,
):
    """No-op: delta stats are computed on-demand from deltalake."""
    pass


def update_pipeline_latency(latency_ms: float):
    """No-op: pipeline latency is not actively tracked."""
    pass
