"""
Pipeline Dashboard — Streamlit UI for monitoring the Spark-to-Rust
migration pipeline.

Features:
  1. Trigger data generation with configurable parameters
  2. Real-time progress monitoring (generator throughput, bytes sent)
  3. Performance instrumentation (publish latency, batch timing)
  4. Delta table inspection (schema, row counts, partitions)
  5. SQL query interface against Delta tables
  6. Spark streaming metrics (when available)

Run:
    streamlit run dashboard/app.py
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
import yaml

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.metrics import get_metrics, reset_metrics

# ── Page Config ───────────────────────────────────────────────

st.set_page_config(
    page_title="Pipeline Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers ───────────────────────────────────────────────────


def load_config() -> dict:
    cfg_path = PROJECT_ROOT / "config" / "app_config.yaml"
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)


def get_spark_cluster_status() -> dict:
    """Query Spark Master REST API for active applications and worker info."""
    try:
        resp = requests.get("http://localhost:8080/json/", timeout=3)
        data = resp.json()
        active_apps = data.get("activeapps", [])
        return {
            "status": "running" if active_apps else "idle",
            "active_apps": len(active_apps),
            "app_names": [a["name"] for a in active_apps],
            "workers": data.get("aliveworkers", 0),
            "cores_used": data.get("coresused", 0),
            "memory_used": data.get("memoryused", 0),
            "reachable": True,
        }
    except Exception:
        return {"status": "unreachable", "active_apps": 0, "app_names": [], "workers": 0, "reachable": False}


def fmt_bytes(b: float) -> str:
    if b >= 1e9:
        return f"{b / 1e9:.2f} GB"
    if b >= 1e6:
        return f"{b / 1e6:.1f} MB"
    if b >= 1e3:
        return f"{b / 1e3:.0f} KB"
    return f"{b:.0f} B"


def fmt_duration(s: float) -> str:
    if s >= 3600:
        return f"{s / 3600:.1f}h"
    if s >= 60:
        return f"{s / 60:.1f}m"
    return f"{s:.1f}s"


def get_delta_table_info(table_path: str) -> dict:
    """Safely read Delta table metadata using deltalake library."""
    try:
        from deltalake import DeltaTable
        p = Path(table_path)
        if not p.exists():
            return None
        dt = DeltaTable(str(p))
        file_uris = dt.file_uris()
        total_size = sum(
            os.path.getsize(f.replace("file://", "")) for f in file_uris
            if os.path.exists(f.replace("file://", ""))
        )
        arrow_table = dt.to_pyarrow_table()
        return {
            "version": dt.version(),
            "num_files": len(file_uris),
            "row_count": arrow_table.num_rows,
            "columns": arrow_table.num_columns,
            "size_bytes": total_size,
            "schema": dt.schema().to_arrow(),
            "partition_columns": dt.metadata().partition_columns,
        }
    except Exception as e:
        return {"error": str(e)}


def query_delta_table(table_path: str, sql: str) -> pd.DataFrame:
    """Query a Delta table using DuckDB SQL."""
    try:
        import duckdb
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        arrow = dt.to_pyarrow_table()
        con = duckdb.connect()
        con.register("txn", arrow)
        result = con.execute(sql).fetchdf()
        return result
    except ImportError:
        # Fallback: use pyarrow compute
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────

cfg = load_config()

st.sidebar.title("⚡ Pipeline Control")
page = st.sidebar.radio(
    "Navigate",
    ["📊 Dashboard", "🚀 Data Generator", "⚙️ Spark Jobs", "🔍 Delta Explorer", "📈 Performance"],
)

# ── Auto-refresh ──────────────────────────────────────────────

auto_refresh = st.sidebar.checkbox("Auto-refresh (3s)", value=False)
if auto_refresh:
    time.sleep(3)
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"Config: `config/app_config.yaml`")
st.sidebar.caption(f"Kafka: `{cfg['kafka']['bootstrap_servers']}`")

# ══════════════════════════════════════════════════════════════
# PAGE: Dashboard
# ══════════════════════════════════════════════════════════════

if page == "📊 Dashboard":
    st.title("📊 Pipeline Overview")

    metrics = get_metrics()
    gen = metrics["generator"]
    spark = metrics["spark"]
    delta = metrics["delta"]

    # ── Status Row ────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    with c1:
        gen_status = gen["status"]
        color = {"running": "🟢", "completed": "✅", "idle": "⚪"}.get(gen_status, "🔴")
        st.metric("Generator", f"{color} {gen_status.upper()}")
    with c2:
        cluster = get_spark_cluster_status()
        spark_status = cluster["status"]
        color = {"running": "🟢", "idle": "⚪", "unreachable": "🔴"}.get(spark_status, "🔴")
        st.metric("Spark Streaming", f"{color} {spark_status.upper()}")
        if cluster["active_apps"] > 0:
            st.caption(f"{cluster['active_apps']} app(s): {', '.join(cluster['app_names'])}")
    with c3:
        latency = metrics["pipeline"]["end_to_end_latency_ms"]
        st.metric("E2E Latency", f"{latency:.0f} ms")

    st.markdown("---")

    # ── Generator Progress ────────────────────────────────
    st.subheader("Data Generation Progress")

    if gen["target_bytes"] > 0:
        progress = min(1.0, gen["bytes_sent"] / gen["target_bytes"])
        st.progress(progress, text=f"{progress * 100:.1f}% — {fmt_bytes(gen['bytes_sent'])} / {fmt_bytes(gen['target_bytes'])}")
    else:
        st.progress(0.0, text="Not started")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Records Sent", f"{gen['records_sent']:,}")
    c2.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
    c3.metric("Elapsed", fmt_duration(gen["elapsed_seconds"]))
    c4.metric("Batches", f"{gen['batches_completed']:,}")

    st.markdown("---")

    # ── Generator Latency Charts ──────────────────────────
    st.subheader("Generator Timing")
    col_a, col_b = st.columns(2)

    with col_a:
        if gen["batch_timings_ms"]:
            st.caption("Batch Production Time (ms)")
            st.line_chart(pd.DataFrame({"batch_time_ms": gen["batch_timings_ms"]}))
        else:
            st.info("No batch timing data yet")

    with col_b:
        if gen["publish_latencies_ms"]:
            st.caption("Kafka Publish Latency — flush() time (ms)")
            st.line_chart(pd.DataFrame({"publish_latency_ms": gen["publish_latencies_ms"]}))
        else:
            st.info("No publish latency data yet")

    st.markdown("---")

    # ── Delta Table Summary ───────────────────────────────
    st.subheader("Delta Tables")
    txn_path = cfg["delta"]["table_path"]
    acct_path = cfg["delta"]["accounts_table_path"]

    col1, col2 = st.columns(2)
    with col1:
        st.caption("📁 Transactions Table")
        info = get_delta_table_info(txn_path)
        if info and "error" not in info:
            st.metric("Rows", f"{info['row_count']:,}")
            st.metric("Files", f"{info['num_files']:,}")
            st.metric("Version", info["version"])
            st.metric("Size", fmt_bytes(info["size_bytes"]))
        elif info and "error" in info:
            st.warning(f"Error: {info['error']}")
        else:
            st.info("Table not found — run Spark streaming first")

    with col2:
        st.caption("📁 Accounts Table")
        info = get_delta_table_info(acct_path)
        if info and "error" not in info:
            st.metric("Rows", f"{info['row_count']:,}")
            st.metric("Files", f"{info['num_files']:,}")
            st.metric("Version", info["version"])
            st.metric("Size", fmt_bytes(info["size_bytes"]))
        elif info and "error" in info:
            st.warning(f"Error: {info['error']}")
        else:
            st.info("Table not found — run account upsert job first")


# ══════════════════════════════════════════════════════════════
# PAGE: Data Generator
# ══════════════════════════════════════════════════════════════

elif page == "🚀 Data Generator":
    st.title("🚀 Data Generator Control")

    st.markdown("""
    Generate synthetic financial transaction data and push to Kafka.
    Each transaction also emits account-update events for the Delta MERGE pipeline.
    """)

    # ── Config & Trigger ──────────────────────────────────
    with st.form("generator_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            target_gb = st.number_input(
                "Target Data (GB)", min_value=0.1, max_value=50.0,
                value=1.0, step=0.5,
            )
        with c2:
            batch_size = st.number_input(
                "Batch Size", min_value=100, max_value=100000,
                value=cfg["generator"]["batch_size"], step=1000,
            )
        with c3:
            num_threads = st.number_input(
                "Producer Threads", min_value=1, max_value=16,
                value=cfg["generator"]["num_threads"], step=1,
            )

        submitted = st.form_submit_button("🚀 Start Data Generation", type="primary")

    if submitted:
        reset_metrics()
        cmd = [
            sys.executable, "-m", "data_generator.run",
            "--target-gb", str(target_gb),
            "--batch-size", str(int(batch_size)),
            "-t", str(int(num_threads)),
        ]
        subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        st.success(f"Data generation started: {target_gb} GB, {int(batch_size)} batch, {int(num_threads)} threads")
        time.sleep(1)
        st.rerun()

    st.markdown("---")

    # ── Live Progress ─────────────────────────────────────
    st.subheader("Live Progress")
    metrics = get_metrics()
    gen = metrics["generator"]

    if gen["status"] == "running":
        if gen["target_bytes"] > 0:
            progress = min(1.0, gen["bytes_sent"] / gen["target_bytes"])
            st.progress(progress, text=f"{progress * 100:.1f}%")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Status", gen["status"].upper())
        c2.metric("Data Sent", fmt_bytes(gen["bytes_sent"]))
        c3.metric("Records", f"{gen['records_sent']:,}")
        c4.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
        c5.metric("Errors", gen["errors"])

    elif gen["status"] == "completed":
        st.success("Generation completed!")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Data", fmt_bytes(gen["bytes_sent"]))
        c2.metric("Total Records", f"{gen['records_sent']:,}")
        c3.metric("Duration", fmt_duration(gen["elapsed_seconds"]))
        c4.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
    else:
        st.info("Generator is idle. Submit the form above to start.")

    st.markdown("---")

    # ── Performance Stats ─────────────────────────────────
    st.subheader("Performance Metrics")

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Avg Batch Production Time", f"{gen['avg_batch_time_ms']:.0f} ms")
        if gen["batch_timings_ms"]:
            st.caption("Batch timing histogram (ms)")
            df = pd.DataFrame({"ms": gen["batch_timings_ms"]})
            st.bar_chart(df["ms"].value_counts().sort_index().head(30))
    with c2:
        st.metric("Avg Kafka Publish Latency", f"{gen['avg_publish_latency_ms']:.0f} ms")
        if gen["publish_latencies_ms"]:
            st.caption("Publish latency histogram (ms)")
            df = pd.DataFrame({"ms": gen["publish_latencies_ms"]})
            st.bar_chart(df["ms"].value_counts().sort_index().head(30))

    # ── Reset ─────────────────────────────────────────────
    st.markdown("---")
    if st.button("🗑️ Reset Metrics"):
        reset_metrics()
        st.rerun()


# ══════════════════════════════════════════════════════════════
# PAGE: Spark Jobs
# ══════════════════════════════════════════════════════════════

elif page == "⚙️ Spark Jobs":
    st.title("⚙️ Spark Jobs")

    st.markdown("""
    Launch Spark jobs inside the Docker Spark cluster to process data from
    Kafka into Delta Lake tables.
    """)

    # Track running jobs in session state
    if "spark_processes" not in st.session_state:
        st.session_state.spark_processes = {}

    def _check_proc(name: str) -> str:
        """Check if a Spark job is running (local subprocess or cluster app)."""
        # First check local subprocess
        proc = st.session_state.spark_processes.get(name)
        if proc is not None:
            ret = proc.poll()
            if ret is None:
                return "running"
            if ret == 0:
                return "completed"
            return f"failed (exit {ret})"
        # Also check Spark Master for active apps (catches jobs started externally)
        try:
            cluster = get_spark_cluster_status()
            if cluster["reachable"]:
                for app_name in cluster["app_names"]:
                    if name.replace("_", "") in app_name.lower().replace("_", "").replace(" ", ""):
                        return "running"
        except Exception:
            pass
        return "stopped"

    # Map job name → the script path inside the container
    CONTAINER_SCRIPT_MAP = {
        "streaming": "/opt/spark-jobs/streaming_job.py",
        "account_upsert": "/opt/spark-jobs/account_upsert_job.py",
        "batch": "/opt/spark-jobs/batch_job.py",
        "analytics": "/opt/spark-jobs/batch_analytics_job.py",
    }

    def _ensure_container_deps():
        """Install Python deps in Spark container if not already present."""
        subprocess.run(
            ["docker", "exec", "spark-master", "pip", "install", "-q",
             "pyyaml", "delta-spark==3.1.0"],
            capture_output=True, timeout=120,
        )

    def _start_spark_job(name: str, script: str, extra_args: list = None):
        """Start a Spark job inside the Docker Spark container via spark-submit."""
        _ensure_container_deps()
        container_script = CONTAINER_SCRIPT_MAP.get(name, f"/opt/spark-jobs/{Path(script).name}")
        cmd = [
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--jars", ",".join([
                "/opt/extra-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
                "/opt/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
                "/opt/extra-jars/kafka-clients-3.4.1.jar",
                "/opt/extra-jars/commons-pool2-2.11.1.jar",
                "/opt/extra-jars/delta-spark_2.12-3.1.0.jar",
                "/opt/extra-jars/delta-storage-3.1.0.jar",
            ]),
            "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            container_script,
            "--config", "/opt/config/app_config.yaml",
        ]
        if extra_args:
            cmd.extend(extra_args)
        log_path = PROJECT_ROOT / "logs" / f"{name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        st.session_state.spark_processes[name] = proc
        return proc

    def _stop_spark_job(name: str):
        """Stop a running Spark job by killing it in the Spark cluster and locally."""
        # Kill the app via Spark Master REST API
        try:
            cluster = get_spark_cluster_status()
            if cluster["reachable"]:
                resp = requests.get("http://localhost:8080/json/", timeout=3)
                for app in resp.json().get("activeapps", []):
                    # Match by job name in the app name
                    app_name_lower = app["name"].lower()
                    if name.replace("_", "") in app_name_lower.replace("_", ""):
                        requests.post(f"http://localhost:8080/app/kill/?id={app['id']}", timeout=5)
        except Exception:
            pass
        # Kill the local docker-exec process
        proc = st.session_state.spark_processes.get(name)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        st.session_state.spark_processes.pop(name, None)

    # ── Job Cards ─────────────────────────────────────────

    SPARK_JOBS = {
        "streaming": {
            "title": "📡 Streaming Job (Kafka → Delta)",
            "desc": "Reads transactions from Kafka, applies transforms (risk classification, "
                    "geo-features, PII masking, amount bucketing), and writes to the "
                    "transactions Delta table. Runs continuously until stopped.",
            "script": "spark_jobs/streaming_job.py",
            "long_running": True,
        },
        "account_upsert": {
            "title": "👤 Account Upsert Job (Kafka → Delta MERGE)",
            "desc": "Reads account-update events from Kafka and upserts into the accounts "
                    "Delta table using MERGE (insert new accounts, update running totals). "
                    "Runs continuously until stopped.",
            "script": "spark_jobs/account_upsert_job.py",
            "long_running": True,
        },
        "batch": {
            "title": "📦 Batch Reprocess Job",
            "desc": "One-shot batch read of the full Kafka topic → Delta table overwrite. "
                    "Use for backfill or reprocessing.",
            "script": "spark_jobs/batch_job.py",
            "long_running": False,
        },
        "analytics": {
            "title": "📊 Batch Analytics Job",
            "desc": "Reads the enriched Delta table and runs advanced analytics: window "
                    "functions, pivots, joins, aggregations. Writes results to Parquet.",
            "script": "spark_jobs/batch_analytics_job.py",
            "long_running": False,
        },
    }

    for job_key, job_info in SPARK_JOBS.items():
        with st.container(border=True):
            st.subheader(job_info["title"])
            st.caption(job_info["desc"])

            status = _check_proc(job_key)
            status_icon = {"running": "🟢", "completed": "✅", "stopped": "⚪"}
            icon = status_icon.get(status, "🔴")

            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.write(f"**Status:** {icon} {status}")
            with col2:
                if status == "running":
                    if st.button("⏹️ Stop", key=f"stop_{job_key}"):
                        _stop_spark_job(job_key)
                        st.success(f"{job_info['title']} stopped.")
                        time.sleep(0.5)
                        st.rerun()
                else:
                    if st.button("▶️ Start", key=f"start_{job_key}", type="primary"):
                        _start_spark_job(job_key, job_info["script"])
                        st.success(f"{job_info['title']} started!")
                        time.sleep(0.5)
                        st.rerun()
            with col3:
                log_path = PROJECT_ROOT / "logs" / f"{job_key}.log"
                if log_path.exists():
                    if st.button("📄 Logs", key=f"logs_{job_key}"):
                        st.session_state[f"show_logs_{job_key}"] = not st.session_state.get(f"show_logs_{job_key}", False)

            # Show logs if toggled
            if st.session_state.get(f"show_logs_{job_key}", False):
                log_path = PROJECT_ROOT / "logs" / f"{job_key}.log"
                if log_path.exists():
                    log_content = log_path.read_text(errors="replace")
                    # Show last 100 lines
                    lines = log_content.strip().split("\n")
                    tail = "\n".join(lines[-100:])
                    st.code(tail, language="log")
                else:
                    st.info("No log file yet.")

    st.markdown("---")

    # ── Bulk Controls ─────────────────────────────────────
    st.subheader("Bulk Controls")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶️ Start All Streaming Jobs", type="primary"):
            for key in ["streaming", "account_upsert"]:
                if _check_proc(key) != "running":
                    _start_spark_job(key, SPARK_JOBS[key]["script"])
            st.success("Streaming + Account Upsert jobs started!")
            time.sleep(0.5)
            st.rerun()
    with col2:
        if st.button("⏹️ Stop All Jobs"):
            for key in SPARK_JOBS:
                _stop_spark_job(key)
            st.success("All jobs stopped.")
            time.sleep(0.5)
            st.rerun()

    # ── Pipeline Reset ────────────────────────────────────
    st.markdown("---")
    st.subheader("🔄 Pipeline Reset")
    st.markdown(
        "Stop all running jobs, wipe checkpoint / Delta data, and "
        "optionally restart ingestion from the beginning of the Kafka topics."
    )

    reset_tab1, reset_tab2, reset_tab3 = st.tabs([
        "🗑️ Wipe Checkpoints & Restart",
        "💣 Delete All Delta Files",
        "☢️ Full Reset (Both)",
    ])

    # Gather paths from config
    _ckpt_paths = [
        ("Transactions checkpoint", Path(cfg["spark"].get("local_checkpoint", "./checkpoints/financial_txn"))),
        ("Accounts checkpoint", Path(cfg["accounts"].get("local_checkpoint", "./checkpoints/account_upsert"))),
    ]
    _delta_paths = [
        ("Transactions Delta", Path(cfg["delta"].get("table_path", "./delta_output/financial_transactions"))),
        ("Accounts Delta", Path(cfg["delta"].get("accounts_table_path", "./delta_output/accounts"))),
    ]

    def _stop_all_jobs():
        for key in SPARK_JOBS:
            _stop_spark_job(key)

    def _wipe_paths(paths: list[tuple[str, Path]]) -> list[str]:
        import shutil
        removed = []
        for label, p in paths:
            if p.exists():
                shutil.rmtree(p)
                removed.append(f"{label} (`{p}`)")
        return removed

    def _restart_streaming_jobs():
        for key in ["streaming", "account_upsert"]:
            _start_spark_job(key, SPARK_JOBS[key]["script"])

    # ── Tab 1: Wipe Checkpoints & Restart ─────────────────
    with reset_tab1:
        st.info(
            "This will **stop all running jobs**, delete checkpoint directories, "
            "then restart the streaming jobs. They will re-read from the earliest "
            "available Kafka offset."
        )
        existing_ckpts = [(l, p) for l, p in _ckpt_paths if p.exists()]
        if existing_ckpts:
            for label, p in existing_ckpts:
                st.write(f"  - {label}: `{p}`")
        else:
            st.write("No checkpoint directories found on disk.")

        ckpt_confirm = st.text_input(
            "Type **WIPE CHECKPOINTS** to confirm", key="ckpt_confirm"
        )
        if st.button("🗑️ Wipe Checkpoints & Restart", key="btn_wipe_ckpt"):
            if ckpt_confirm.strip() == "WIPE CHECKPOINTS":
                _stop_all_jobs()
                removed = _wipe_paths(_ckpt_paths)
                if removed:
                    st.success("Removed: " + ", ".join(removed))
                else:
                    st.info("No checkpoint directories to remove.")
                _restart_streaming_jobs()
                st.success("Streaming jobs restarted — ingestion will resume from earliest offset.")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Type exactly: `WIPE CHECKPOINTS`")

    # ── Tab 2: Delete All Delta Files ─────────────────────
    with reset_tab2:
        st.warning(
            "This will **stop all running jobs** and permanently delete all "
            "Delta Lake table data. The tables will be empty until jobs re-create them."
        )
        existing_delta = [(l, p) for l, p in _delta_paths if p.exists()]
        if existing_delta:
            for label, p in existing_delta:
                st.write(f"  - {label}: `{p}`")
        else:
            st.write("No Delta table directories found on disk.")

        delta_confirm = st.text_input(
            "Type **DELETE ALL DELTA** to confirm", key="delta_confirm"
        )
        if st.button("💣 Delete All Delta Files", key="btn_del_delta"):
            if delta_confirm.strip() == "DELETE ALL DELTA":
                _stop_all_jobs()
                removed = _wipe_paths(_delta_paths)
                if removed:
                    st.success("Removed: " + ", ".join(removed))
                else:
                    st.info("No Delta directories to remove.")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Type exactly: `DELETE ALL DELTA`")

    # ── Tab 3: Full Reset (checkpoints + delta + restart) ─
    with reset_tab3:
        st.error(
            "⚠️ **Nuclear option** — stops all jobs, deletes **all** checkpoint "
            "and Delta Lake data, resets metrics, then restarts ingestion from scratch."
        )
        full_confirm = st.text_input(
            "Type **FULL RESET** to confirm", key="full_reset_confirm"
        )
        if st.button("☢️ Full Reset", key="btn_full_reset", type="secondary"):
            if full_confirm.strip() == "FULL RESET":
                _stop_all_jobs()
                removed_ckpt = _wipe_paths(_ckpt_paths)
                removed_delta = _wipe_paths(_delta_paths)
                reset_metrics()
                all_removed = removed_ckpt + removed_delta
                if all_removed:
                    st.success("Removed: " + ", ".join(all_removed))
                else:
                    st.info("Nothing to remove — directories were already clean.")
                st.info("Metrics reset.")
                _restart_streaming_jobs()
                st.success("Streaming jobs restarted — full pipeline reset complete.")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Type exactly: `FULL RESET`")


# ══════════════════════════════════════════════════════════════
# PAGE: Delta Explorer
# ══════════════════════════════════════════════════════════════

elif page == "🔍 Delta Explorer":
    st.title("🔍 Delta Table Explorer")

    table_choice = st.selectbox(
        "Select Table",
        ["Transactions", "Accounts"],
    )

    table_path = (
        cfg["delta"]["table_path"]
        if table_choice == "Transactions"
        else cfg["delta"]["accounts_table_path"]
    )

    info = get_delta_table_info(table_path)

    if info is None:
        st.warning(f"Delta table not found at `{table_path}`. Run the pipeline first.")
    elif "error" in info:
        st.error(f"Error reading table: {info['error']}")
    else:
        # ── Table Info ────────────────────────────────────
        st.subheader("Table Metadata")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Version", info["version"])
        c2.metric("Rows", f"{info['row_count']:,}")
        c3.metric("Files", f"{info['num_files']:,}")
        c4.metric("Size", fmt_bytes(info["size_bytes"]))

        if info.get("partition_columns"):
            st.caption(f"**Partitioned by:** {', '.join(info['partition_columns'])}")

        # ── Schema ────────────────────────────────────────
        st.subheader("Schema")
        schema_data = []
        for i, field in enumerate(info["schema"]):
            schema_data.append({
                "#": i,
                "Column": field.name,
                "Type": str(field.type),
                "Nullable": field.nullable,
            })
        st.dataframe(pd.DataFrame(schema_data), use_container_width=True, hide_index=True)

        # ── Sample Data ───────────────────────────────────
        st.subheader("Sample Data")
        num_rows = st.slider("Number of rows", 5, 500, 20)
        try:
            from deltalake import DeltaTable
            dt = DeltaTable(table_path)
            sample_df = dt.to_pandas().head(num_rows)
            st.dataframe(sample_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading sample: {e}")

        # ── SQL Query ─────────────────────────────────────
        st.subheader("SQL Query")
        st.caption("Query against the table using SQL. The table is registered as `txn`.")

        if table_choice == "Transactions":
            default_queries = {
                "Row count": "SELECT COUNT(*) as total_rows FROM txn",
                "By currency": "SELECT currency, COUNT(*) as cnt, ROUND(AVG(amount), 2) as avg_amt FROM txn GROUP BY currency ORDER BY cnt DESC",
                "By status": "SELECT status, COUNT(*) as cnt FROM txn GROUP BY status ORDER BY cnt DESC",
                "By transaction type": "SELECT transaction_type, COUNT(*) as cnt, ROUND(SUM(amount), 2) as total_volume FROM txn GROUP BY transaction_type ORDER BY total_volume DESC",
                "Flagged transactions": "SELECT * FROM txn WHERE is_flagged = true LIMIT 20",
                "Top amounts": "SELECT transaction_id, amount, currency, sender_name, receiver_name FROM txn ORDER BY amount DESC LIMIT 20",
                "Hourly volume": "SELECT EXTRACT(HOUR FROM CAST(timestamp AS TIMESTAMP)) as hour, COUNT(*) as cnt FROM txn GROUP BY hour ORDER BY hour",
                "Custom": "",
            }
        else:
            default_queries = {
                "Row count": "SELECT COUNT(*) as total_rows FROM txn",
                "By country": "SELECT country, COUNT(*) as cnt FROM txn GROUP BY country ORDER BY cnt DESC",
                "By currency": "SELECT currency, COUNT(*) as cnt, ROUND(AVG(balance), 2) as avg_balance FROM txn GROUP BY currency ORDER BY cnt DESC",
                "By account type": "SELECT account_type, COUNT(*) as cnt, ROUND(AVG(balance), 2) as avg_balance FROM txn GROUP BY account_type ORDER BY cnt DESC",
                "By status": "SELECT account_status, COUNT(*) as cnt FROM txn GROUP BY account_status ORDER BY cnt DESC",
                "Top balances": "SELECT account_id, holder_name, balance, currency, country FROM txn ORDER BY balance DESC LIMIT 20",
                "High risk": "SELECT account_id, holder_name, risk_rating, balance, country FROM txn WHERE risk_rating > 0.7 ORDER BY risk_rating DESC LIMIT 20",
                "Most transactions": "SELECT account_id, holder_name, transaction_count, total_sent, total_received FROM txn ORDER BY transaction_count DESC LIMIT 20",
                "Custom": "",
            }

        query_choice = st.selectbox("Preset queries", list(default_queries.keys()))
        sql_input = st.text_area(
            "SQL",
            value=default_queries[query_choice],
            height=80,
            placeholder="SELECT * FROM txn LIMIT 10",
        )

        if st.button("▶️ Run Query", type="primary"):
            if sql_input.strip():
                with st.spinner("Running query..."):
                    result_df = query_delta_table(table_path, sql_input.strip())
                if not result_df.empty:
                    st.dataframe(result_df, use_container_width=True)
                    st.caption(f"{len(result_df)} rows returned")
                else:
                    st.info("Query returned no results")
            else:
                st.warning("Enter a SQL query first")

        # ── History ───────────────────────────────────────
        st.subheader("Table History")
        try:
            from deltalake import DeltaTable
            dt = DeltaTable(table_path)
            history = dt.history()
            if history:
                hist_df = pd.DataFrame(history)
                display_cols = [c for c in ["version", "timestamp", "operation", "operationParameters"] if c in hist_df.columns]
                st.dataframe(hist_df[display_cols].head(20), use_container_width=True)
            else:
                st.info("No history available")
        except Exception as e:
            st.error(f"Error loading history: {e}")

        # ── Delete All Data ───────────────────────────────
        st.markdown("---")
        st.subheader("🗑️ Delete Table Data")
        st.warning("This will permanently remove all data and reset the Delta table. This cannot be undone.")

        confirm_text = st.text_input(
            f"Type **DELETE {table_choice.upper()}** to confirm",
            key="delete_confirm",
        )
        if st.button("🗑️ Delete All Data", type="secondary"):
            expected = f"DELETE {table_choice.upper()}"
            if confirm_text.strip() == expected:
                try:
                    import shutil
                    p = Path(table_path)
                    if p.exists():
                        shutil.rmtree(p)
                        st.success(f"Deleted all data at `{table_path}`.")
                        # Also clean corresponding checkpoints
                        ckpt_key = "local_checkpoint" if table_choice == "Transactions" else "local_checkpoint"
                        cfg_section = "spark" if table_choice == "Transactions" else "accounts"
                        ckpt_path = Path(cfg[cfg_section].get(ckpt_key, ""))
                        if ckpt_path.exists():
                            shutil.rmtree(ckpt_path)
                            st.info(f"Cleaned checkpoint at `{ckpt_path}`.")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.info("Table directory does not exist — nothing to delete.")
                except Exception as e:
                    st.error(f"Error deleting table: {e}")
            else:
                st.error(f"Confirmation mismatch. Type exactly: `{expected}`")


# ══════════════════════════════════════════════════════════════
# PAGE: Performance
# ══════════════════════════════════════════════════════════════

elif page == "📈 Performance":
    st.title("📈 Performance Analytics")

    metrics = get_metrics()
    gen = metrics["generator"]
    spark = metrics["spark"]

    # ── Generator Performance ─────────────────────────────
    st.subheader("🔧 Data Generator Performance")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Batch Time", f"{gen['avg_batch_time_ms']:.0f} ms")
    c2.metric("Avg Publish Latency", f"{gen['avg_publish_latency_ms']:.0f} ms")
    c3.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
    c4.metric("Batches", f"{gen['batches_completed']:,}")

    if gen["batch_timings_ms"]:
        col1, col2 = st.columns(2)
        with col1:
            st.caption("Batch Production Time (ms) — per batch")
            st.line_chart(
                pd.DataFrame({"batch_time_ms": gen["batch_timings_ms"]}),
                use_container_width=True,
            )
        with col2:
            st.caption("Kafka Publish Latency (ms) — flush() time")
            if gen["publish_latencies_ms"]:
                st.line_chart(
                    pd.DataFrame({"publish_latency_ms": gen["publish_latencies_ms"]}),
                    use_container_width=True,
                )

        # Distribution summary
        st.caption("Timing Distribution Summary")
        batch_series = pd.Series(gen["batch_timings_ms"])
        pub_series = pd.Series(gen["publish_latencies_ms"]) if gen["publish_latencies_ms"] else pd.Series([0])
        summary = pd.DataFrame({
            "Metric": ["Batch Time (ms)", "Publish Latency (ms)"],
            "Min": [batch_series.min(), pub_series.min()],
            "P50": [batch_series.median(), pub_series.median()],
            "P95": [batch_series.quantile(0.95), pub_series.quantile(0.95)],
            "P99": [batch_series.quantile(0.99), pub_series.quantile(0.99)],
            "Max": [batch_series.max(), pub_series.max()],
            "Avg": [batch_series.mean(), pub_series.mean()],
        }).round(2)
        st.dataframe(summary, use_container_width=True, hide_index=True)
    else:
        st.info("No generator timing data yet. Start a data generation run.")

    st.markdown("---")

    # ── Spark Performance ─────────────────────────────────
    st.subheader("⚡ Spark Streaming Performance")

    # ── Cost & Runtime Summary ────────────────────────────
    #
    # Estimate compute cost based on cluster resources and
    # wall-clock time.  Uses Azure HDInsight D4v2 pricing as
    # a reference (~$0.752 / core-hour for on-demand Spark).
    COST_PER_CORE_HOUR = float(cfg.get("spark", {}).get("cost_per_core_hour", 0.752))
    WORKER_CORES = int(cfg.get("spark", {}).get("worker_cores", 4))
    _mem_raw = cfg.get("spark", {}).get("worker_memory", "4g")
    WORKER_MEMORY_GB = int(str(_mem_raw).lower().replace("g", "")) if isinstance(_mem_raw, str) else int(_mem_raw)

    first_ts = spark.get("first_update")
    last_ts = spark.get("last_update")
    wall_clock_s = (last_ts - first_ts) if (first_ts and last_ts and last_ts > first_ts) else 0.0
    wall_clock_hours = wall_clock_s / 3600.0
    total_compute_hours = wall_clock_hours * WORKER_CORES
    estimated_cost = total_compute_hours * COST_PER_CORE_HOUR

    # Total time actually spent processing (sum of batch durations)
    total_transform_ms = sum(spark["transform_times_ms"]) if spark["transform_times_ms"] else 0
    total_write_ms = sum(spark["delta_write_times_ms"]) if spark["delta_write_times_ms"] else 0
    total_processing_ms = sum(spark["batch_durations_ms"]) if spark["batch_durations_ms"] else 0

    st.markdown("##### 💰 Compute Cost & Runtime")
    cc1, cc2, cc3, cc4, cc5 = st.columns(5)
    cc1.metric("Wall-Clock Time", fmt_duration(wall_clock_s) if wall_clock_s > 0 else "—")
    cc2.metric("CPU Processing", fmt_duration(total_processing_ms / 1000))
    cc3.metric("Cluster Cores", f"{WORKER_CORES}")
    cc4.metric("Compute Hours", f"{total_compute_hours:.2f} hrs")
    cc5.metric("Est. Cost", f"${estimated_cost:.3f}",
               help=f"Based on {WORKER_CORES} cores × {wall_clock_hours:.2f} hrs × ${COST_PER_CORE_HOUR}/core-hr (Azure HDInsight D4v2 reference)")

    if wall_clock_s > 0:
        idle_s = max(0, wall_clock_s - (total_processing_ms / 1000))
        util_pct = min(100.0, (total_processing_ms / 1000) / wall_clock_s * 100)
        uc1, uc2, uc3 = st.columns(3)
        uc1.metric("Cluster Utilization", f"{util_pct:.1f}%",
                    help="% of wall-clock time spent actively processing batches")
        uc2.metric("Idle Time", fmt_duration(idle_s))
        uc3.metric("Cost / 1M Rows",
                    f"${(estimated_cost / max(1, spark['total_rows_processed']) * 1_000_000):.4f}"
                    if spark['total_rows_processed'] > 0 else "—")

    st.markdown("")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Micro-batches", f"{spark['micro_batches_completed']:,}")
    c2.metric("Avg Batch Duration", f"{spark['avg_batch_duration_ms']:.0f} ms")
    c3.metric("Avg Transform Time", f"{spark['avg_transform_time_ms']:.0f} ms")
    c4.metric("Avg Delta Write Time", f"{spark['avg_delta_write_time_ms']:.0f} ms")

    if spark["batch_durations_ms"]:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption("Micro-batch Duration (ms)")
            st.line_chart(
                pd.DataFrame({"duration_ms": spark["batch_durations_ms"]}),
                use_container_width=True,
            )
        with col2:
            st.caption("Transform Time (ms)")
            if spark["transform_times_ms"]:
                st.line_chart(
                    pd.DataFrame({"transform_ms": spark["transform_times_ms"]}),
                    use_container_width=True,
                )
        with col3:
            st.caption("Delta Write Time (ms)")
            if spark["delta_write_times_ms"]:
                st.line_chart(
                    pd.DataFrame({"write_ms": spark["delta_write_times_ms"]}),
                    use_container_width=True,
                )

        # Spark distribution summary
        st.caption("Spark Timing Distribution")
        batch_s = pd.Series(spark["batch_durations_ms"])
        transform_s = pd.Series(spark["transform_times_ms"]) if spark["transform_times_ms"] else pd.Series([0])
        write_s = pd.Series(spark["delta_write_times_ms"]) if spark["delta_write_times_ms"] else pd.Series([0])
        spark_summary = pd.DataFrame({
            "Metric": ["Batch Duration (ms)", "Transform Time (ms)", "Delta Write (ms)"],
            "Min": [batch_s.min(), transform_s.min(), write_s.min()],
            "P50": [batch_s.median(), transform_s.median(), write_s.median()],
            "P95": [batch_s.quantile(0.95), transform_s.quantile(0.95), write_s.quantile(0.95)],
            "P99": [batch_s.quantile(0.99), transform_s.quantile(0.99), write_s.quantile(0.99)],
            "Max": [batch_s.max(), transform_s.max(), write_s.max()],
            "Avg": [batch_s.mean(), transform_s.mean(), write_s.mean()],
        }).round(2)
        st.dataframe(spark_summary, use_container_width=True, hide_index=True)

        # ── Transform vs Write Breakdown ──────────────────
        st.markdown("")
        st.markdown("##### ⏱️ Time Breakdown: Transform vs Delta Write")

        # Build a per-batch breakdown DataFrame
        n = min(len(spark["transform_times_ms"]), len(spark["delta_write_times_ms"]))
        if n > 0:
            breakdown_df = pd.DataFrame({
                "Transform (ms)": spark["transform_times_ms"][:n],
                "Delta Write (ms)": spark["delta_write_times_ms"][:n],
            })
            st.area_chart(breakdown_df, use_container_width=True)

            # Totals
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("Total Transform Time", fmt_duration(total_transform_ms / 1000))
            tc2.metric("Total Delta Write Time", fmt_duration(total_write_ms / 1000))
            pct_transform = total_transform_ms / max(1, total_processing_ms) * 100
            pct_write = total_write_ms / max(1, total_processing_ms) * 100
            tc3.metric("Transform / Write Ratio", f"{pct_transform:.0f}% / {pct_write:.0f}%")

        st.metric("Total Rows Processed", f"{spark['total_rows_processed']:,}")
        st.metric("Last Batch Rows", f"{spark['last_batch_rows']:,}")
    else:
        st.info("No Spark metrics yet. Run the streaming job to collect data.")

    st.markdown("---")

    # ── End-to-End Pipeline ───────────────────────────────
    st.subheader("🔗 End-to-End Pipeline")

    pipeline = metrics["pipeline"]
    st.metric("End-to-End Latency", f"{pipeline['end_to_end_latency_ms']:.0f} ms",
              help="Time from Kafka publish to Delta commit")

    # Waterfall breakdown
    st.caption("Latency Waterfall (averages)")
    waterfall_data = {
        "Stage": ["Generate + Serialize", "Kafka Publish (flush)", "Spark Transform", "Delta Write"],
        "Avg (ms)": [
            max(0, gen["avg_batch_time_ms"] - gen["avg_publish_latency_ms"]),
            gen["avg_publish_latency_ms"],
            spark["avg_transform_time_ms"],
            spark["avg_delta_write_time_ms"],
        ],
    }
    wf_df = pd.DataFrame(waterfall_data)
    st.bar_chart(wf_df.set_index("Stage"), use_container_width=True)
    st.dataframe(wf_df, use_container_width=True, hide_index=True)

    # ── Raw Metrics JSON ──────────────────────────────────
    st.markdown("---")
    with st.expander("📋 Raw Metrics JSON"):
        st.json(metrics)
