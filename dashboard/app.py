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
_IN_DOCKER = os.environ.get("DASHBOARD_DOCKER") == "1"
PROJECT_ROOT = Path("/app") if _IN_DOCKER else Path(__file__).resolve().parent.parent
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
        spark_host = os.environ.get("SPARK_MASTER_HOST", "localhost")
        resp = requests.get(f"http://{spark_host}:8080/json/", timeout=3)
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
        import pyarrow as pa
        p = Path(table_path)
        if not p.exists():
            return None
        dt = DeltaTable(str(p))
        # Get row count, file count, and sizes from metadata (no per-file stat calls)
        arro3_table = dt.get_add_actions(flatten=True)
        add_actions = pa.table(arro3_table).to_pydict()
        row_count = sum(add_actions.get("num_records", [0]))
        total_size = sum(add_actions.get("size_bytes", [0]))
        num_files = len(add_actions.get("path", []))
        schema = dt.schema().to_arrow()
        return {
            "version": dt.version(),
            "num_files": num_files,
            "row_count": row_count,
            "columns": len(schema),
            "size_bytes": total_size,
            "schema": schema,
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
        # Delete stale generator metrics so the UI shows fresh progress
        gen_json = PROJECT_ROOT / "logs" / "generator_metrics.json"
        gen_json.unlink(missing_ok=True)
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

    _gen_is_running = gen["status"] == "running"

    if _gen_is_running:
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

    # ── Auto-refresh while generator is running ───────────
    if _gen_is_running:
        time.sleep(2)
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
        metrics_filename = {
            "streaming": "spark_stream_metrics.json",
            "account_upsert": "spark_account_metrics.json",
        }.get(name, "spark_metrics.json")
        max_cores_per_app = str(cfg.get("spark", {}).get("max_cores_per_app", cfg.get("spark", {}).get("worker_cores", 4)))
        cmd = [
            "docker", "exec", "-e", f"SPARK_METRICS_FILENAME={metrics_filename}", "spark-master",
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
            "--conf", f"spark.cores.max={max_cores_per_app}",
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
        # 1. Kill the app via Spark Master REST API
        try:
            cluster = get_spark_cluster_status()
            spark_host = os.environ.get("SPARK_MASTER_HOST", "localhost")
            if cluster["reachable"]:
                resp = requests.get(f"http://{spark_host}:8080/json/", timeout=3)
                for app in resp.json().get("activeapps", []):
                    app_name_lower = app["name"].lower()
                    if name.replace("_", "") in app_name_lower.replace("_", ""):
                        requests.post(f"http://{spark_host}:8080/app/kill/?id={app['id']}", timeout=5)
        except Exception:
            pass

        # 2. Kill the spark-submit process *inside* the container.
        #    The local subprocess is just a `docker exec` wrapper —
        #    terminating it does NOT stop the JVM inside the container.
        try:
            script_name = SPARK_JOBS.get(name, {}).get("script", "").split("/")[-1]
            if script_name:
                subprocess.run(
                    ["docker", "exec", "spark-master", "bash", "-c",
                     f"pkill -f '{script_name}' || true"],
                    timeout=10, capture_output=True,
                )
        except Exception:
            pass

        # 3. Kill the local docker-exec wrapper process
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

    # ── Rust Pipeline ─────────────────────────────────────
    st.markdown("---")
    st.subheader("🦀 Rust Pipeline")
    st.markdown(
        "Launch Rust pipeline jobs inside the `rust-pipeline` container for "
        "migration validation. Rust writes to separate `_rust` Delta tables."
    )

    RUST_JOBS = {
        "rust_stream": {
            "title": "📡 Rust Streaming (Kafka → Delta)",
            "desc": "Reads financial transactions from Kafka, applies the same 7 transforms "
                    "in Rust (rayon parallel), and writes to the `financial_transactions_rust` "
                    "Delta table via delta-rs.",
            "subcommand": "stream",
            "long_running": True,
        },
        "rust_upsert": {
            "title": "👤 Rust Account Upsert (Kafka → Delta MERGE)",
            "desc": "Reads account-update events from Kafka and upserts into `accounts_rust` "
                    "Delta table using delta-rs MERGE. Separate consumer group from Spark.",
            "subcommand": "upsert",
            "long_running": True,
        },
        "rust_validate": {
            "title": "✅ Validation (Spark vs Rust)",
            "desc": "Compares Spark and Rust Delta tables using DataFusion SQL — reports "
                    "row parity, numeric tolerance checks, and string equality. Results "
                    "appear on the Performance page.",
            "subcommand": "validate",
            "long_running": False,
        },
    }

    def _start_rust_job(name: str, subcommand: str):
        """Start a Rust pipeline job inside the rust-pipeline container."""
        cmd = [
            "docker", "exec", "rust-pipeline",
            "/usr/local/bin/rust-pipeline",
            "--config", "/opt/config/app_config.yaml",
            subcommand,
        ]
        log_path = PROJECT_ROOT / "logs" / f"{name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_ROOT),
            stdout=log_file, stderr=subprocess.STDOUT,
        )
        st.session_state.spark_processes[name] = proc
        return proc

    def _stop_rust_job(name: str):
        """Stop a running Rust pipeline job."""
        try:
            subprocess.run(
                ["docker", "exec", "rust-pipeline", "bash", "-c",
                 "pkill -f 'rust-pipeline' || true"],
                timeout=10, capture_output=True,
            )
        except Exception:
            pass
        proc = st.session_state.spark_processes.get(name)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        st.session_state.spark_processes.pop(name, None)

    for job_key, job_info in RUST_JOBS.items():
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
                        _stop_rust_job(job_key)
                        st.success(f"{job_info['title']} stopped.")
                        time.sleep(0.5)
                        st.rerun()
                else:
                    if st.button("▶️ Start", key=f"start_{job_key}", type="primary"):
                        _start_rust_job(job_key, job_info["subcommand"])
                        st.success(f"{job_info['title']} started!")
                        time.sleep(0.5)
                        st.rerun()
            with col3:
                log_path = PROJECT_ROOT / "logs" / f"{job_key}.log"
                if log_path.exists():
                    if st.button("📄 Logs", key=f"logs_{job_key}"):
                        st.session_state[f"show_logs_{job_key}"] = not st.session_state.get(f"show_logs_{job_key}", False)

            if st.session_state.get(f"show_logs_{job_key}", False):
                log_path = PROJECT_ROOT / "logs" / f"{job_key}.log"
                if log_path.exists():
                    log_content = log_path.read_text(errors="replace")
                    lines = log_content.strip().split("\n")
                    tail = "\n".join(lines[-100:])
                    st.code(tail, language="log")
                else:
                    st.info("No log file yet.")

    st.markdown("---")

    # ── Bulk Controls ─────────────────────────────────────
    st.subheader("Bulk Controls")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("▶️ Start Spark Streaming", type="primary"):
            for key in ["streaming", "account_upsert"]:
                if _check_proc(key) != "running":
                    _start_spark_job(key, SPARK_JOBS[key]["script"])
            st.success("Spark streaming jobs started!")
            time.sleep(0.5)
            st.rerun()
    with col2:
        if st.button("🦀 Start Rust Streaming", type="primary"):
            for key in ["rust_stream", "rust_upsert"]:
                if _check_proc(key) != "running":
                    _start_rust_job(key, RUST_JOBS[key]["subcommand"])
            st.success("Rust streaming jobs started!")
            time.sleep(0.5)
            st.rerun()
    with col3:
        if st.button("⏹️ Stop All Jobs"):
            for key in SPARK_JOBS:
                _stop_spark_job(key)
            for key in RUST_JOBS:
                _stop_rust_job(key)
            st.success("All jobs stopped.")
            time.sleep(0.5)
            st.rerun()

    # ── Live Pipeline Metrics ─────────────────────────────
    st.markdown("---")
    st.subheader("📊 Live Pipeline Metrics")

    _jobs_metrics = get_metrics()
    _spark_stream_m = _jobs_metrics.get("spark_stream", _jobs_metrics["spark"])
    _spark_account_m = _jobs_metrics.get("spark_account", {})
    _rust_stream_m = _jobs_metrics.get("rust_stream", _jobs_metrics.get("rust", {}))
    _rust_account_m = _jobs_metrics.get("rust_account", {})

    _any_job_running = any(
        _check_proc(k) == "running"
        for k in list(SPARK_JOBS.keys()) + list(RUST_JOBS.keys())
    )

    if any(m.get("micro_batches_completed", 0) > 0 for m in [_spark_stream_m, _spark_account_m, _rust_stream_m, _rust_account_m]):
        col_s, col_r = st.columns(2)
        with col_s:
            st.markdown("**⚡ Spark Streaming**")
            s_status = _spark_stream_m.get("status", "idle")
            s_icon = {"running": "🟢", "idle": "⚪", "completed": "✅"}.get(s_status, "🔴")
            st.write(f"Status: {s_icon} **{s_status.upper()}**")
            ss1, ss2, ss3 = st.columns(3)
            ss1.metric("Batches", f"{_spark_stream_m['micro_batches_completed']:,}")
            ss2.metric("Rows", f"{_spark_stream_m['total_rows_processed']:,}")
            ss3.metric("Avg Batch", f"{_spark_stream_m['avg_batch_duration_ms']:.0f} ms")
        with col_r:
            st.markdown("**🦀 Rust Streaming**")
            r_status = _rust_stream_m.get("status", "idle")
            r_icon = {"running": "🟢", "idle": "⚪", "completed": "✅"}.get(r_status, "🔴")
            st.write(f"Status: {r_icon} **{r_status.upper()}**")
            rr1, rr2, rr3 = st.columns(3)
            rr1.metric("Batches", f"{_rust_stream_m.get('micro_batches_completed', 0):,}")
            rr2.metric("Rows", f"{_rust_stream_m.get('total_rows_processed', 0):,}")
            rr3.metric("Avg Batch", f"{_rust_stream_m.get('avg_batch_duration_ms', 0):.0f} ms")

        if _spark_account_m.get("micro_batches_completed", 0) > 0 or _rust_account_m.get("micro_batches_completed", 0) > 0:
            st.markdown("##### 👤 Account Upsert Metrics")
            col_sa, col_ra = st.columns(2)
            with col_sa:
                st.markdown("**⚡ Spark Account Upsert**")
                sa1, sa2, sa3 = st.columns(3)
                sa1.metric("Batches", f"{_spark_account_m.get('micro_batches_completed', 0):,}")
                sa2.metric("Rows", f"{_spark_account_m.get('total_rows_processed', 0):,}")
                sa3.metric("Avg Batch", f"{_spark_account_m.get('avg_batch_duration_ms', 0):.0f} ms")
            with col_ra:
                st.markdown("**🦀 Rust Account Upsert**")
                ra1, ra2, ra3 = st.columns(3)
                ra1.metric("Batches", f"{_rust_account_m.get('micro_batches_completed', 0):,}")
                ra2.metric("Rows", f"{_rust_account_m.get('total_rows_processed', 0):,}")
                ra3.metric("Avg Batch", f"{_rust_account_m.get('avg_batch_duration_ms', 0):.0f} ms")
    elif _any_job_running:
        st.info("Pipeline jobs are running — metrics will appear after the first batch completes.")
    else:
        st.info("Start a pipeline job above to see live metrics.")

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
        ("Rust Txn checkpoint", Path(cfg.get("rust", {}).get("local_checkpoint_db", "./checkpoints/rust_financial_txn/offsets.db")).parent),
        ("Rust Acct checkpoint", Path(cfg.get("rust", {}).get("local_accounts_checkpoint_db", "./checkpoints/rust_account_upsert/offsets.db")).parent),
    ]
    _delta_paths = [
        ("Transactions Delta", Path(cfg["delta"].get("table_path", "./delta_output/financial_transactions"))),
        ("Accounts Delta", Path(cfg["delta"].get("accounts_table_path", "./delta_output/accounts"))),
        ("Rust Transactions Delta", Path(cfg.get("rust", {}).get("local_delta_output", "./delta_output/financial_transactions_rust"))),
        ("Rust Accounts Delta", Path(cfg.get("rust", {}).get("local_accounts_delta_output", "./delta_output/accounts_rust"))),
    ]

    def _stop_all_jobs():
        for key in SPARK_JOBS:
            _stop_spark_job(key)
        for key in RUST_JOBS:
            _stop_rust_job(key)

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

    # ── Auto-refresh while any pipeline job is running ────
    if _any_job_running:
        time.sleep(3)
        st.rerun()


# ══════════════════════════════════════════════════════════════
# PAGE: Delta Explorer
# ══════════════════════════════════════════════════════════════

elif page == "🔍 Delta Explorer":
    st.title("🔍 Delta Table Explorer")

    table_choice = st.selectbox(
        "Select Table",
        ["Transactions", "Accounts", "Transactions (Rust)", "Accounts (Rust)"],
    )

    _table_path_map = {
        "Transactions": cfg["delta"]["table_path"],
        "Accounts": cfg["delta"]["accounts_table_path"],
        "Transactions (Rust)": cfg.get("rust", {}).get("local_delta_output", "./delta_output/financial_transactions_rust"),
        "Accounts (Rust)": cfg.get("rust", {}).get("local_accounts_delta_output", "./delta_output/accounts_rust"),
    }
    table_path = _table_path_map[table_choice]

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
        st.dataframe(pd.DataFrame(schema_data), width="stretch", hide_index=True)

        # ── Sample Data ───────────────────────────────────
        st.subheader("Sample Data")
        num_rows = st.slider("Number of rows", 5, 500, 20)
        try:
            from deltalake import DeltaTable
            dt = DeltaTable(table_path)
            sample_df = dt.to_pandas().head(num_rows)
            st.dataframe(sample_df, width="stretch")
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
                    st.dataframe(result_df, width="stretch")
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
                st.dataframe(hist_df[display_cols].head(20), width="stretch")
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
    spark = metrics.get("spark_stream", metrics["spark"])
    rust = metrics.get("rust_stream", metrics.get("rust", {}))

    # ── Quick Spark vs Rust Summary (always visible at top) ──
    _rs = rust.get("micro_batches_completed", 0)
    _ss = spark.get("micro_batches_completed", 0)
    if _rs > 0 or _ss > 0:
        st.markdown("---")
        st.subheader("🦀 Spark vs Rust — Quick Summary")
        _qs1, _qs2, _qs3 = st.columns(3)
        _qs1.metric("Spark Batches", f"{_ss:,}")
        _qs2.metric("Rust Batches", f"{_rs:,}")
        _spark_avg = spark.get("avg_batch_duration_ms", 0)
        _rust_bd = rust.get("batch_details", [])
        _rust_avg = (sum(b.get("wall_ms", 0) for b in _rust_bd) / len(_rust_bd)) if _rust_bd else 0
        if _spark_avg > 0 and _rust_avg > 0:
            _ratio = _spark_avg / _rust_avg
            _qs3.metric("Rust Speedup", f"{_ratio:.1f}x",
                        delta=f"{'Rust' if _ratio > 1 else 'Spark'} faster")
        else:
            _qs3.metric("Rust Speedup", "—")
        st.info("📊 Scroll down to **'Comprehensive Batch Run Statistics'** for the full comparison table with per-category winners.")
        st.markdown("---")

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
                width="stretch",
            )
        with col2:
            st.caption("Kafka Publish Latency (ms) — flush() time")
            if gen["publish_latencies_ms"]:
                st.line_chart(
                    pd.DataFrame({"publish_latency_ms": gen["publish_latencies_ms"]}),
                    width="stretch",
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
        st.dataframe(summary, width="stretch", hide_index=True)
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
                width="stretch",
            )
        with col2:
            st.caption("Transform Time (ms)")
            if spark["transform_times_ms"]:
                st.line_chart(
                    pd.DataFrame({"transform_ms": spark["transform_times_ms"]}),
                    width="stretch",
                )
        with col3:
            st.caption("Delta Write Time (ms)")
            if spark["delta_write_times_ms"]:
                st.line_chart(
                    pd.DataFrame({"write_ms": spark["delta_write_times_ms"]}),
                    width="stretch",
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
        st.dataframe(spark_summary, width="stretch", hide_index=True)

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
            st.area_chart(breakdown_df, width="stretch")

            # Totals
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("Total Transform Time", fmt_duration(total_transform_ms / 1000))
            tc2.metric("Total Delta Write Time", fmt_duration(total_write_ms / 1000))
            pct_transform = total_transform_ms / max(1, total_processing_ms) * 100
            pct_write = total_write_ms / max(1, total_processing_ms) * 100
            tc3.metric("Transform / Write Ratio", f"{pct_transform:.0f}% / {pct_write:.0f}%")

        st.metric("Total Rows Processed", f"{spark['total_rows_processed']:,}")
        st.metric("Last Batch Rows", f"{spark['last_batch_rows']:,}")

        # ── Detailed Execution Metrics (for migration evaluation) ──
        st.markdown("---")
        st.markdown("##### 🔬 Execution Metrics (Migration Baseline)")

        batch_details = spark.get("batch_details", [])
        executor_data = spark.get("executor", {})

        if executor_data:
            st.markdown("###### Executor Summary (cumulative)")
            ex1, ex2, ex3, ex4 = st.columns(4)
            ex1.metric("Total Tasks", f"{executor_data.get('total_tasks', 0):,}")
            ex2.metric("Active Cores", f"{executor_data.get('active_cores', 0)}")
            ex3.metric("Peak JVM Heap", f"{executor_data.get('peak_memory_mb', 0):.0f} MB")
            ex4.metric("Max Memory", f"{executor_data.get('max_memory_mb', 0):.0f} MB")

            ex5, ex6, ex7, ex8 = st.columns(4)
            total_dur = executor_data.get("total_duration_ms", 0)
            ex5.metric("Total Task Time", fmt_duration(total_dur / 1000) if total_dur else "—")
            ex6.metric("Total GC Time", f"{executor_data.get('total_gc_ms', 0):,.0f} ms")
            ex7.metric("GC Overhead", f"{executor_data.get('gc_pct', 0):.1f}%")
            total_shuf = executor_data.get("total_shuffle_read_mb", 0) + executor_data.get("total_shuffle_write_mb", 0)
            ex8.metric("Total Shuffle", f"{total_shuf:.1f} MB")

            io1, io2, io3, io4 = st.columns(4)
            io1.metric("Total Input", f"{executor_data.get('total_input_mb', 0):.1f} MB")
            io2.metric("Total Output", f"{executor_data.get('total_output_mb', 0):.1f} MB")
            io3.metric("Shuffle Read", f"{executor_data.get('total_shuffle_read_mb', 0):.1f} MB")
            io4.metric("Shuffle Write", f"{executor_data.get('total_shuffle_write_mb', 0):.1f} MB")

        if batch_details:
            st.markdown("###### Per-Batch Breakdown")

            detail_df = pd.DataFrame(batch_details)
            # Display key columns
            display_cols = [
                "batch_id", "rows", "wall_ms", "cpu_ms", "gc_ms",
                "cpu_util_pct", "gc_pct", "input_mb", "output_mb",
                "shuffle_read_mb", "shuffle_write_mb", "peak_memory_mb", "tasks",
            ]
            available = [c for c in display_cols if c in detail_df.columns]
            if available:
                rename_map = {
                    "batch_id": "Batch",
                    "rows": "Rows",
                    "wall_ms": "Wall (ms)",
                    "cpu_ms": "CPU (ms)",
                    "gc_ms": "GC (ms)",
                    "cpu_util_pct": "CPU %",
                    "gc_pct": "GC %",
                    "input_mb": "In (MB)",
                    "output_mb": "Out (MB)",
                    "shuffle_read_mb": "Shuf R (MB)",
                    "shuffle_write_mb": "Shuf W (MB)",
                    "peak_memory_mb": "Peak Mem (MB)",
                    "tasks": "Tasks",
                }
                show_df = detail_df[available].rename(columns=rename_map)
                st.dataframe(show_df, width="stretch", hide_index=True)

            # Charts for CPU, GC, Memory across batches
            ch1, ch2, ch3 = st.columns(3)
            with ch1:
                st.caption("CPU Time vs GC Time (ms)")
                if "cpu_ms" in detail_df.columns and "gc_ms" in detail_df.columns:
                    st.bar_chart(detail_df[["cpu_ms", "gc_ms"]].rename(
                        columns={"cpu_ms": "CPU", "gc_ms": "GC"}), width="stretch")
            with ch2:
                st.caption("CPU Utilization (%)")
                if "cpu_util_pct" in detail_df.columns:
                    st.line_chart(detail_df["cpu_util_pct"], width="stretch")
            with ch3:
                st.caption("Peak Memory (MB)")
                if "peak_memory_mb" in detail_df.columns:
                    st.area_chart(detail_df["peak_memory_mb"], width="stretch")

            # Shuffle charts
            if "shuffle_read_mb" in detail_df.columns:
                sc1, sc2 = st.columns(2)
                with sc1:
                    st.caption("Shuffle I/O (MB)")
                    st.bar_chart(detail_df[["shuffle_read_mb", "shuffle_write_mb"]].rename(
                        columns={"shuffle_read_mb": "Read", "shuffle_write_mb": "Write"}),
                        width="stretch")
                with sc2:
                    st.caption("I/O (MB)")
                    st.bar_chart(detail_df[["input_mb", "output_mb"]].rename(
                        columns={"input_mb": "Input", "output_mb": "Output"}),
                        width="stretch")

            # Migration evaluation summary
            if len(batch_details) >= 2:
                st.markdown("###### 📊 Migration Evaluation Summary")
                avg_cpu = detail_df["cpu_ms"].mean() if "cpu_ms" in detail_df.columns else 0
                avg_gc = detail_df["gc_ms"].mean() if "gc_ms" in detail_df.columns else 0
                avg_util = detail_df["cpu_util_pct"].mean() if "cpu_util_pct" in detail_df.columns else 0
                avg_shuf_r = detail_df["shuffle_read_mb"].mean() if "shuffle_read_mb" in detail_df.columns else 0
                avg_shuf_w = detail_df["shuffle_write_mb"].mean() if "shuffle_write_mb" in detail_df.columns else 0
                avg_peak = detail_df["peak_memory_mb"].mean() if "peak_memory_mb" in detail_df.columns else 0
                avg_rows = detail_df["rows"].mean() if "rows" in detail_df.columns else 0

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Avg CPU / Batch", f"{avg_cpu:.0f} ms")
                m2.metric("Avg GC / Batch", f"{avg_gc:.0f} ms ({100*avg_gc/max(1,avg_cpu):.0f}% of CPU)")
                m3.metric("Avg CPU Utilization", f"{avg_util:.1f}%",
                          help="Low % means idle time (likely Python UDF serialization overhead)")
                m4.metric("Avg Peak Memory", f"{avg_peak:.0f} MB")

                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Avg Shuffle Read", f"{avg_shuf_r:.2f} MB / batch")
                s2.metric("Avg Shuffle Write", f"{avg_shuf_w:.2f} MB / batch")
                s3.metric("Throughput", f"{avg_rows / max(1, detail_df['wall_ms'].mean()) * 1000:.0f} rows/sec"
                          if "wall_ms" in detail_df.columns else "—")
                s4.metric("Memory Efficiency",
                          f"{avg_rows / max(1, avg_peak) * 1000:.0f} rows/GB"
                          if avg_peak > 0 else "—",
                          help="Higher = more memory-efficient")

        elif not batch_details and not executor_data:
            st.info("Detailed metrics will appear when the streaming job runs. "
                    "The Spark REST API is queried after each micro-batch.")
    else:
        st.info("No Spark metrics yet. Run the streaming job to collect data.")

    # ── Delta Table Statistics (always visible) ─────────────
    st.markdown("---")
    st.subheader("📁 Delta Table Statistics")

    _all_tables = {
        "Transactions (Spark)": cfg["delta"]["table_path"],
        "Transactions (Rust)": cfg.get("rust", {}).get("local_delta_output", "./delta_output/financial_transactions_rust"),
        "Accounts (Spark)": cfg["delta"]["accounts_table_path"],
        "Accounts (Rust)": cfg.get("rust", {}).get("local_accounts_delta_output", "./delta_output/accounts_rust"),
    }

    table_stats = {}
    for label, path in _all_tables.items():
        info = get_delta_table_info(path)
        if info and "error" not in info:
            table_stats[label] = info

    if table_stats:
        cols = st.columns(len(table_stats))
        for i, (label, info) in enumerate(table_stats.items()):
            with cols[i]:
                engine = "🦀" if "Rust" in label else "⚡"
                st.markdown(f"**{engine} {label}**")
                st.metric("Rows", f"{info['row_count']:,}")
                st.metric("Files", f"{info['num_files']:,}")
                st.metric("Size", fmt_bytes(info["size_bytes"]))
                st.metric("Version", info["version"])
                if info.get("partition_columns"):
                    st.caption(f"Partitions: {', '.join(info['partition_columns'])}")

        # ── Schema Comparison ─────────────────────────────
        spark_txn = table_stats.get("Transactions (Spark)")
        rust_txn = table_stats.get("Transactions (Rust)")
        if spark_txn and rust_txn:
            st.markdown("##### 📋 Schema Comparison: Transactions")
            spark_cols = {f.name: str(f.type) for f in spark_txn["schema"]}
            rust_cols = {f.name: str(f.type) for f in rust_txn["schema"]}
            all_cols = sorted(set(spark_cols.keys()) | set(rust_cols.keys()))
            schema_rows = []
            for col in all_cols:
                s_type = spark_cols.get(col, "—")
                r_type = rust_cols.get(col, "—")
                match = "✅" if s_type == r_type else ("⚠️" if s_type != "—" and r_type != "—" else "❌")
                schema_rows.append({"Column": col, "Spark Type": s_type, "Rust Type": r_type, "Match": match})
            st.dataframe(pd.DataFrame(schema_rows), width="stretch", hide_index=True)

        # ── Row Count Parity ─────────────────────────────
        pairs = [("Transactions (Spark)", "Transactions (Rust)"), ("Accounts (Spark)", "Accounts (Rust)")]
        parity_rows = []
        for spark_label, rust_label in pairs:
            si = table_stats.get(spark_label)
            ri = table_stats.get(rust_label)
            if si or ri:
                spark_rc = si["row_count"] if si else 0
                rust_rc = ri["row_count"] if ri else 0
                diff = abs(spark_rc - rust_rc)
                pct = (min(spark_rc, rust_rc) / max(spark_rc, rust_rc) * 100) if max(spark_rc, rust_rc) > 0 else 0
                status = "✅ Match" if diff == 0 else (f"⚠️ Δ{diff:,}" if pct > 95 else f"❌ Δ{diff:,}")
                parity_rows.append({
                    "Table": spark_label.replace(" (Spark)", ""),
                    "Spark Rows": f"{spark_rc:,}" if si else "—",
                    "Rust Rows": f"{rust_rc:,}" if ri else "—",
                    "Parity": f"{pct:.1f}%",
                    "Status": status,
                })
        if parity_rows:
            st.markdown("##### 🔄 Row Count Parity")
            st.dataframe(pd.DataFrame(parity_rows), width="stretch", hide_index=True)

        # ── Data Sample Comparison ────────────────────────
        if rust_txn:
            with st.expander("🔍 Sample Data Comparison (Transactions)"):
                try:
                    from deltalake import DeltaTable
                    sample_dfs = {}
                    for label, path in [("Spark", cfg["delta"]["table_path"]),
                                         ("Rust", cfg.get("rust", {}).get("local_delta_output", ""))]:
                        try:
                            dt = DeltaTable(path)
                            sample_dfs[label] = dt.to_pyarrow_dataset().head(5).to_pandas()
                        except Exception:
                            pass
                    if sample_dfs:
                        for label, df in sample_dfs.items():
                            st.caption(f"**{label}** (first 5 rows)")
                            st.dataframe(df, width="stretch")
                except Exception as e:
                    st.error(f"Error loading samples: {e}")
    else:
        st.info("No Delta tables found. Run the pipeline to generate data.")

    # ── Spark vs Rust Runtime Comparison ──────────────────
    st.markdown("---")
    st.subheader("🦀 Spark vs Rust — Batch Run Statistics")

    rust = metrics.get("rust", {})
    validation = metrics.get("validation", {})

    # Helper: compute per-engine stats from batch_details
    def _engine_stats(engine_data: dict, label: str) -> dict:
        """Build a comprehensive stats dict from engine metrics."""
        bd = engine_data.get("batch_details", [])
        durations = engine_data.get("batch_durations_ms", [])
        transforms = engine_data.get("transform_times_ms", [])
        writes = engine_data.get("delta_write_times_ms", [])
        executor = engine_data.get("executor", {})

        total_wall_ms = sum(d.get("wall_ms", 0) for d in bd) if bd else sum(durations)
        total_transform_ms = sum(d.get("transform_ms", d.get("cpu_ms", 0)) for d in bd) if bd else sum(transforms)
        total_write_ms = sum(d.get("write_ms", 0) for d in bd) if bd else sum(writes)
        total_rows = engine_data.get("total_rows_processed", 0)
        n_batches = engine_data.get("micro_batches_completed", 0)

        # Kafka read time = wall time - transform - write (approximate)
        total_kafka_ms = max(0, total_wall_ms - total_transform_ms - total_write_ms)

        avg_cpu_util = 0.0
        avg_peak_mem = 0.0
        total_gc_ms = 0.0
        total_shuffle_read = 0.0
        total_shuffle_write = 0.0
        active_cores = executor.get("active_cores", 0)
        total_tasks = executor.get("total_tasks", 0)

        if bd:
            cpu_utils = [d.get("cpu_util_pct", 0) for d in bd if d.get("cpu_util_pct") is not None]
            avg_cpu_util = sum(cpu_utils) / len(cpu_utils) if cpu_utils else 0.0
            mems = [d.get("peak_memory_mb", 0) for d in bd if d.get("peak_memory_mb") is not None]
            avg_peak_mem = sum(mems) / len(mems) if mems else 0.0
            total_gc_ms = sum(d.get("gc_ms", 0) for d in bd)
            total_shuffle_read = sum(d.get("shuffle_read_mb", 0) for d in bd)
            total_shuffle_write = sum(d.get("shuffle_write_mb", 0) for d in bd)
            if not active_cores:
                tasks_list = [d.get("tasks", 0) for d in bd if d.get("tasks")]
                active_cores = max(tasks_list) if tasks_list else 0
            if not total_tasks:
                total_tasks = sum(d.get("tasks", 0) for d in bd)

        throughput = (total_rows / (total_wall_ms / 1000)) if total_wall_ms > 0 else 0

        return {
            "n_batches": n_batches,
            "total_rows": total_rows,
            "total_wall_ms": total_wall_ms,
            "total_transform_ms": total_transform_ms,
            "total_write_ms": total_write_ms,
            "total_kafka_ms": total_kafka_ms,
            "avg_batch_ms": (total_wall_ms / n_batches) if n_batches else 0,
            "avg_transform_ms": (total_transform_ms / n_batches) if n_batches else 0,
            "avg_write_ms": (total_write_ms / n_batches) if n_batches else 0,
            "avg_kafka_ms": (total_kafka_ms / n_batches) if n_batches else 0,
            "avg_cpu_util": avg_cpu_util,
            "avg_peak_mem_mb": avg_peak_mem,
            "total_gc_ms": total_gc_ms,
            "gc_pct": (total_gc_ms / total_wall_ms * 100) if total_wall_ms > 0 else 0,
            "total_shuffle_read_mb": total_shuffle_read,
            "total_shuffle_write_mb": total_shuffle_write,
            "active_cores": active_cores,
            "total_tasks": total_tasks,
            "throughput_rows_sec": throughput,
        }

    spark_s = _engine_stats(spark, "Spark")
    rust_s = _engine_stats(rust, "Rust")

    has_spark = spark_s["n_batches"] > 0
    has_rust = rust_s["n_batches"] > 0

    if has_spark or has_rust:

        # ── Speedup Banner ────────────────────────────────
        if has_spark and has_rust and spark_s["avg_batch_ms"] > 0 and rust_s["avg_batch_ms"] > 0:
            speedup = spark_s["avg_batch_ms"] / rust_s["avg_batch_ms"]
            delta_label = (f"{(speedup - 1) * 100:.0f}% faster"
                           if speedup > 1
                           else f"{(1 - speedup) * 100:.0f}% slower")
            st.metric("🏎️ Rust Batch Speedup", f"{speedup:.2f}x", delta=delta_label)

        # ── Comprehensive Statistics Table ────────────────
        st.markdown("##### 📊 Comprehensive Batch Run Statistics")

        def _fmt_ms(v):
            if v >= 60000:
                return f"{v / 60000:.1f} min"
            if v >= 1000:
                return f"{v / 1000:.1f} s"
            return f"{v:.1f} ms"

        def _fmt_val(v, fmt="ms"):
            if fmt == "ms":
                return _fmt_ms(v) if v else "—"
            if fmt == "int":
                return f"{int(v):,}" if v else "—"
            if fmt == "pct":
                return f"{v:.1f}%" if v else "—"
            if fmt == "mb":
                return f"{v:.1f} MB" if v else "—"
            if fmt == "rows_sec":
                return f"{v:,.0f}" if v else "—"
            return str(v) if v else "—"

        def _winner(s_val, r_val, metric_type="lower_better"):
            """Determine winner. lower_better for times, higher_better for throughput."""
            if not s_val or not r_val:
                return "—"
            if metric_type == "lower_better":
                return "🦀 Rust" if r_val < s_val else ("⚡ Spark" if s_val < r_val else "Tie")
            else:
                return "🦀 Rust" if r_val > s_val else ("⚡ Spark" if s_val > r_val else "Tie")

        # Build the rows
        stats_rows = [
            # ── Batch Overview ──
            {"Category": "📋 Batch Overview", "Metric": "Total Batches Completed",
             "Spark (⚡)": _fmt_val(spark_s["n_batches"], "int"),
             "Rust (🦀)": _fmt_val(rust_s["n_batches"], "int"), "Winner": "—"},
            {"Category": "", "Metric": "Total Rows Processed",
             "Spark (⚡)": _fmt_val(spark_s["total_rows"], "int"),
             "Rust (🦀)": _fmt_val(rust_s["total_rows"], "int"), "Winner": "—"},
            {"Category": "", "Metric": "Throughput (rows/sec)",
             "Spark (⚡)": _fmt_val(spark_s["throughput_rows_sec"], "rows_sec"),
             "Rust (🦀)": _fmt_val(rust_s["throughput_rows_sec"], "rows_sec"),
             "Winner": _winner(spark_s["throughput_rows_sec"], rust_s["throughput_rows_sec"], "higher_better")},

            # ── End-to-End Timing ──
            {"Category": "⏱️ End-to-End Timing", "Metric": "Total Batch Wall-Clock Time",
             "Spark (⚡)": _fmt_val(spark_s["total_wall_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["total_wall_ms"]),
             "Winner": _winner(spark_s["total_wall_ms"], rust_s["total_wall_ms"])},
            {"Category": "", "Metric": "Avg Batch Duration",
             "Spark (⚡)": _fmt_val(spark_s["avg_batch_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["avg_batch_ms"]),
             "Winner": _winner(spark_s["avg_batch_ms"], rust_s["avg_batch_ms"])},

            # ── Kafka Read ──
            {"Category": "📨 Kafka Read", "Metric": "Total Kafka Read Time",
             "Spark (⚡)": _fmt_val(spark_s["total_kafka_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["total_kafka_ms"]),
             "Winner": _winner(spark_s["total_kafka_ms"], rust_s["total_kafka_ms"])},
            {"Category": "", "Metric": "Avg Kafka Read / Batch",
             "Spark (⚡)": _fmt_val(spark_s["avg_kafka_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["avg_kafka_ms"]),
             "Winner": _winner(spark_s["avg_kafka_ms"], rust_s["avg_kafka_ms"])},

            # ── Transform ──
            {"Category": "🔄 Transform (CPU)", "Metric": "Total Transform Time",
             "Spark (⚡)": _fmt_val(spark_s["total_transform_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["total_transform_ms"]),
             "Winner": _winner(spark_s["total_transform_ms"], rust_s["total_transform_ms"])},
            {"Category": "", "Metric": "Avg Transform / Batch",
             "Spark (⚡)": _fmt_val(spark_s["avg_transform_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["avg_transform_ms"]),
             "Winner": _winner(spark_s["avg_transform_ms"], rust_s["avg_transform_ms"])},

            # ── Delta Lake Write ──
            {"Category": "💾 Delta Lake Write", "Metric": "Total Delta Write Time",
             "Spark (⚡)": _fmt_val(spark_s["total_write_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["total_write_ms"]),
             "Winner": _winner(spark_s["total_write_ms"], rust_s["total_write_ms"])},
            {"Category": "", "Metric": "Avg Delta Write / Batch",
             "Spark (⚡)": _fmt_val(spark_s["avg_write_ms"]),
             "Rust (🦀)": _fmt_val(rust_s["avg_write_ms"]),
             "Winner": _winner(spark_s["avg_write_ms"], rust_s["avg_write_ms"])},

            # ── Memory Footprint ──
            {"Category": "🧠 Memory Footprint", "Metric": "Avg Peak Memory / Batch",
             "Spark (⚡)": _fmt_val(spark_s["avg_peak_mem_mb"], "mb"),
             "Rust (🦀)": _fmt_val(rust_s["avg_peak_mem_mb"], "mb"),
             "Winner": _winner(spark_s["avg_peak_mem_mb"], rust_s["avg_peak_mem_mb"])},
            {"Category": "", "Metric": "Memory Efficiency (rows/GB)",
             "Spark (⚡)": f"{spark_s['total_rows'] / max(1, spark_s['avg_peak_mem_mb']) * 1000:.0f}" if spark_s["avg_peak_mem_mb"] else "—",
             "Rust (🦀)": f"{rust_s['total_rows'] / max(1, rust_s['avg_peak_mem_mb']) * 1000:.0f}" if rust_s["avg_peak_mem_mb"] else "—",
             "Winner": _winner(
                 spark_s['total_rows'] / max(1, spark_s['avg_peak_mem_mb']) if spark_s["avg_peak_mem_mb"] else 0,
                 rust_s['total_rows'] / max(1, rust_s['avg_peak_mem_mb']) if rust_s["avg_peak_mem_mb"] else 0,
                 "higher_better")},

            # ── CPU Utilization ──
            {"Category": "⚙️ CPU Utilization", "Metric": "Avg CPU Utilization",
             "Spark (⚡)": _fmt_val(spark_s["avg_cpu_util"], "pct"),
             "Rust (🦀)": _fmt_val(rust_s["avg_cpu_util"], "pct"),
             "Winner": _winner(spark_s["avg_cpu_util"], rust_s["avg_cpu_util"], "higher_better")},
            {"Category": "", "Metric": "Active Cores / Threads",
             "Spark (⚡)": _fmt_val(spark_s["active_cores"], "int"),
             "Rust (🦀)": _fmt_val(rust_s["active_cores"], "int"), "Winner": "—"},

            # ── Thread Pool / Task Utilization ──
            {"Category": "🧵 Thread Pool", "Metric": "Total Tasks Executed",
             "Spark (⚡)": _fmt_val(spark_s["total_tasks"], "int"),
             "Rust (🦀)": _fmt_val(rust_s["total_tasks"], "int"), "Winner": "—"},
            {"Category": "", "Metric": "Avg Tasks / Batch",
             "Spark (⚡)": f"{spark_s['total_tasks'] / max(1, spark_s['n_batches']):.0f}" if spark_s["n_batches"] else "—",
             "Rust (🦀)": f"{rust_s['total_tasks'] / max(1, rust_s['n_batches']):.0f}" if rust_s["n_batches"] else "—",
             "Winner": "—"},

            # ── GC / Pause Time ──
            {"Category": "🗑️ GC / Pause Time", "Metric": "Total GC Time",
             "Spark (⚡)": _fmt_val(spark_s["total_gc_ms"]),
             "Rust (🦀)": "0 ms (no GC)",
             "Winner": "🦀 Rust" if spark_s["total_gc_ms"] > 0 else "Tie"},
            {"Category": "", "Metric": "GC Overhead (% of wall time)",
             "Spark (⚡)": _fmt_val(spark_s["gc_pct"], "pct"),
             "Rust (🦀)": "0.0%",
             "Winner": "🦀 Rust" if spark_s["gc_pct"] > 0 else "Tie"},

            # ── Shuffle (Spark-specific) ──
            {"Category": "🔀 Shuffle (Spark)", "Metric": "Total Shuffle Read",
             "Spark (⚡)": _fmt_val(spark_s["total_shuffle_read_mb"], "mb"),
             "Rust (🦀)": "N/A (no shuffle)",
             "Winner": "🦀 Rust" if spark_s["total_shuffle_read_mb"] > 0 else "—"},
            {"Category": "", "Metric": "Total Shuffle Write",
             "Spark (⚡)": _fmt_val(spark_s["total_shuffle_write_mb"], "mb"),
             "Rust (🦀)": "N/A (no shuffle)",
             "Winner": "🦀 Rust" if spark_s["total_shuffle_write_mb"] > 0 else "—"},

            # ── Serialization Overhead ──
            {"Category": "📦 Serialization", "Metric": "Serialization Model",
             "Spark (⚡)": "Catalyst → Tungsten binary",
             "Rust (🦀)": "serde → Arrow IPC (zero-copy)", "Winner": "—"},
            {"Category": "", "Metric": "Data Format",
             "Spark (⚡)": "JVM objects → Parquet",
             "Rust (🦀)": "Rust structs → Arrow → Parquet", "Winner": "—"},

            # ── Startup & JVM Overhead ──
            {"Category": "🚀 Runtime Overhead", "Metric": "Runtime Model",
             "Spark (⚡)": "JVM (HotSpot) + Python UDFs",
             "Rust (🦀)": "Native binary (AOT compiled)", "Winner": "—"},
            {"Category": "", "Metric": "Warm-up Required",
             "Spark (⚡)": "Yes (JIT compilation)",
             "Rust (🦀)": "No (pre-compiled)", "Winner": "🦀 Rust"},
        ]

        stats_df = pd.DataFrame(stats_rows)
        st.dataframe(
            stats_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Category": st.column_config.TextColumn(width="medium"),
                "Metric": st.column_config.TextColumn(width="large"),
                "Spark (⚡)": st.column_config.TextColumn(width="medium"),
                "Rust (🦀)": st.column_config.TextColumn(width="medium"),
                "Winner": st.column_config.TextColumn(width="small"),
            },
        )

        # ── Batch Duration Overlay Chart ──────────────────
        rust_bd = rust.get("batch_durations_ms", [])
        spark_bd = spark.get("batch_durations_ms", [])
        if rust_bd or spark_bd:
            st.markdown("##### Batch Duration Comparison (ms)")
            max_len = max(len(spark_bd), len(rust_bd)) if (spark_bd or rust_bd) else 0
            chart_data = {}
            if spark_bd:
                chart_data["Spark"] = spark_bd + [None] * (max_len - len(spark_bd))
            if rust_bd:
                chart_data["Rust"] = rust_bd + [None] * (max_len - len(rust_bd))
            if chart_data:
                st.line_chart(pd.DataFrame(chart_data), width="stretch")

        # ── Time breakdown stacked chart ──────────────────
        if has_spark and has_rust:
            st.markdown("##### ⏱️ Time Breakdown: Where Time Is Spent")
            breakdown_data = {
                "Stage": ["Kafka Read", "Transform (CPU)", "Delta Write", "GC Overhead"],
                "Spark (ms)": [spark_s["total_kafka_ms"], spark_s["total_transform_ms"],
                               spark_s["total_write_ms"], spark_s["total_gc_ms"]],
                "Rust (ms)": [rust_s["total_kafka_ms"], rust_s["total_transform_ms"],
                              rust_s["total_write_ms"], 0],
            }
            bd_df = pd.DataFrame(breakdown_data)
            st.bar_chart(bd_df.set_index("Stage"), width="stretch")

        # ── Per-Batch Detail Tables ───────────────────────
        with st.expander("📋 Spark Per-Batch Details"):
            batch_details = spark.get("batch_details", [])
            if batch_details:
                detail_df = pd.DataFrame(batch_details)
                display_cols = [
                    "batch_id", "rows", "wall_ms", "transform_ms", "write_ms",
                    "cpu_ms", "gc_ms", "cpu_util_pct", "gc_pct",
                    "shuffle_read_mb", "shuffle_write_mb", "peak_memory_mb", "tasks",
                ]
                available = [c for c in display_cols if c in detail_df.columns]
                if available:
                    st.dataframe(detail_df[available], width="stretch", hide_index=True)
            else:
                st.info("No Spark batch details available.")

        with st.expander("📋 Rust Per-Batch Details"):
            rust_details = rust.get("batch_details", [])
            if rust_details:
                rdf = pd.DataFrame(rust_details)
                r_display = [c for c in ["batch_id", "rows", "wall_ms", "transform_ms", "write_ms",
                                          "cpu_ms", "peak_memory_mb", "tasks"] if c in rdf.columns]
                if r_display:
                    st.dataframe(rdf[r_display], width="stretch", hide_index=True)
            else:
                st.info("No Rust batch details available.")
    else:
        st.info("No runtime metrics yet. Start **Rust Streaming** or **Spark Streaming** "
                "from the ⚙️ Spark Jobs page to collect performance data.")

    # Validation report
    if validation:
        st.markdown("---")
        st.markdown("##### ✅ Validation Report")
        v1, v2, v3, v4 = st.columns(4)
        spark_rc = validation.get("spark_row_count", "—")
        rust_rc = validation.get("rust_row_count", "—")
        v1.metric("Spark Rows", f"{spark_rc:,}" if isinstance(spark_rc, int) else str(spark_rc))
        v2.metric("Rust Rows", f"{rust_rc:,}" if isinstance(rust_rc, int) else str(rust_rc))
        v3.metric("Parity", f"{validation.get('parity_pct', 0):.4f}%")
        v4.metric("Status", validation.get("status", "—"))

        if validation.get("missing_in_rust", 0) > 0 or validation.get("missing_in_spark", 0) > 0:
            m1, m2 = st.columns(2)
            m1.metric("Missing in Rust", validation.get("missing_in_rust", 0))
            m2.metric("Missing in Spark", validation.get("missing_in_spark", 0))

        if validation.get("mismatch_columns"):
            st.markdown("###### Mismatched Columns")
            mc_df = pd.DataFrame([
                {"Column": k, "Mismatches": v}
                for k, v in validation["mismatch_columns"].items()
            ])
            st.dataframe(mc_df, width="stretch", hide_index=True)

        with st.expander("📋 Raw Validation JSON"):
            st.json(validation)

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
    st.bar_chart(wf_df.set_index("Stage"), width="stretch")
    st.dataframe(wf_df, width="stretch", hide_index=True)

    # ── Raw Metrics JSON ──────────────────────────────────
    st.markdown("---")
    with st.expander("📋 Raw Metrics JSON"):
        st.json(metrics)
