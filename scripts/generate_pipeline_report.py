from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
DEFAULT_OUTPUT = ROOT / "pipeline_comparison_report.html"


@dataclass
class BatchRecord:
    batch_id: int
    rows: int
    transform_ms: float
    write_ms: float
    total_ms: float
    started_at: datetime | None = None
    completed_at: datetime | None = None


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_spark_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f")


def _parse_rust_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")


def _fmt_int(value: int | float | None) -> str:
    if value is None:
        return "-"
    return f"{int(round(value)):,}"


def _fmt_ms(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{int(round(value)):,} ms"


def _fmt_wall_clock(seconds: float | None) -> str:
    if not seconds:
        return "-"
    total_seconds = int(round(seconds))
    minutes, sec = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours} h {minutes} m {sec} s"
    return f"{minutes} m {sec} s"


def _fmt_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    return f"{seconds:.1f} s"


def _fmt_minutes(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    return f"{seconds / 60:.1f} min"


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _fmt_speedup(a: float, b: float, lower_is_better: bool = True) -> tuple[str, str]:
    if a <= 0 or b <= 0:
        return "-", "Tie"
    if lower_is_better:
        if a == b:
            return "1.0x", "Tie"
        if a > b:
            return f"{a / b:.1f}x", "Rust"
        return f"{b / a:.1f}x", "Spark"
    if a == b:
        return "1.0x", "Tie"
    if a < b:
        return f"{b / a:.1f}x", "Rust"
    return f"{a / b:.1f}x", "Spark"


def _winner_cell(a: float, b: float, lower_is_better: bool = True) -> str:
    speedup, winner = _fmt_speedup(a, b, lower_is_better=lower_is_better)
    if winner == "Tie":
        return "Tie"
    color = "var(--rust-color)" if winner == "Rust" else "var(--spark-color)"
    return f'<span style="color:{color}">{winner} ({speedup})</span>'


def _bar_fill_width(value: float, baseline: float) -> float:
    if baseline <= 0 or value <= 0:
        return 8.0
    return max(8.0, min(100.0, value / baseline * 100.0))


def _render_bar_rows(spark_label: str, spark_value: str, spark_width: float, rust_label: str, rust_value: str, rust_width: float) -> str:
    return f"""
<div class=\"bar-chart\">
  <div class=\"bar-row\">
    <span class=\"bar-label\"><span class=\"tag spark\">{spark_label}</span></span>
    <div class=\"bar-track\"><div class=\"bar-fill spark\" style=\"width:{spark_width:.1f}%\">{spark_value}</div></div>
  </div>
  <div class=\"bar-row\">
    <span class=\"bar-label\"><span class=\"tag rust\">{rust_label}</span></span>
    <div class=\"bar-track\"><div class=\"bar-fill rust\" style=\"width:{rust_width:.1f}%\">{rust_value}</div></div>
  </div>
</div>
"""


def _timestamp_series(payload: dict, count: int) -> list[datetime | None]:
    if count <= 0:
        return []
    first = payload.get("first_update")
    last = payload.get("last_update")
    if not first or not last:
        return [None] * count
    start_dt = datetime.fromtimestamp(float(first))
    end_dt = datetime.fromtimestamp(float(last))
    if count == 1:
        return [end_dt]
    step = (end_dt - start_dt) / (count - 1)
    return [start_dt + (step * index) for index in range(count)]


def _load_streaming_from_metrics(path: Path) -> list[BatchRecord]:
    payload = _read_json(path)
    details = payload.get("batch_details") or []
    records: list[BatchRecord] = []
    timestamps = _timestamp_series(payload, len(details))
    for detail in details:
        idx = len(records)
        records.append(
            BatchRecord(
                batch_id=int(detail.get("batch_id", idx)),
                rows=int(detail.get("rows", 0)),
                transform_ms=float(detail.get("transform_ms", 0) or 0),
                write_ms=float(detail.get("write_ms", 0) or 0),
                total_ms=float(detail.get("wall_ms", 0) or 0),
                completed_at=timestamps[idx] if idx < len(timestamps) else None,
            )
        )
    return records


def _load_account_from_metrics(path: Path) -> list[BatchRecord]:
    payload = _read_json(path)
    details = payload.get("batch_details") or []
    records: list[BatchRecord] = []
    if details:
        timestamps = _timestamp_series(payload, len(details))
        for detail in details:
            idx = len(records)
            records.append(
                BatchRecord(
                    batch_id=int(detail.get("batch_id", idx)),
                    rows=int(detail.get("rows", 0)),
                    transform_ms=0.0,
                    write_ms=float(detail.get("write_ms", 0) or 0),
                    total_ms=float(detail.get("wall_ms", 0) or 0),
                    completed_at=timestamps[idx] if idx < len(timestamps) else None,
                )
            )
        return records

    durations = [float(value) for value in (payload.get("batch_durations_ms") or [])]
    if not durations:
        return records

    total_rows = int(payload.get("total_rows_processed", 0) or 0)
    batch_count = len(durations)
    base_rows = total_rows // batch_count if batch_count else 0
    remainder = total_rows - (base_rows * batch_count)
    timestamps = _timestamp_series(payload, batch_count)
    for index, duration in enumerate(durations):
        rows = base_rows + (1 if index < remainder else 0)
        records.append(
            BatchRecord(
                batch_id=index,
                rows=rows,
                transform_ms=0.0,
                write_ms=duration,
                total_ms=duration,
                completed_at=timestamps[index] if index < len(timestamps) else None,
            )
        )
    return records


def _parse_spark_stream_log(path: Path) -> list[BatchRecord]:
    pattern = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*Batch (?P<batch>\d+): (?P<rows>\d+) rows \| transform (?P<transform>\d+) ms \| delta_write (?P<write>\d+) ms \| total (?P<total>\d+) ms"
    )
    records: list[BatchRecord] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        completed = _parse_spark_ts(match.group("ts"))
        records.append(
            BatchRecord(
                batch_id=int(match.group("batch")),
                rows=int(match.group("rows")),
                transform_ms=float(match.group("transform")),
                write_ms=float(match.group("write")),
                total_ms=float(match.group("total")),
                completed_at=completed,
            )
        )
    return records


def _parse_rust_stream_log(path: Path) -> list[BatchRecord]:
    pattern = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).*Batch (?P<batch>\d+): (?P<rows>\d+) rows \| transform (?P<transform>\d+)ms \| write (?P<write>\d+)ms \| total (?P<total>\d+)ms"
    )
    records: list[BatchRecord] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        completed = _parse_rust_ts(match.group("ts"))
        records.append(
            BatchRecord(
                batch_id=int(match.group("batch")),
                rows=int(match.group("rows")),
                transform_ms=float(match.group("transform")),
                write_ms=float(match.group("write")),
                total_ms=float(match.group("total")),
                completed_at=completed,
            )
        )
    return records


def _parse_spark_account_log(path: Path) -> list[BatchRecord]:
    start_pattern = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*Batch (?P<batch>\d+): upserting (?P<rows>\d+) account updates"
    )
    end_pattern = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*Batch (?P<batch>\d+): (?P<rows>\d+) rows \| upsert (?P<total>[\d.]+) ms"
    )
    started: dict[int, tuple[datetime, int]] = {}
    records: list[BatchRecord] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        start_match = start_pattern.search(line)
        if start_match:
            started[int(start_match.group("batch"))] = (
                _parse_spark_ts(start_match.group("ts")),
                int(start_match.group("rows")),
            )
            continue
        end_match = end_pattern.search(line)
        if not end_match:
            continue
        batch_id = int(end_match.group("batch"))
        start_info = started.get(batch_id)
        records.append(
            BatchRecord(
                batch_id=batch_id,
                rows=start_info[1] if start_info else int(end_match.group("rows")),
                transform_ms=0.0,
                write_ms=float(end_match.group("total")),
                total_ms=float(end_match.group("total")),
                started_at=start_info[0] if start_info else None,
                completed_at=_parse_spark_ts(end_match.group("ts")),
            )
        )
    return records


def _parse_rust_account_log(path: Path) -> list[BatchRecord]:
    pattern = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).*Account batch (?P<batch>\d+): (?P<rows>\d+) rows \(deduped\) \| merge (?P<merge>\d+)ms \| total (?P<total>\d+)ms"
    )
    records: list[BatchRecord] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        completed = _parse_rust_ts(match.group("ts"))
        records.append(
            BatchRecord(
                batch_id=int(match.group("batch")),
                rows=int(match.group("rows")),
                transform_ms=0.0,
                write_ms=float(match.group("merge")),
                total_ms=float(match.group("total")),
                completed_at=completed,
            )
        )
    return records


def _run_span(records: list[BatchRecord]) -> float:
    moments = [record.completed_at or record.started_at for record in records if record.completed_at or record.started_at]
    if len(moments) < 2:
        if len(records) == 1 and records[0].total_ms > 0:
            return records[0].total_ms / 1000.0
        return 0.0
    return max((max(moments) - min(moments)).total_seconds(), 0.0)


def _table_rows(records: list[BatchRecord], account_mode: bool = False) -> str:
    row_html: list[str] = []
    colspan = 4
    for record in records:
        if account_mode:
            row_html.append(f"<tr><td>{record.batch_id}</td><td>{_fmt_int(record.rows)}</td><td>{_fmt_ms(record.write_ms)}</td><td>{_fmt_ms(record.total_ms)}</td></tr>")
        else:
            colspan = 5
            row_html.append(f"<tr><td>{record.batch_id}</td><td>{_fmt_int(record.rows)}</td><td>{_fmt_ms(record.transform_ms)}</td><td>{_fmt_ms(record.write_ms)}</td><td>{_fmt_ms(record.total_ms)}</td></tr>")
    return "\n".join(row_html) if row_html else f"<tr><td colspan=\"{colspan}\">No completed batches found.</td></tr>"


def _throughput(records: list[BatchRecord]) -> float:
    total_ms = sum(record.total_ms for record in records)
    total_rows = sum(record.rows for record in records)
    if total_ms <= 0:
        return 0.0
    return total_rows / (total_ms / 1000.0)


def _summary(records: list[BatchRecord]) -> dict:
    totals = [record.total_ms for record in records]
    transforms = [record.transform_ms for record in records]
    writes = [record.write_ms for record in records]
    rows = [record.rows for record in records]
    return {
        "batches": len(records),
        "rows": sum(rows),
        "avg_total_ms": _avg(totals),
        "avg_transform_ms": _avg(transforms),
        "avg_write_ms": _avg(writes),
        "cumulative_total_ms": sum(totals),
        "wall_clock_seconds": _run_span(records),
        "throughput_rows_per_sec": _throughput(records),
    }


def _full_batch_subset(records: list[BatchRecord], threshold_ratio: float = 0.97) -> list[BatchRecord]:
    if not records:
        return []
    max_rows = max(record.rows for record in records)
    threshold = int(max_rows * threshold_ratio)
    return [record for record in records if record.rows >= threshold]


def _aligned_prefix(records: list[BatchRecord], target_rows: int) -> list[BatchRecord]:
    if not records or target_rows <= 0:
        return []

    selected: list[BatchRecord] = []
    cumulative_rows = 0
    for record in records:
        previous_rows = cumulative_rows
        selected.append(record)
        cumulative_rows += record.rows
        if cumulative_rows >= target_rows:
            if abs(previous_rows - target_rows) < abs(cumulative_rows - target_rows):
                selected.pop()
            break
    return selected


def _render_report(
    stream_spark: list[BatchRecord],
    stream_rust: list[BatchRecord],
    acct_spark: list[BatchRecord],
    acct_rust: list[BatchRecord],
    output: Path,
    transactions_only: bool = False,
) -> None:
    if transactions_only and stream_spark and stream_rust:
        comparable_rows = min(sum(record.rows for record in stream_spark), sum(record.rows for record in stream_rust))
        stream_spark = _aligned_prefix(stream_spark, comparable_rows)
        stream_rust = _aligned_prefix(stream_rust, comparable_rows)

    spark_stream = _summary(stream_spark)
    rust_stream = _summary(stream_rust)
    spark_account = _summary(acct_spark)
    rust_account = _summary(acct_rust)
    spark_full = _full_batch_subset(stream_spark)
    rust_full = _full_batch_subset(stream_rust)
    spark_full_s = _summary(spark_full)
    rust_full_s = _summary(rust_full)

    dataset_rows = max(
        spark_stream["rows"],
        rust_stream["rows"],
        0 if transactions_only else spark_account["rows"],
        0 if transactions_only else rust_account["rows"],
    )
    run_date = datetime.now().strftime("%Y-%m-%d")
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    workload_label = "Financial Transactions Streaming" if transactions_only else "Financial Transactions Streaming &amp; Account Upsert"
    topic_label = "financial_transactions" if transactions_only else "financial_transactions, account_updates"
    wall_clock_ratio, wall_clock_winner = _fmt_speedup(spark_stream["wall_clock_seconds"], rust_stream["wall_clock_seconds"])
    stream_ratio, _ = _fmt_speedup(spark_stream["avg_total_ms"], rust_stream["avg_total_ms"])
    cumulative_ratio, _ = _fmt_speedup(spark_stream["cumulative_total_ms"], rust_stream["cumulative_total_ms"])
    full_total_ratio, full_total_winner = _fmt_speedup(spark_full_s["avg_total_ms"], rust_full_s["avg_total_ms"])
    full_transform_ratio, full_transform_winner = _fmt_speedup(spark_full_s["avg_transform_ms"], rust_full_s["avg_transform_ms"])
    full_write_ratio, full_write_winner = _fmt_speedup(spark_full_s["avg_write_ms"], rust_full_s["avg_write_ms"])
    throughput_ratio, _ = _fmt_speedup(spark_full_s["throughput_rows_per_sec"], rust_full_s["throughput_rows_per_sec"], lower_is_better=False)

    notes: list[str] = []
    if spark_stream["rows"] != rust_stream["rows"]:
        notes.append(f"Streaming row totals differ: Spark processed {_fmt_int(spark_stream['rows'])} rows while Rust processed {_fmt_int(rust_stream['rows'])}.")
    if not transactions_only and spark_account["rows"] != rust_account["rows"]:
        notes.append(f"Account-upsert row totals differ: Spark processed {_fmt_int(spark_account['rows'])} rows while Rust processed {_fmt_int(rust_account['rows'])}.")
    if not notes:
        if transactions_only:
            notes.append("Spark and Rust transaction row totals match in the completed logs.")
        else:
            notes.append("Spark and Rust row totals match for both workloads in the completed logs.")
    if spark_stream["rows"] != rust_stream["rows"]:
        notes.append("Use the near-full-batch sections as the strongest decision signal; end-state row parity was not reached in both engines at the same cutoff.")

    wall_clock_baseline = max(spark_stream["wall_clock_seconds"], rust_stream["wall_clock_seconds"], 1)
    cumulative_baseline = max(spark_stream["cumulative_total_ms"] / 1000.0, rust_stream["cumulative_total_ms"] / 1000.0, 1)
    latency_baseline = max(spark_full_s["avg_total_ms"], rust_full_s["avg_total_ms"], 1)
    transform_baseline = max(spark_full_s["avg_transform_ms"], rust_full_s["avg_transform_ms"], 1)
    write_baseline = max(spark_full_s["avg_write_ms"], rust_full_s["avg_write_ms"], 1)

    analysis_points: list[str] = []
    if full_write_winner == "Rust":
        analysis_points.append(f"Rust's main advantage is Delta Lake write cost at {full_write_ratio} faster on large batches.")
    if full_transform_winner == "Rust":
        analysis_points.append(f"Transform cost also favors Rust at {full_transform_ratio} on large batches.")
    if full_total_winner == "Rust":
        analysis_points.append(f"Overall sustained batch latency favors Rust at {full_total_ratio} on the near-full-batch benchmark.")
    if not transactions_only and spark_account["avg_total_ms"] > 0 and rust_account["avg_total_ms"] > 0:
        account_ratio, account_winner = _fmt_speedup(spark_account["avg_total_ms"], rust_account["avg_total_ms"])
        analysis_points.append(f"Account upserts favor {account_winner} with a {account_ratio} average batch latency advantage.")
    if not analysis_points:
        analysis_points.append("No single engine dominated every section of the captured run.")

    account_section = ""
    account_takeaway_row = ""
    if not transactions_only:
        account_section = f"""
<h2>Account Upsert</h2>
<div class=\"two-col\">
<div>
<h3><span class=\"tag rust\">Rust</span> Account Upsert &mdash; {_fmt_int(rust_account['batches'])} Batches</h3>
<table>
    <thead><tr><th>Batch</th><th>Rows</th><th>Merge</th><th>Total</th></tr></thead>
    <tbody>{_table_rows(acct_rust, account_mode=True)}</tbody>
</table>
</div>
<div>
<h3><span class=\"tag spark\">Spark</span> Account Upsert &mdash; {_fmt_int(spark_account['batches'])} Batches</h3>
<table>
    <thead><tr><th>Batch</th><th>Rows</th><th>Merge</th><th>Total</th></tr></thead>
    <tbody>{_table_rows(acct_spark, account_mode=True)}</tbody>
</table>

<h3 style=\"margin-top:2rem\">Account Upsert Summary</h3>
<div class=\"kpi-grid\">
    <div class=\"kpi spark\"><div class=\"value\">{_fmt_int(spark_account['batches'])}</div><div class=\"label\">Spark Batches Processed</div></div>
    <div class=\"kpi rust\"><div class=\"value\">{_fmt_int(rust_account['batches'])}</div><div class=\"label\">Rust Batches Processed</div></div>
    <div class=\"kpi spark\"><div class=\"value\">{_fmt_ms(spark_account['avg_write_ms'])}</div><div class=\"label\">Spark Avg Merge Latency</div></div>
    <div class=\"kpi rust\"><div class=\"value\">{_fmt_ms(rust_account['avg_write_ms'])}</div><div class=\"label\">Rust Avg Merge Latency</div></div>
</div>
</div>
</div>
"""
        account_takeaway_row = f"<tr><td style=\"text-align:left\">Account avg upsert</td><td>{_fmt_ms(spark_account['avg_total_ms'])}</td><td>{_fmt_ms(rust_account['avg_total_ms'])}</td><td>{_winner_cell(spark_account['avg_total_ms'], rust_account['avg_total_ms'])}</td></tr>"

    html_text = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"UTF-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
<title>Spark vs Rust Pipeline &mdash; Performance Comparison Report</title>
<style>
  :root {{
    --spark-color: #e8740c;
    --rust-color: #b7410e;
    --bg: #0d1117;
    --card: #161b22;
    --border: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --green: #3fb950;
    --blue: #58a6ff;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.6; padding: 2rem;
  }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  h1 {{ font-size: 2rem; margin-bottom: .25rem; }}
  h2 {{ font-size: 1.4rem; margin: 2rem 0 1rem; border-bottom: 1px solid var(--border); padding-bottom: .5rem; }}
  h3 {{ font-size: 1.1rem; margin: 1.5rem 0 .75rem; color: var(--blue); }}
  .subtitle {{ color: var(--muted); margin-bottom: 2rem; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; margin: 1.5rem 0; }}
  .kpi {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; text-align: center; }}
  .kpi .value {{ font-size: 2rem; font-weight: 700; }}
  .kpi .label {{ color: var(--muted); font-size: .85rem; margin-top: .25rem; }}
  .kpi.spark .value {{ color: var(--spark-color); }}
  .kpi.rust .value {{ color: var(--rust-color); }}
  .kpi.highlight .value {{ color: var(--green); }}
  table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; background: var(--card); border-radius: 8px; overflow: hidden; }}
  th, td {{ padding: .6rem .8rem; text-align: right; border-bottom: 1px solid var(--border); }}
  th {{ background: #1c2129; color: var(--muted); font-size: .8rem; text-transform: uppercase; letter-spacing: .04em; }}
  td:first-child, th:first-child {{ text-align: left; }}
  tr:last-child td {{ border-bottom: none; }}
  .bar-chart {{ margin: 1.5rem 0; }}
  .bar-row {{ display: flex; align-items: center; margin-bottom: .5rem; }}
  .bar-label {{ width: 80px; font-size: .85rem; color: var(--muted); flex-shrink: 0; }}
  .bar-track {{ flex: 1; height: 28px; background: #21262d; border-radius: 4px; position: relative; overflow: hidden; }}
  .bar-fill {{ height: 100%; border-radius: 4px; display: flex; align-items: center; padding-left: .5rem; font-size: .75rem; font-weight: 600; color: #fff; white-space: nowrap; min-width: 40px; }}
  .bar-fill.spark {{ background: var(--spark-color); }}
  .bar-fill.rust {{ background: var(--rust-color); }}
  .note {{ background: #1c2129; border-left: 3px solid var(--blue); padding: .75rem 1rem; border-radius: 0 6px 6px 0; margin: 1rem 0; font-size: .9rem; color: var(--muted); }}
  .footer {{ margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border); color: var(--muted); font-size: .8rem; text-align: center; }}
  .tag {{ display: inline-block; padding: .15rem .5rem; border-radius: 12px; font-size: .75rem; font-weight: 600; }}
  .tag.spark {{ background: rgba(232,116,12,.2); color: var(--spark-color); }}
  .tag.rust {{ background: rgba(183,65,14,.2); color: var(--rust-color); }}
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }}
  @media (max-width: 800px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<div class=\"container\">
<h1>Spark vs Rust Pipeline &mdash; Performance Comparison</h1>
<p class=\"subtitle\">{workload_label} &bull; Run Date: {html.escape(run_date)} &bull; Dataset: captured logs/metrics (~{_fmt_int(dataset_rows)} rows max observed)</p>

<h2>Executive Summary</h2>
<div class=\"kpi-grid\">
  <div class=\"kpi spark\"><div class=\"value\">{_fmt_wall_clock(spark_stream['wall_clock_seconds'])}</div><div class=\"label\">Spark Wall-Clock Time</div></div>
  <div class=\"kpi rust\"><div class=\"value\">{_fmt_wall_clock(rust_stream['wall_clock_seconds'])}</div><div class=\"label\">Rust Wall-Clock Time</div></div>
  <div class=\"kpi highlight\"><div class=\"value\">{wall_clock_ratio}</div><div class=\"label\">{wall_clock_winner} Wall-Clock Speedup</div></div>
  <div class=\"kpi\"><div class=\"value\">{_fmt_int(min(spark_stream['rows'], rust_stream['rows']))}</div><div class=\"label\">Comparable Streaming Rows</div></div>
  <div class=\"kpi spark\"><div class=\"value\">{_fmt_ms(spark_stream['avg_total_ms'])}</div><div class=\"label\">Spark Avg Batch Latency</div></div>
  <div class=\"kpi rust\"><div class=\"value\">{_fmt_ms(rust_stream['avg_total_ms'])}</div><div class=\"label\">Rust Avg Batch Latency</div></div>
  <div class=\"kpi highlight\"><div class=\"value\">{stream_ratio}</div><div class=\"label\">Per-Batch Latency Speedup</div></div>
  <div class=\"kpi\"><div class=\"value\">{_fmt_int(spark_stream['batches'])} / {_fmt_int(rust_stream['batches'])}</div><div class=\"label\">Batches (Spark / Rust)</div></div>
</div>

<h3>Wall-Clock Duration</h3>
{_render_bar_rows('Spark', _fmt_wall_clock(spark_stream['wall_clock_seconds']), _bar_fill_width(spark_stream['wall_clock_seconds'], wall_clock_baseline), 'Rust', _fmt_wall_clock(rust_stream['wall_clock_seconds']), _bar_fill_width(rust_stream['wall_clock_seconds'], wall_clock_baseline))}

<h3>Cumulative Batch Processing Time</h3>
{_render_bar_rows('Spark', f"{_fmt_seconds(spark_stream['cumulative_total_ms'] / 1000.0)} ({_fmt_minutes(spark_stream['cumulative_total_ms'] / 1000.0)})", _bar_fill_width(spark_stream['cumulative_total_ms'] / 1000.0, cumulative_baseline), 'Rust', f"{_fmt_seconds(rust_stream['cumulative_total_ms'] / 1000.0)} ({_fmt_minutes(rust_stream['cumulative_total_ms'] / 1000.0)})", _bar_fill_width(rust_stream['cumulative_total_ms'] / 1000.0, cumulative_baseline))}
<p class=\"note\">Cumulative time = sum of all individual batch durations. This is often the clearest decision metric when comparing engine overhead independent of Kafka idle gaps.</p>

<div class=\"note\">{' '.join(html.escape(note) for note in notes)}</div>

<h2>Transaction Streaming &mdash; Per-Batch Results</h2>
<div class=\"two-col\">
<div>
<h3><span class=\"tag spark\">Spark</span> &mdash; {_fmt_int(spark_stream['batches'])} Batches</h3>
<table>
  <thead><tr><th>Batch</th><th>Rows</th><th>Transform</th><th>Write</th><th>Total</th></tr></thead>
  <tbody>{_table_rows(stream_spark)}</tbody>
</table>
</div>
<div>
<h3><span class=\"tag rust\">Rust</span> &mdash; {_fmt_int(rust_stream['batches'])} Batches</h3>
<table>
  <thead><tr><th>Batch</th><th>Rows</th><th>Transform</th><th>Write</th><th>Total</th></tr></thead>
  <tbody>{_table_rows(stream_rust)}</tbody>
</table>
</div>
</div>

<h2>Apples-to-Apples &mdash; Near-Full Batches</h2>
<p class=\"note\">This compares only large streaming batches, which is the strongest benchmark for steady-state throughput and Delta write cost.</p>
<table>
  <thead><tr><th>Metric</th><th><span class=\"tag spark\">Spark</span></th><th><span class=\"tag rust\">Rust</span></th><th>Speedup</th></tr></thead>
  <tbody>
    <tr><td style=\"text-align:left\">Avg Transform</td><td>{_fmt_ms(spark_full_s['avg_transform_ms'])}</td><td>{_fmt_ms(rust_full_s['avg_transform_ms'])}</td><td style=\"color:var(--green)\">{full_transform_ratio}</td></tr>
    <tr><td style=\"text-align:left\">Avg Write</td><td>{_fmt_ms(spark_full_s['avg_write_ms'])}</td><td>{_fmt_ms(rust_full_s['avg_write_ms'])}</td><td style=\"color:var(--green)\">{full_write_ratio}</td></tr>
    <tr><td style=\"text-align:left\">Avg Total</td><td>{_fmt_ms(spark_full_s['avg_total_ms'])}</td><td>{_fmt_ms(rust_full_s['avg_total_ms'])}</td><td style=\"color:var(--green)\">{full_total_ratio}</td></tr>
    <tr><td style=\"text-align:left\">Throughput</td><td>{_fmt_int(spark_full_s['throughput_rows_per_sec'])} rows/s</td><td>{_fmt_int(rust_full_s['throughput_rows_per_sec'])} rows/s</td><td style=\"color:var(--green)\">{throughput_ratio}</td></tr>
  </tbody>
</table>

<h3>Average Batch Latency &mdash; Near-Full Batches</h3>
{_render_bar_rows('Spark', _fmt_ms(spark_full_s['avg_total_ms']), _bar_fill_width(spark_full_s['avg_total_ms'], latency_baseline), 'Rust', _fmt_ms(rust_full_s['avg_total_ms']), _bar_fill_width(rust_full_s['avg_total_ms'], latency_baseline))}

<h3>Latency Breakdown &mdash; Near-Full Batches</h3>
<div class=\"two-col\">
<div>
  <h3 style=\"margin-top:0\">Transform Phase</h3>
  {_render_bar_rows('Spark', _fmt_ms(spark_full_s['avg_transform_ms']), _bar_fill_width(spark_full_s['avg_transform_ms'], transform_baseline), 'Rust', _fmt_ms(rust_full_s['avg_transform_ms']), _bar_fill_width(rust_full_s['avg_transform_ms'], transform_baseline))}
</div>
<div>
  <h3 style=\"margin-top:0\">Write Phase (Delta Lake)</h3>
  {_render_bar_rows('Spark', _fmt_ms(spark_full_s['avg_write_ms']), _bar_fill_width(spark_full_s['avg_write_ms'], write_baseline), 'Rust', _fmt_ms(rust_full_s['avg_write_ms']), _bar_fill_width(rust_full_s['avg_write_ms'], write_baseline))}
</div>
</div>

{account_section}

<h2>Key Takeaways</h2>
<table>
  <thead><tr><th style=\"text-align:left\">Dimension</th><th><span class=\"tag spark\">Spark</span></th><th><span class=\"tag rust\">Rust</span></th><th>Winner</th></tr></thead>
  <tbody>
    <tr><td style=\"text-align:left\">Wall-clock time</td><td>{_fmt_wall_clock(spark_stream['wall_clock_seconds'])}</td><td>{_fmt_wall_clock(rust_stream['wall_clock_seconds'])}</td><td>{_winner_cell(spark_stream['wall_clock_seconds'], rust_stream['wall_clock_seconds'])}</td></tr>
    <tr><td style=\"text-align:left\">Cumulative batch time</td><td>{_fmt_seconds(spark_stream['cumulative_total_ms'] / 1000.0)}</td><td>{_fmt_seconds(rust_stream['cumulative_total_ms'] / 1000.0)}</td><td>{_winner_cell(spark_stream['cumulative_total_ms'], rust_stream['cumulative_total_ms'])}</td></tr>
    <tr><td style=\"text-align:left\">Avg batch latency (all)</td><td>{_fmt_ms(spark_stream['avg_total_ms'])}</td><td>{_fmt_ms(rust_stream['avg_total_ms'])}</td><td>{_winner_cell(spark_stream['avg_total_ms'], rust_stream['avg_total_ms'])}</td></tr>
    <tr><td style=\"text-align:left\">Avg batch latency (near-full)</td><td>{_fmt_ms(spark_full_s['avg_total_ms'])}</td><td>{_fmt_ms(rust_full_s['avg_total_ms'])}</td><td>{_winner_cell(spark_full_s['avg_total_ms'], rust_full_s['avg_total_ms'])}</td></tr>
    <tr><td style=\"text-align:left\">Avg transform (near-full)</td><td>{_fmt_ms(spark_full_s['avg_transform_ms'])}</td><td>{_fmt_ms(rust_full_s['avg_transform_ms'])}</td><td>{_winner_cell(spark_full_s['avg_transform_ms'], rust_full_s['avg_transform_ms'])}</td></tr>
    <tr><td style=\"text-align:left\">Avg write (near-full)</td><td>{_fmt_ms(spark_full_s['avg_write_ms'])}</td><td>{_fmt_ms(rust_full_s['avg_write_ms'])}</td><td>{_winner_cell(spark_full_s['avg_write_ms'], rust_full_s['avg_write_ms'])}</td></tr>
    <tr><td style=\"text-align:left\">Streaming rows processed</td><td>{_fmt_int(spark_stream['rows'])}</td><td>{_fmt_int(rust_stream['rows'])}</td><td>{'Tie' if spark_stream['rows'] == rust_stream['rows'] else 'Not directly comparable'}</td></tr>
        {account_takeaway_row}
  </tbody>
</table>

<div class=\"note\" style=\"margin-top:1.5rem\"><strong>Analysis:</strong> {html.escape(' '.join(analysis_points))}</div>

<h2>Test Environment</h2>
<table>
  <tbody>
    <tr><td style=\"text-align:left\">Platform</td><td style=\"text-align:left\">Docker Desktop on Windows (WSL 2 backend)</td></tr>
    <tr><td style=\"text-align:left\">Services</td><td style=\"text-align:left\">zookeeper, kafka, spark-master, spark-worker(s), rust-pipeline, dashboard</td></tr>
    <tr><td style=\"text-align:left\">Kafka Topics</td><td style=\"text-align:left\">{topic_label}</td></tr>
    <tr><td style=\"text-align:left\">Spark</td><td style=\"text-align:left\">PySpark Structured Streaming with Delta Lake</td></tr>
    <tr><td style=\"text-align:left\">Rust</td><td style=\"text-align:left\">rdkafka + deltalake-rs</td></tr>
    <tr><td style=\"text-align:left\">Generated</td><td style=\"text-align:left\">{html.escape(generated_at)}</td></tr>
  </tbody>
</table>

<div class=\"footer\">Generated from pipeline logs &bull; spark_to_rust_migration &bull; {html.escape(generated_at)}</div>
</div>
</body>
</html>
"""

    output.write_text(html_text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a Spark vs Rust HTML comparison report from local logs.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Path to the HTML file to write.")
    parser.add_argument("--transactions-only", action="store_true", help="Render a transaction-only report and ignore account-upsert logs/metrics.")
    args = parser.parse_args()

    spark_stream = (
        _load_streaming_from_metrics(LOGS / "spark_stream_metrics.json")
        or _parse_spark_stream_log(LOGS / "streaming.log")
        or _parse_spark_stream_log(LOGS / "streaming_restart.log")
    )
    rust_stream = (
        _load_streaming_from_metrics(LOGS / "rust_metrics.json")
        or _parse_rust_stream_log(LOGS / "rust_stream.log")
    )
    if args.transactions_only:
        spark_account = []
        rust_account = []
    else:
        spark_account = _parse_spark_account_log(LOGS / "account_upsert_restart_3.log") or _load_account_from_metrics(LOGS / "spark_account_metrics.json")
        rust_account = _parse_rust_account_log(LOGS / "rust_upsert.log") or _load_account_from_metrics(LOGS / "rust_acct_metrics.json")
    _render_report(spark_stream, rust_stream, spark_account, rust_account, args.output, transactions_only=args.transactions_only)


if __name__ == "__main__":
    main()