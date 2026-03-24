"""
Delta Table Viewer — inspect, query, and profile Delta Lake tables.

Provides a rich CLI for exploring Delta tables without Spark,
using the native `deltalake` Python library (backed by delta-rs).

Usage:
    python -m delta_viewer.viewer info
    python -m delta_viewer.viewer schema
    python -m delta_viewer.viewer history
    python -m delta_viewer.viewer sample --rows 20
    python -m delta_viewer.viewer stats
    python -m delta_viewer.viewer query --sql "SELECT currency, COUNT(*) as cnt FROM txn GROUP BY currency ORDER BY cnt DESC"
    python -m delta_viewer.viewer partitions
"""

import logging
import sys
from pathlib import Path

import click
import pyarrow.compute as pc
import pyarrow.dataset as ds
import yaml
from deltalake import DeltaTable
from rich.console import Console
from rich.table import Table as RichTable
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("delta_viewer")
console = Console()


def _load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _get_delta_table(table_path: str) -> DeltaTable:
    if not Path(table_path).exists():
        console.print(f"[red]Delta table not found:[/red] {table_path}")
        sys.exit(1)
    return DeltaTable(table_path)


@click.group()
@click.option("--config", default="config/app_config.yaml", help="Config file path")
@click.option("--path", default=None, help="Delta table path (overrides config)")
@click.pass_context
def cli(ctx, config, path):
    """Delta Table Viewer — inspect and query Delta Lake tables."""
    cfg = _load_config(config)
    table_path = path or cfg["delta"]["table_path"]
    ctx.ensure_object(dict)
    ctx.obj["table_path"] = table_path


@cli.command()
@click.pass_context
def info(ctx):
    """Show table metadata: version, partitions, file count, size."""
    dt = _get_delta_table(ctx.obj["table_path"])

    files = dt.files()
    metadata = dt.metadata()

    tbl = RichTable(title="Delta Table Info", show_lines=True)
    tbl.add_column("Property", style="bold cyan")
    tbl.add_column("Value")

    tbl.add_row("Path", ctx.obj["table_path"])
    tbl.add_row("Version", str(dt.version()))
    tbl.add_row("Name", metadata.name or "(none)")
    tbl.add_row("Description", metadata.description or "(none)")
    tbl.add_row("Partition Columns", ", ".join(metadata.partition_columns) or "(none)")
    tbl.add_row("Num Files", f"{len(files):,}")
    tbl.add_row("Created At", str(metadata.created_time))

    console.print(tbl)


@cli.command()
@click.pass_context
def schema(ctx):
    """Print the table schema."""
    dt = _get_delta_table(ctx.obj["table_path"])
    arrow_schema = dt.schema().to_pyarrow()

    tbl = RichTable(title="Table Schema", show_lines=True)
    tbl.add_column("#", style="dim")
    tbl.add_column("Column", style="bold")
    tbl.add_column("Type")
    tbl.add_column("Nullable")

    for i, field in enumerate(arrow_schema):
        tbl.add_row(str(i), field.name, str(field.type), str(field.nullable))

    console.print(tbl)


@cli.command()
@click.option("--limit", default=10, help="Number of history entries to show")
@click.pass_context
def history(ctx, limit):
    """Show commit history / transaction log."""
    dt = _get_delta_table(ctx.obj["table_path"])
    hist = dt.history(limit=limit)

    tbl = RichTable(title=f"Delta History (last {limit})", show_lines=True)
    tbl.add_column("Version", style="bold")
    tbl.add_column("Timestamp")
    tbl.add_column("Operation")
    tbl.add_column("Parameters")

    for entry in hist:
        tbl.add_row(
            str(entry.get("version", "")),
            str(entry.get("timestamp", "")),
            str(entry.get("operation", "")),
            str(entry.get("operationParameters", "")),
        )

    console.print(tbl)


@cli.command()
@click.option("--rows", default=10, help="Number of sample rows")
@click.pass_context
def sample(ctx, rows):
    """Show a sample of rows from the table."""
    dt = _get_delta_table(ctx.obj["table_path"])
    df = dt.to_pyarrow_table()

    # Take first N rows
    sample_df = df.slice(0, rows).to_pandas()
    console.print(f"\n[bold]Sample ({rows} rows):[/bold]\n")
    click.echo(tabulate(sample_df, headers="keys", tablefmt="pretty", showindex=False))


@cli.command()
@click.pass_context
def stats(ctx):
    """Show table-level statistics: row count, size, per-column stats."""
    dt = _get_delta_table(ctx.obj["table_path"])
    table = dt.to_pyarrow_table()

    num_rows = table.num_rows
    num_cols = table.num_columns
    nbytes = table.nbytes

    console.print(f"\n[bold cyan]Table Statistics[/bold cyan]")
    console.print(f"  Rows:    {num_rows:,}")
    console.print(f"  Columns: {num_cols}")
    console.print(f"  Size:    {nbytes / 1e9:.2f} GB (in-memory Arrow)")

    # Per-column null counts
    tbl = RichTable(title="Column Null Counts", show_lines=True)
    tbl.add_column("Column", style="bold")
    tbl.add_column("Nulls", justify="right")
    tbl.add_column("Null %", justify="right")

    for col_name in table.column_names:
        col = table.column(col_name)
        null_count = col.null_count
        pct = (null_count / num_rows * 100) if num_rows > 0 else 0
        tbl.add_row(col_name, f"{null_count:,}", f"{pct:.2f}%")

    console.print(tbl)

    # Amount distribution if present
    if "amount" in table.column_names:
        amounts = table.column("amount")
        console.print(f"\n[bold cyan]Amount Distribution[/bold cyan]")
        console.print(f"  Min:    {pc.min(amounts).as_py():,.2f}")
        console.print(f"  Max:    {pc.max(amounts).as_py():,.2f}")
        console.print(f"  Mean:   {pc.mean(amounts).as_py():,.2f}")
        console.print(f"  Stddev: {pc.stddev(amounts).as_py():,.2f}")


@cli.command()
@click.pass_context
def partitions(ctx):
    """List partitions and their file counts."""
    dt = _get_delta_table(ctx.obj["table_path"])
    files = dt.files()

    partition_counts = {}
    for f in files:
        parts = Path(f).parts[:-1]  # everything except filename
        key = "/".join(parts) if parts else "(root)"
        partition_counts[key] = partition_counts.get(key, 0) + 1

    sorted_parts = sorted(partition_counts.items(), key=lambda x: -x[1])

    tbl = RichTable(title="Partitions", show_lines=True)
    tbl.add_column("Partition", style="bold")
    tbl.add_column("Files", justify="right")

    for part, count in sorted_parts[:50]:  # Show top 50
        tbl.add_row(part, str(count))

    if len(sorted_parts) > 50:
        tbl.add_row(f"... and {len(sorted_parts) - 50} more", "")

    console.print(tbl)
    console.print(f"\nTotal partitions: {len(sorted_parts):,}")
    console.print(f"Total files:      {len(files):,}")


@cli.command()
@click.option("--sql", required=True, help="SQL query (table alias: 'txn')")
@click.option("--limit", default=50, help="Max rows to display")
@click.pass_context
def query(ctx, sql, limit):
    """Run a SQL query against the Delta table using DuckDB (if available) or PyArrow."""
    dt = _get_delta_table(ctx.obj["table_path"])

    try:
        import duckdb

        arrow_table = dt.to_pyarrow_table()
        con = duckdb.connect()
        con.register("txn", arrow_table)
        result = con.execute(sql).fetchdf()

        if len(result) > limit:
            result = result.head(limit)
            console.print(f"[dim]Showing first {limit} of {len(result)} rows[/dim]")

        click.echo(tabulate(result, headers="keys", tablefmt="pretty", showindex=False))

    except ImportError:
        console.print(
            "[yellow]DuckDB not installed. Install with: pip install duckdb[/yellow]"
        )
        console.print("Falling back to PyArrow filter (limited SQL support)...")

        table = dt.to_pyarrow_table()
        df = table.to_pandas()
        console.print(f"Table loaded: {len(df):,} rows. Use 'sample' or 'stats' commands instead.")


if __name__ == "__main__":
    cli()
