"""
Data Cleanup Tool — vacuum, compact, and purge Delta tables,
clear Kafka topics, and remove checkpoint state.

Usage:
    python -m cleanup.cleanup vacuum                     # Remove old Delta files
    python -m cleanup.cleanup compact                    # Compact small files
    python -m cleanup.cleanup purge                      # Delete entire Delta table
    python -m cleanup.cleanup kafka-reset                # Reset Kafka topic offsets
    python -m cleanup.cleanup full-reset                 # Nuclear option: everything
    python -m cleanup.cleanup status                     # Show what would be cleaned
"""

import logging
import shutil
import sys
from pathlib import Path

import click
import humanize
import yaml
from deltalake import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("cleanup")


def _load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _dir_size(path: Path) -> int:
    """Calculate total size of a directory in bytes."""
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _file_count(path: Path) -> int:
    """Count files in a directory recursively."""
    if not path.exists():
        return 0
    return sum(1 for f in path.rglob("*") if f.is_file())


@click.group()
@click.option("--config", default="config/app_config.yaml", help="Config file path")
@click.pass_context
def cli(ctx, config):
    """Data Cleanup Tool — manage Delta tables, Kafka state, and checkpoints."""
    ctx.ensure_object(dict)
    ctx.obj["cfg"] = _load_config(config)


@cli.command()
@click.option(
    "--retention-hours",
    type=int,
    default=None,
    help="Vacuum retention in hours (overrides config)",
)
@click.option("--dry-run", is_flag=True, help="Preview what would be deleted")
@click.pass_context
def vacuum(ctx, retention_hours, dry_run):
    """Run Delta VACUUM to remove old files beyond retention period."""
    cfg = ctx.obj["cfg"]
    table_path = cfg["delta"]["table_path"]
    retention = retention_hours or cfg["delta"]["vacuum_retention_hours"]

    if not Path(table_path).exists():
        click.echo(f"Delta table not found: {table_path}")
        return

    dt = DeltaTable(table_path)
    current_version = dt.version()
    files_before = len(dt.files())

    click.echo(f"Table:     {table_path}")
    click.echo(f"Version:   {current_version}")
    click.echo(f"Files:     {files_before:,}")
    click.echo(f"Retention: {retention} hours")
    click.echo()

    if dry_run:
        click.echo("[DRY RUN] Would vacuum files older than "
                    f"{retention} hours. No files removed.")
        return

    removed = dt.vacuum(
        retention_hours=retention,
        enforce_retention_duration=True,
        dry_run=False,
    )

    click.echo(f"Vacuum complete: {len(removed)} files removed.")


@cli.command()
@click.option("--target-size", type=int, default=256, help="Target file size in MB")
@click.pass_context
def compact(ctx, target_size):
    """Compact small Delta files into larger ones (optimize/Z-ORDER)."""
    cfg = ctx.obj["cfg"]
    table_path = cfg["delta"]["table_path"]

    if not Path(table_path).exists():
        click.echo(f"Delta table not found: {table_path}")
        return

    dt = DeltaTable(table_path)
    files_before = len(dt.files())
    size_before = _dir_size(Path(table_path))

    click.echo(f"Table:        {table_path}")
    click.echo(f"Files before: {files_before:,}")
    click.echo(f"Size before:  {humanize.naturalsize(size_before)}")
    click.echo(f"Target size:  {target_size} MB per file")
    click.echo()

    result = dt.optimize.compact()
    files_after = len(dt.files())
    size_after = _dir_size(Path(table_path))

    click.echo(f"Files after:  {files_after:,}")
    click.echo(f"Size after:   {humanize.naturalsize(size_after)}")
    click.echo(f"Metrics:      {result}")

    # Run vacuum to clean up compacted files
    click.echo("\nRunning vacuum to remove compacted fragments...")
    dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    click.echo("Compact + vacuum complete.")


@cli.command()
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def purge(ctx, yes):
    """Delete the entire Delta table (irreversible!)."""
    cfg = ctx.obj["cfg"]
    table_path = Path(cfg["delta"]["table_path"])
    checkpoint_path = Path(cfg["spark"]["local_checkpoint"])

    if not table_path.exists():
        click.echo("Nothing to purge — table does not exist.")
        return

    size = _dir_size(table_path)
    files = _file_count(table_path)

    click.echo(f"Table: {table_path}")
    click.echo(f"Size:  {humanize.naturalsize(size)}")
    click.echo(f"Files: {files:,}")

    if not yes:
        click.confirm(
            "\nThis will PERMANENTLY DELETE the Delta table and checkpoints. Continue?",
            abort=True,
        )

    click.echo("Deleting Delta table...")
    shutil.rmtree(table_path, ignore_errors=True)

    if checkpoint_path.exists():
        click.echo("Deleting checkpoints...")
        shutil.rmtree(checkpoint_path, ignore_errors=True)

    click.echo("Purge complete.")


@cli.command("kafka-reset")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def kafka_reset(ctx, yes):
    """Delete and recreate the Kafka topic (requires kafka-python)."""
    cfg = ctx.obj["cfg"]
    kafka_cfg = cfg["kafka"]

    if not yes:
        click.confirm(
            f"\nThis will DELETE topic '{kafka_cfg['topic']}' and all its data. Continue?",
            abort=True,
        )

    try:
        from kafka.admin import KafkaAdminClient, NewTopic

        admin = KafkaAdminClient(bootstrap_servers=kafka_cfg["bootstrap_servers"])

        # Delete topic
        try:
            admin.delete_topics([kafka_cfg["topic"]])
            click.echo(f"Deleted topic: {kafka_cfg['topic']}")
        except Exception as e:
            click.echo(f"Delete failed (may not exist): {e}")

        # Recreate topic
        topic = NewTopic(
            name=kafka_cfg["topic"],
            num_partitions=kafka_cfg["partitions"],
            replication_factor=1,
        )
        admin.create_topics([topic])
        click.echo(f"Recreated topic: {kafka_cfg['topic']} ({kafka_cfg['partitions']} partitions)")
        admin.close()

    except ImportError:
        click.echo("kafka-python not installed. Run: pip install kafka-python")
    except Exception as e:
        click.echo(f"Kafka admin error: {e}")


@cli.command("full-reset")
@click.option("--yes", is_flag=True, help="Skip all confirmation prompts")
@click.pass_context
def full_reset(ctx, yes):
    """Nuclear option: delete Delta table, checkpoints, AND reset Kafka topic."""
    cfg = ctx.obj["cfg"]

    if not yes:
        click.confirm(
            "\nThis will DELETE the Delta table, checkpoints, and Kafka topic data. Continue?",
            abort=True,
        )

    # Purge Delta + checkpoints
    table_path = Path(cfg["delta"]["table_path"])
    checkpoint_path = Path(cfg["spark"]["local_checkpoint"])

    if table_path.exists():
        click.echo(f"Deleting Delta table: {table_path}")
        shutil.rmtree(table_path, ignore_errors=True)
    else:
        click.echo("Delta table does not exist — skipping.")

    if checkpoint_path.exists():
        click.echo(f"Deleting checkpoints: {checkpoint_path}")
        shutil.rmtree(checkpoint_path, ignore_errors=True)
    else:
        click.echo("Checkpoints do not exist — skipping.")

    # Kafka reset
    click.echo("\nResetting Kafka topic...")
    ctx.invoke(kafka_reset, yes=True)

    # Clean Spark metadata
    for meta_dir in ["spark-warehouse", "metastore_db"]:
        p = Path(meta_dir)
        if p.exists():
            click.echo(f"Deleting: {p}")
            shutil.rmtree(p, ignore_errors=True)

    derby_log = Path("derby.log")
    if derby_log.exists():
        derby_log.unlink()

    click.echo("\nFull reset complete.")


@cli.command()
@click.pass_context
def status(ctx):
    """Show current state: table size, checkpoint size, file counts."""
    cfg = ctx.obj["cfg"]
    table_path = Path(cfg["delta"]["table_path"])
    checkpoint_path = Path(cfg["spark"]["local_checkpoint"])

    click.echo("=" * 50)
    click.echo("  CLEANUP STATUS")
    click.echo("=" * 50)

    # Delta table
    if table_path.exists():
        size = _dir_size(table_path)
        files = _file_count(table_path)
        try:
            dt = DeltaTable(str(table_path))
            version = dt.version()
            delta_files = len(dt.files())
            click.echo(f"\n  Delta Table:  {table_path}")
            click.echo(f"  Version:      {version}")
            click.echo(f"  Delta Files:  {delta_files:,}")
            click.echo(f"  Total Files:  {files:,} (incl. _delta_log)")
            click.echo(f"  Total Size:   {humanize.naturalsize(size)}")
        except Exception:
            click.echo(f"\n  Delta Table:  {table_path}")
            click.echo(f"  Total Files:  {files:,}")
            click.echo(f"  Total Size:   {humanize.naturalsize(size)}")
    else:
        click.echo(f"\n  Delta Table:  (not found)")

    # Checkpoints
    if checkpoint_path.exists():
        cp_size = _dir_size(checkpoint_path)
        cp_files = _file_count(checkpoint_path)
        click.echo(f"\n  Checkpoints:  {checkpoint_path}")
        click.echo(f"  Files:        {cp_files:,}")
        click.echo(f"  Size:         {humanize.naturalsize(cp_size)}")
    else:
        click.echo(f"\n  Checkpoints:  (not found)")

    # Spark metadata
    for meta_dir in ["spark-warehouse", "metastore_db"]:
        p = Path(meta_dir)
        if p.exists():
            click.echo(f"\n  {meta_dir}:  {humanize.naturalsize(_dir_size(p))}")

    click.echo("\n" + "=" * 50)


if __name__ == "__main__":
    cli()
