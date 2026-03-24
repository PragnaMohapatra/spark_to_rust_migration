# Spark-to-Rust Migration — Financial Transactions Pipeline

A high-throughput data pipeline that generates synthetic financial transaction data, streams it through Kafka, processes it with Spark Structured Streaming, and writes to Delta Lake. Designed as a baseline Spark implementation for benchmarking against a future Rust port.

## Architecture

```
┌──────────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Data Generator   │────▶│    Kafka     │────▶│  Spark Streaming │────▶│  Delta Lake  │
│  (Python, 4 thr) │     │ (8 partns)  │     │  (Structured)    │     │  (Parquet)   │
│  ~5 GB / run      │     │             │     │                  │     │              │
└──────────────────┘     └─────────────┘     └──────────────────┘     └──────────────┘
                                                                            │
                                                                ┌───────────┴──────────┐
                                                                │                      │
                                                          ┌─────▼──────┐    ┌──────────▼──┐
                                                          │Delta Viewer│    │Cleanup Tool  │
                                                          │ (CLI/SQL)  │    │(vacuum/purge)│
                                                          └────────────┘    └─────────────┘
```

**Data domain**: Synthetic financial transactions (25 fields per record, ~500 bytes each).

## Components

| Component | Path | Description |
|-----------|------|-------------|
| Data Generator | `data_generator/` | Multi-threaded Kafka producer using Faker. Pumps 5 GB per run. |
| Spark Streaming | `spark_jobs/streaming_job.py` | Structured Streaming: Kafka → parse → enrich → Delta Lake |
| Spark Batch | `spark_jobs/batch_job.py` | One-shot batch reprocessing of Kafka topic into Delta |
| Delta Viewer | `delta_viewer/` | Rich CLI to inspect schema, stats, history, partitions, and run SQL |
| Cleanup Tool | `cleanup/` | Vacuum, compact, purge Delta tables; reset Kafka topics |
| Infrastructure | `docker-compose.yml` | Zookeeper + Kafka (8 partitions) + Spark master/worker |
| Config | `config/app_config.yaml` | Centralized pipeline configuration |

## Quick Start

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Java 11+ (for local Spark)

### 1. Setup

```bash
# Linux/macOS
./scripts/setup.sh

# Windows
.\scripts\setup.ps1
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

Wait for Kafka to be healthy (~15s), then create the topic:

```bash
docker compose up kafka-setup
```

### 3. Generate Data (5 GB)

```bash
# Activate venv first
source .venv/bin/activate          # Linux/macOS
.\.venv\Scripts\Activate.ps1       # Windows

# Generate 5 GB (default)
python -m data_generator.run

# Generate 1 GB (quick test)
python -m data_generator.run --target-gb 1

# Generate 10 GB with 8 threads
python -m data_generator.run --target-gb 10 --threads 8
```

### 4. Run Spark Streaming Job

**Local mode** (no cluster needed):
```bash
python spark_jobs/streaming_job.py --local
```

**Docker mode** (uses Spark cluster):
```bash
docker exec spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
    /opt/spark-jobs/streaming_job.py
```

### 5. Inspect Delta Table

```bash
python -m delta_viewer.viewer info          # Table metadata
python -m delta_viewer.viewer schema        # Column types
python -m delta_viewer.viewer stats         # Row counts, null %, amount distribution
python -m delta_viewer.viewer sample        # Preview 10 rows
python -m delta_viewer.viewer history       # Commit log
python -m delta_viewer.viewer partitions    # Partition layout

# SQL queries (requires: pip install duckdb)
python -m delta_viewer.viewer query --sql "SELECT currency, COUNT(*) cnt FROM txn GROUP BY currency ORDER BY cnt DESC"
```

### 6. Cleanup

```bash
python -m cleanup.cleanup status            # What exists
python -m cleanup.cleanup vacuum            # Remove old Delta files
python -m cleanup.cleanup compact           # Merge small files
python -m cleanup.cleanup purge             # Delete Delta table
python -m cleanup.cleanup kafka-reset       # Reset Kafka topic
python -m cleanup.cleanup full-reset --yes  # Everything
```

### 7. Stop

```bash
docker compose down
```

## Configuration

All settings are in [`config/app_config.yaml`](config/app_config.yaml):

| Section | Key Settings |
|---------|-------------|
| `kafka` | bootstrap servers, topic, partitions |
| `generator` | `target_bytes` (5 GB default), batch size, thread count |
| `spark` | master URL, trigger interval, checkpoint/output paths, shuffle partitions |
| `delta` | table path, vacuum retention, compact threshold |

## Data Schema

Each financial transaction record contains 25 fields:

| Field | Type | Example |
|-------|------|---------|
| `transaction_id` | UUID | `a1b2c3d4-...` |
| `timestamp` | ISO-8601 | `2026-03-21T14:30:00+00:00` |
| `sender_account` | string(16) | `4829103847561029` |
| `amount` | float | `12499.50` |
| `currency` | ISO-4217 | `USD` |
| `transaction_type` | enum | `WIRE`, `ACH`, `CARD`, `P2P`, `CHECK` |
| `status` | enum | `COMPLETED`, `PENDING`, `FAILED`, `REVERSED` |
| `risk_score` | float | `0.0342` |
| `is_flagged` | bool | `false` |
| `category` | enum | `RETAIL`, `CORPORATE`, `GOVERNMENT`, `INTERBANK` |
| `channel` | enum | `ONLINE`, `BRANCH`, `ATM`, `MOBILE`, `API` |
| ... | | *(25 fields total — see `data_generator/schemas.py`)* |

**Enriched columns** added by Spark: `transaction_date`, `txn_year`, `txn_month`, `amount_bucket`

## 5 GB Throughput Design

- **Record size**: ~500 bytes JSON → ~10M records per 5 GB
- **Generator**: 4 threads × 10K batch = 40K records/flush → ~100–200 MB/s throughput
- **Kafka**: 8 partitions, LZ4 compression, 128 MB producer buffer
- **Spark**: 500K offsets/trigger, 8 shuffle partitions, append mode

## Project Structure

```
spark_to_rust_migration/
├── config/
│   └── app_config.yaml          # Pipeline configuration
├── data_generator/
│   ├── __init__.py
│   ├── generator.py             # Multi-threaded Kafka producer
│   ├── run.py                   # CLI entry point
│   └── schemas.py               # Transaction data schema
├── spark_jobs/
│   ├── streaming_job.py         # Kafka → Delta streaming
│   └── batch_job.py             # Kafka → Delta batch
├── delta_viewer/
│   ├── __init__.py
│   └── viewer.py                # Rich CLI table inspector
├── cleanup/
│   ├── __init__.py
│   └── cleanup.py               # Vacuum, compact, purge, reset
├── scripts/
│   ├── setup.sh                 # Linux/macOS setup
│   ├── setup.ps1                # Windows setup
│   ├── start_pipeline.sh        # Start everything
│   └── stop_pipeline.sh         # Stop everything
├── docker-compose.yml           # Kafka + Spark infrastructure
├── requirements.txt             # Python dependencies
├── .gitignore
├── LICENSE
└── README.md
```

## Future: Rust Migration

This Spark pipeline serves as the **baseline** for performance benchmarking. The migration plan:

1. **Phase 1** (this repo): Fully working Spark pipeline with 5 GB data generation
2. **Phase 2**: Rust implementation using `rdkafka` + `delta-rs` + `arrow-rs`
3. **Phase 3**: Side-by-side benchmarking — throughput, latency, resource usage

## License

MIT
