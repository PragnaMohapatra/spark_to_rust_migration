# Spark vs Rust for Streaming Pipelines: What a 9.89 Million Row Benchmark Actually Showed

When teams talk about replacing Spark with Rust, the conversation usually jumps straight to performance. That is understandable, but it is also incomplete. In practice, the harder question is not whether Rust can be faster. The harder question is whether a Rust pipeline can deliver a meaningful speedup while preserving the operational guardrails, recoverability, and observability that people already rely on in a Spark-based platform.

This project was built to answer that question with a real streaming workload instead of a synthetic microbenchmark. The repository generates roughly 5 GB of synthetic financial transaction data, pushes it through Kafka, processes it with both Spark Structured Streaming and a Rust pipeline, and writes both outputs to Delta Lake. The benchmark report is captured in [pipeline_comparison_report.html](../pipeline_comparison_report.html), and the architecture used for this article is captured in [pipeline_architecture.pdf](./pipeline_architecture.pdf).

The result is not subtle: Rust wins decisively on latency and cumulative processing time for this workload. But Spark still provides real advantages in ecosystem maturity, operator familiarity, and SQL-native extensibility. The more useful conclusion is not "Spark bad, Rust good." The useful conclusion is where each engine pays for itself.

## The Benchmark in One View

The comparison report shows the following headline numbers for the transaction streaming path:

- Comparable streaming rows: 9,893,825
- Spark wall-clock time: 20 m 5 s
- Rust wall-clock time: 12 m 12 s
- Rust wall-clock speedup: 1.6x
- Spark average batch latency: 67,009 ms
- Rust average batch latency: 9,184 ms
- Per-batch latency speedup: 7.3x
- Spark batches: 20
- Rust batches: 52

The near-full-batch comparison from the HTML report is even more telling because it reduces the noise from Kafka idle gaps and end-of-stream behavior:

- Average transform time: Spark 6,891 ms, Rust 767 ms, Rust 9.0x faster
- Average write time: Spark 61,585 ms, Rust 3,356 ms, Rust 18.3x faster
- Average total batch time: Spark 68,476 ms, Rust 7,390 ms, Rust 9.3x faster
- Throughput: Spark 7,302 rows/s, Rust 33,831 rows/s, Rust 4.6x faster

Those numbers matter because they show the speedup is not coming from only one hot path. Rust is faster in transformation work and dramatically faster in Delta write time for this setup.

## What We Built

At a high level, the system has two planes.

### Data plane

The data plane carries the workload itself:

- A Python data generator produces synthetic financial transactions and related account updates.
- Kafka stores two topics: `financial_transactions` and `account_updates`.
- Spark Structured Streaming consumes both topics with separate jobs.
- A Rust pipeline consumes both topics with separate commands.
- Both implementations write results to Delta Lake tables.

The data plane is intentionally symmetric so that Spark and Rust are compared against the same source topics, schemas, and destination semantics.

### Control plane

The control plane contains the operational provisions that make the benchmark repeatable and inspectable:

- A Streamlit dashboard starts and stops jobs, reads metrics, and surfaces the comparison view.
- Metrics files record Spark and Rust transaction and account pipeline performance.
- Checkpoint volumes preserve Spark streaming progress and Rust offset databases.
- Delta Viewer and cleanup tooling provide inspection, vacuum, compaction, purge, and Kafka reset workflows.
- Docker Compose defines and wires the runtime services and shared mounted volumes.

That separation matters. A high-performance data plane is not enough on its own. For migration work to be credible, it also needs a control plane that lets engineers operate, recover, validate, and compare both stacks side by side.

## Runtime Setup

The environment is intentionally simple and reproducible.

### Infrastructure

The core runtime is defined in [docker-compose.yml](../docker-compose.yml):

- `zookeeper`
- `kafka`
- `kafka-setup`
- `spark-master`
- `spark-worker`
- `spark-worker-2`
- `rust-pipeline`
- `dashboard`

Kafka is configured with 8 partitions and 7-day retention. The `account_updates` topic is also compacted, which matches the account upsert use case.

Spark runs as a small cluster with one master and two workers. Each worker is configured for 4 cores and 4 GB of memory. Shared project folders are mounted into the Spark containers so jobs, logs, Delta output, and checkpoints are visible across the environment.

The Rust pipeline runs in its own container, built from a multi-stage Dockerfile. The container mounts the same config, Delta output, checkpoint, and logs directories as Spark. That is important because both implementations are being evaluated within the same operational envelope.

### Shared configuration

Most runtime decisions are centralized in [app_config.yaml](../config/app_config.yaml):

- Kafka bootstrap servers and topic names
- Generator target size, thread count, and batch size
- Spark trigger interval, checkpoint paths, Delta output paths, and offsets-per-trigger
- Rust batch size, flush interval, Delta output paths, and checkpoint database paths
- Delta retention and compaction thresholds

This is one of the more useful parts of the design. Instead of hard-coding behavior in multiple runtimes, the project uses a shared config contract so Spark and Rust stay aligned on the same workload definition.

## How the Workload Was Built

The generator produces synthetic financial transactions in Python using Faker. The run is tuned for scale, not just convenience:

- Target volume: 5 GB
- Parallel producer threads: 4
- Producer batch size: 10,000
- Estimated average record size: about 500 bytes

For each business transaction, the generator emits:

- 1 event to `financial_transactions`
- 2 events to `account_updates`

That means the benchmark is not only measuring an append-heavy transaction stream. It is also exercising account-state maintenance through upsert semantics.

## How to Build and Run the Pipelines

The article would be incomplete without the actual run model. The following is the practical sequence.

### 1. Start the infrastructure

From the repository root:

```powershell
docker compose up -d
docker compose up kafka-setup
```

This brings up Kafka, Spark, the Rust container, and the dashboard. The `kafka-setup` service creates and configures both Kafka topics.

### 2. Generate the test data

The generator can be run locally after environment setup:

```powershell
python -m data_generator.run --target-gb 5
```

That produces the full benchmark volume used in the final comparison.

### 3. Run the Spark transaction pipeline

Spark can run locally, but the repo is structured primarily around the Dockerized cluster path. The startup script in [start_pipeline.sh](../scripts/start_pipeline.sh) uses `spark-submit` from the master container:

```powershell
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-jobs/streaming_job.py
```

For account upserts, the matching Spark job is the account pipeline:

```powershell
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-jobs/account_upsert_job.py
```

Spark uses a `10 seconds` trigger interval and `500000` max offsets per trigger in the shared configuration.

### 4. Build the Rust pipeline

The Rust implementation is defined in [rust_pipeline/Dockerfile](../rust_pipeline/Dockerfile) and [rust_pipeline/Cargo.toml](../rust_pipeline/Cargo.toml).

The Dockerfile uses a multi-stage build:

- Builder image: `rust:1.85-slim-bookworm`
- Runtime image: `debian:bookworm-slim`
- Release binary copied to `/usr/local/bin/rust-pipeline`

To build the container:

```powershell
docker compose build rust-pipeline
```

This compiles the release binary with:

- `opt-level = 3`
- `lto = "thin"`

Those choices are exactly what you want in a benchmarking context: optimized builds with link-time optimization, while still keeping compile cost reasonable.

### 5. Run the Rust transaction and account pipelines

The Rust CLI in [rust_pipeline/src/main.rs](../rust_pipeline/src/main.rs) exposes three commands: `stream`, `upsert`, and `validate`.

Run the transaction stream:

```powershell
docker exec rust-pipeline rust-pipeline --config /opt/config/app_config.yaml stream
```

Run the account upsert path:

```powershell
docker exec rust-pipeline rust-pipeline --config /opt/config/app_config.yaml upsert
```

Validate Rust output against Spark output:

```powershell
docker exec rust-pipeline rust-pipeline --config /opt/config/app_config.yaml validate --accounts
```

The Rust runtime settings in the config are important to understanding the benchmark result:

- Transaction batch size: 250,000
- Flush interval: 3 seconds
- Separate Delta outputs for transactions and accounts
- Separate SQLite-backed checkpoint databases for transaction and account consumers

## What Provisions Existed in the Control Plane

This is where the project is stronger than a one-off benchmark. It is not only a pair of data processors. It has supporting control-plane features that make the comparison operationally meaningful.

### 1. Dashboard-driven orchestration

The dashboard is the operator surface. It provides a place to start and stop jobs, inspect output, and review benchmark status. In migration work, that matters because engineers need a shared control surface while two implementations coexist.

### 2. Metrics as first-class outputs

The control plane persists metrics files for:

- Spark transaction stream
- Rust transaction stream
- Spark account upsert
- Rust account upsert

That is how the final HTML report was produced. Instead of relying on screenshots or manual timing, the benchmark records machine-readable metrics that can be rolled up into a repeatable report.

### 3. Checkpoint durability and replay safety

Spark and Rust both keep progress state:

- Spark uses checkpoint directories for transaction and account streams.
- Rust uses SQLite-based offset databases.

This is essential for migration testing because it lets both systems resume work, replay comparable datasets, and recover from interrupted runs without redesigning the benchmark every time.

### 4. Delta inspection and cleanup

The Delta Viewer and cleanup tooling give the control plane operational depth:

- schema and history inspection
- row-count and stats analysis
- vacuum and compaction
- full purge when resetting runs
- Kafka topic reset support

Without these tools, the benchmark would quickly become noisy because output state and topic state would bleed across runs.

### 5. Docker Compose as the system boundary

Compose is more than just convenience here. It is the boundary that standardizes the runtime:

- consistent mounts
- consistent paths
- consistent container names
- consistent service startup order

That reduces one of the biggest risks in platform comparisons: accidental environmental skew.

## Why Rust Won This Benchmark

The report suggests three main reasons.

### 1. Lower engine overhead

Spark brings a large execution engine, scheduling model, JVM runtime, and micro-batch orchestration. Those are valuable when you need elasticity, distributed query planning, or a large SQL ecosystem. They are also overhead when the transformation logic is relatively direct and the deployment footprint is controlled.

Rust, by contrast, runs much closer to the metal. For this pipeline, that translated into meaningfully lower per-batch cost.

### 2. Much faster write path in this setup

The most dramatic gap in the HTML report is Delta write time: 61,585 ms for Spark versus 3,356 ms for Rust in near-full batches. That does not automatically mean Rust will always beat Spark on every storage pattern, but it does show that for this workload and this environment, the Rust write path was materially more efficient.

### 3. Tighter control over batching

The Rust pipeline was tuned with a `250000` row batch size and a `3` second flush interval. That produced 52 smaller batches across the comparable run, versus 20 larger Spark batches. Smaller, more frequent Rust batches still beat Spark decisively on latency, which makes the result hard to dismiss as simply a batching artifact.

## Where Spark Still Makes Sense

Even with a strong Rust result, Spark remains a sensible default when:

- the team needs native SQL-first processing
- jobs need rich joins, windowing, and ad hoc analytical composition
- existing operations are heavily Spark-oriented
- the platform benefits from managed Spark services and familiar debugging tools

Spark is not only a processing engine. It is also a very mature operating model.

## Where Rust Starts to Look Compelling

Rust becomes compelling when:

- latency matters more than engine abstraction
- the transformation logic is stable and explicit
- the team wants tighter control over memory and batching
- the workload is operationally predictable enough to justify a purpose-built runtime
- infrastructure cost and per-batch overhead are under pressure

That is the pattern this benchmark supports. The Rust implementation did not win because it was more magical. It won because the workload was structured enough that a focused runtime could outperform a general-purpose distributed engine.

## Final Takeaway

The most important lesson from this comparison is not that Spark should be replaced everywhere. The lesson is that a migration discussion gets better when it is grounded in complete systems, not isolated kernels.

This project compared Spark and Rust with:

- shared topics
- shared schemas
- shared Delta destinations
- shared configuration
- explicit control-plane provisions
- repeatable metrics and validation

Under those conditions, Rust processed the same 9,893,825-row streaming workload faster, with much lower batch latency and much lower cumulative processing time.

That makes Rust a serious candidate for targeted high-throughput, low-latency data-plane services. Spark still has a strong case where flexibility, ecosystem breadth, and operational familiarity matter more than raw efficiency. The right decision is not ideological. It depends on whether the workload rewards specialization.

## Source Artifacts

- Benchmark report: [pipeline_comparison_report.html](../pipeline_comparison_report.html)
- Architecture export: [pipeline_architecture.pdf](./pipeline_architecture.pdf)
- Draw.io source: [pipeline_architecture.drawio](./pipeline_architecture.drawio)
- Runtime definition: [docker-compose.yml](../docker-compose.yml)
- Shared config: [app_config.yaml](../config/app_config.yaml)
- Spark startup flow: [start_pipeline.sh](../scripts/start_pipeline.sh)
- Rust entry point: [main.rs](../rust_pipeline/src/main.rs)