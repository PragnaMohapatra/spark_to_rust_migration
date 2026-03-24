#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# start_pipeline.sh — Start the full Spark streaming pipeline
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "================================================="
echo "  Financial Transactions Pipeline — START"
echo "================================================="

# 1. Start infrastructure
echo ""
echo "[1/4] Starting Kafka + Spark infrastructure..."
docker compose up -d zookeeper kafka spark-master spark-worker

echo ""
echo "[2/4] Waiting for Kafka to be healthy..."
timeout=60
elapsed=0
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ $elapsed -ge $timeout ]; then
        echo "ERROR: Kafka failed to start within ${timeout}s"
        exit 1
    fi
    echo "  Waiting... (${elapsed}s)"
done
echo "  Kafka is healthy."

# 2. Create topic
echo ""
echo "[3/4] Creating Kafka topic..."
docker compose up kafka-setup

# 3. Start Spark streaming job
echo ""
echo "[4/4] Submitting Spark streaming job..."
docker exec spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /opt/spark-jobs/streaming_job.py &

SPARK_PID=$!
echo "  Spark streaming job started (PID: $SPARK_PID)"

echo ""
echo "================================================="
echo "  Pipeline is running!"
echo "  Kafka UI:   http://localhost:9092"
echo "  Spark UI:   http://localhost:8080"
echo ""
echo "  To generate data:"
echo "    python -m data_generator.run --target-gb 5"
echo ""
echo "  To stop:"
echo "    ./scripts/stop_pipeline.sh"
echo "================================================="
