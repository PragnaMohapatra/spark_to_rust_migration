#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# stop_pipeline.sh — Stop all pipeline components
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "================================================="
echo "  Financial Transactions Pipeline — STOP"
echo "================================================="

echo ""
echo "Stopping Docker containers..."
docker compose down

echo ""
echo "Pipeline stopped."
echo "  Delta table data preserved at: ./delta_output/"
echo "  Checkpoints preserved at:      ./checkpoints/"
echo ""
echo "  To purge all data: python -m cleanup.cleanup full-reset --yes"
echo "================================================="
