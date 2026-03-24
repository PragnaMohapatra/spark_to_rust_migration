#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# setup.sh — First-time setup: install Python deps, build images
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "================================================="
echo "  Financial Transactions Pipeline — SETUP"
echo "================================================="

# 1. Python environment
echo ""
echo "[1/3] Setting up Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "  Created .venv"
else
    echo "  .venv already exists"
fi

source .venv/bin/activate
pip install --upgrade pip > /dev/null 2>&1

echo ""
echo "[2/3] Installing Python dependencies..."
pip install -r requirements.txt

# 3. Create output directories
echo ""
echo "[3/3] Creating output directories..."
mkdir -p delta_output checkpoints logs

echo ""
echo "================================================="
echo "  Setup complete!"
echo ""
echo "  Activate the venv:  source .venv/bin/activate"
echo ""
echo "  Next steps:"
echo "    1. Start infrastructure:  ./scripts/start_pipeline.sh"
echo "    2. Generate data:         python -m data_generator.run --target-gb 5"
echo "    3. View Delta table:      python -m delta_viewer.viewer info"
echo "================================================="
