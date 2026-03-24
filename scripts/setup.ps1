<# 
.SYNOPSIS
    Windows setup script for the Financial Transactions Pipeline.

.DESCRIPTION
    Creates Python venv, installs dependencies, and prepares output directories.

.EXAMPLE
    .\scripts\setup.ps1
#>

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
Set-Location $ProjectDir

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  Financial Transactions Pipeline - SETUP"
Write-Host "=================================================" -ForegroundColor Cyan

# 1. Python venv
Write-Host ""
Write-Host "[1/3] Setting up Python virtual environment..." -ForegroundColor Yellow
if (-not (Test-Path ".venv")) {
    python -m venv .venv
    Write-Host "  Created .venv"
} else {
    Write-Host "  .venv already exists"
}

& .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip | Out-Null

# 2. Install dependencies
Write-Host ""
Write-Host "[2/3] Installing Python dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt

# 3. Output directories
Write-Host ""
Write-Host "[3/3] Creating output directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path delta_output, checkpoints, logs | Out-Null

Write-Host ""
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "  Activate the venv:  .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "  Next steps:"
Write-Host "    1. Start infrastructure:  docker compose up -d"
Write-Host "    2. Generate data:         python -m data_generator.run --target-gb 5"
Write-Host "    3. View Delta table:      python -m delta_viewer.viewer info"
Write-Host "=================================================" -ForegroundColor Cyan
