#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "Stopping all services and removing volumes..."
docker-compose --profile dbt --profile ingest --profile dashboard down -v
echo "Done."
