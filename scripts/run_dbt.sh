#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

CMD="${1:-run}"
shift || true

docker-compose --profile dbt run --rm dbt dbt "$CMD" "$@"
