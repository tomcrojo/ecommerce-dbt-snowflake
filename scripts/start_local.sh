#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

PROFILE_ARG="--profile dbt"

echo "==> Starting PostgreSQL and dashboard..."
docker-compose --profile dbt --profile dashboard up -d

echo "==> Waiting for PostgreSQL to be healthy..."
until docker-compose exec -T postgres pg_isready -U dbt_user -d ecommerce >/dev/null 2>&1; do
  sleep 1
done
echo "    PostgreSQL is ready."

echo "==> Installing dbt packages..."
docker-compose run --rm dbt dbt deps

echo "==> Loading seed data into PostgreSQL..."
docker-compose run --rm dbt dbt seed

echo "==> Running dbt transformations..."
docker-compose run --rm dbt dbt run

echo "==> Running dbt tests..."
docker-compose run --rm dbt dbt test

echo ""
echo "============================================"
echo "  Local environment is running!"
echo ""
echo "  Dashboard:  http://localhost:8501"
echo "  PostgreSQL: localhost:5432 (dbt_user / dbt_pass / ecommerce)"
echo "============================================"
echo ""
echo "To also start the batch ingestor:"
echo "  docker-compose --profile ingest up -d"
echo ""
echo "To run dbt commands:"
echo "  ./scripts/run_dbt.sh [command]"
echo "  e.g. ./scripts/run_dbt.sh test"
