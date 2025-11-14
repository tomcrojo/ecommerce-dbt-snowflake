# E-Commerce Analytics — dbt + Snowflake

ELT pipeline that transforms raw Spanish e-commerce transactional data into a clean dimensional model (star schema) using dbt Core on Snowflake. Implements a staging → intermediate → marts layer structure with data quality tests, SCD2 snapshots, and CI/CD via GitHub Actions. Includes batch ingestion from a live API and a Streamlit analytics dashboard. Built as a portfolio project demonstrating dimensional modeling, data engineering best practices, and Snowflake proficiency for Madrid-based roles.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          End-to-End Data Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐                       │
│  │  DummyJSON   │    │  Batch Ingestor  │    │  CSV Staging │                       │
│  │  API         │───▶│  (5 min batches) │───▶│  data/       │                       │
│  │              │    │                  │    │  raw_batches/│                       │
│  │  /products   │    │  scripts/        │    │              │                       │
│  │  /carts      │    │  batch_ingestor  │    └──────┬───────┘                       │
│  │  /users      │    │  .py             │           │                               │
│  └──────────────┘    └──────────────────┘           │                               │
│                                                      ▼                               │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────┐    ┌───────────────┐           │
│  │          │    │   STAGING   │    │ INTERMEDIATE │    │    MARTS      │           │
│  │  SEEDS   │───▶│  (views)    │───▶│   (tables)   │───▶│   (tables)    │           │
│  │          │    │             │    │              │    │               │           │
│  │ raw_     │    │ stg_orders  │    │ int_order_   │    │ fct_orders    │           │
│  │ orders   │    │ stg_cust.   │    │ items_enr.   │    │ fct_order_    │           │
│  │ raw_     │    │ stg_prod.   │    │ int_cust.    │    │   items       │           │
│  │ cust.    │    │ stg_order_  │    │   orders     │    │ dim_customers │           │
│  │ prod.    │    │   items     │    │              │    │ dim_products  │           │
│  │ raw_     │    │             │    │              │    │ rpt_daily_    │           │
│  │ order_   │    │             │    │              │    │   revenue     │           │
│  │ items    │    │             │    │              │    │               │           │
│  └──────────┘    └─────────────┘    └──────────────┘    └───────┬───────┘           │
│                                                                  │                   │
│                                                                  ▼                   │
│                                                        ┌─────────────────┐          │
│                                                        │   Streamlit     │          │
│                                                        │   Dashboard     │          │
│                                                        │                 │          │
│                                                        │  KPIs, charts,  │          │
│                                                        │  filters,       │          │
│                                                        │  refresh        │          │
│                                                        └─────────────────┘          │
│                                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────────────┐          │
│  │  SNAPSHOTS   │    │    TESTS     │    │         CI/CD                 │          │
│  │              │    │              │    │                               │          │
│  │ scd2_       │    │ Generic +    │    │ GitHub Actions                │          │
│  │ customers   │    │ singular     │    │ seed → run → test → docs      │          │
│  └──────────────┘    └──────────────┘    └───────────────────────────────┘          │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Batch Ingestion

The project includes a batch ingestion script that fetches data from the [DummyJSON](https://dummyjson.com) API and writes timestamped JSON files to `data/raw_batches/`.

```
DummyJSON API  →  Batch Ingestor (5 min)  →  JSON Staging  →  dbt  →  Snowflake  →  Dashboard
```

### Usage

```bash
# One-shot dry run (fetch + print, no files written)
python scripts/batch_ingestor.py --dry-run

# Generate 3 batches with default 5-minute interval
python scripts/batch_ingestor.py --count 3

# Run forever (production mode)
python scripts/batch_ingestor.py

# Custom interval: 1 batch every 60 seconds
python scripts/batch_ingestor.py --interval 60
```

Each batch produces a file like `data/raw_batches/batch_2026-03-21T10:05:00.json` containing:
- `orders` — order_id, customer_id, order_date, status, region
- `customers` — customer_id, name, email, country
- `products` — product_id, name, category, price
- `order_items` — order_item_id, order_id, product_id, quantity, unit_price

## Seed Data Generation

For large-scale testing, a Faker-based script generates 100K+ rows across all seed tables:

```bash
python scripts/generate_large_dataset.py
```

This replaces the CSV files in `seeds/` with:
- `raw_customers.csv` — 10,000 Spanish customers
- `raw_products.csv` — 200 products across 5 categories
- `raw_orders.csv` — 100,000 orders (2023-01-01 to 2026-03-21)
- `raw_order_items.csv` — ~250,000 line items

Referential integrity is enforced. All data uses Spanish names, cities, and EUR prices.

## Streamlit Dashboard

Interactive analytics dashboard built with Streamlit and Plotly.

### Launch

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

### Features

The dashboard has two tabs: **Business Metrics** and **Data Quality**.

#### Business Metrics

- **KPI cards**: Total Revenue, Total Orders, Avg Order Value, Active Customers
- **Revenue over time** — monthly line chart
- **Orders by region** — bar chart with Spanish cities
- **Top categories by revenue** — horizontal bar chart
- **Customer segment breakdown** — pie chart (new / returning / churned)
- **Order status distribution** — donut chart
- **Sidebar filters**: date range, region multi-select

#### Data Quality

- **dbt test results** — reads `target/run_results.json` after `dbt test`, shows pass/fail counts and failed test details
- **Dataset volume** — row and column counts from seed CSVs with a bar chart
- **Null rate analysis** — per-column null rates for each seed dataset
- **Referential integrity** — checks for orphaned keys across orders, items, customers, and products
- **Refresh Data** button — re-runs batch ingestor and clears cache

Works standalone with simulated data if no Snowflake connection is available.

## Data Model (Star Schema)

```
                        ┌───────────────────┐
                        │   dim_customers   │
                        │───────────────────│
                        │ customer_id (PK)  │
                        │ customer_name     │
                        │ email             │
                        │ country           │
                        │ signup_date       │
                        │ customer_segment  │
                        │ lifetime_value_   │
                        │   tier            │
                        │ first_order_date  │
                        │ last_order_date   │
                        │ total_orders      │
                        │ total_revenue     │
                        └────────┬──────────┘
                                 │
                                 │ 1:N
┌───────────────────┐     ┌──────┴──────────┐     ┌───────────────────┐
│   dim_products    │◀────│   fct_orders    │────▶│  fct_order_items  │
│───────────────────│     │─────────────────│     │───────────────────│
│ product_id (PK)   │     │ order_id (PK)   │     │ order_item_id (PK)│
│ product_name      │     │ customer_id (FK)│     │ order_id (FK)     │
│ category          │     │ order_date      │     │ customer_id (FK)  │
│ subcategory       │     │ order_year      │     │ product_id (FK)   │
│ price_tier        │     │ order_month     │     │ product_name      │
│ unit_price        │     │ region          │     │ product_category  │
│ is_active         │     │ total_amount    │     │ quantity          │
└───────────────────┘     │ item_count      │     │ unit_price        │
       ▲                  │ is_completed    │     │ line_total        │
       │                  └────────┬────────┘     │ order_date        │
       │                           │              │ region            │
       └───────────────────────────┘              └───────────────────┘
                          N:1                             N:1
```

## Layer Descriptions

| Layer | Materialization | Purpose |
|-------|----------------|---------|
| **Staging** | `view` | 1:1 mapping with source tables. Column renaming, recasting, light cleaning (lowercase emails, standardized country codes). No business logic joins. Serves as a stable contract for upstream layers. |
| **Intermediate** | `table` | Cross-entity joins and business logic enrichment. Pre-computes expensive joins (order items + products + orders) so marts stay simple. Never queried by BI tools directly. |
| **Marts** | `table` / `incremental` | Star schema facts and dimensions ready for BI consumption. `fct_orders` is incremental on `order_date` for efficiency. Dimensions implement derived attributes (customer segmentation, lifetime value tiers). |
| **Snapshots** | `scd2` | Type-2 slowly changing dimensions tracking historical changes to customer attributes (email, country, name). |

## Setup

### Prerequisites

- Snowflake account (trial is sufficient)
- Python 3.9+ and pip
- dbt Core 1.7+

### 1. Clone and install

```bash
git clone https://github.com/tomcrojo/ecommerce-dbt-snowflake.git
cd ecommerce-dbt-snowflake
pip install -r requirements.txt
pip install -r dashboard/requirements.txt   # for Streamlit dashboard
dbt deps
```

### 2. Configure Snowflake connection

Set environment variables:

```bash
export DBT_SNOWFLAKE_USER="your_username"
export DBT_SNOWFLAKE_PASSWORD="your_password"
```

Edit `profiles.yml` — update the `account` field to your Snowflake account identifier (e.g., `abc123.eu-west-1`).

### 3. Generate seed data

```bash
# Option A: Use existing small seed files (25 products, 60 customers, 100 orders)
dbt seed

# Option B: Generate large seed dataset (200 products, 10K customers, 100K orders)
python scripts/generate_large_dataset.py
dbt seed
```

### 4. Run batch ingestion (optional)

```bash
# Run 3 test batches
python scripts/batch_ingestor.py --count 3 --dry-run
```

### 5. Run models

```bash
dbt run
```

### 6. Run tests

```bash
dbt test
```

### 7. Launch dashboard

```bash
streamlit run dashboard/app.py
```

### 8. Generate documentation

```bash
dbt docs generate
dbt docs serve
```

## Data Quality

| Test Type | Examples |
|-----------|---------|
| **Schema tests** | `unique`, `not_null` on all primary/foreign keys; `accepted_values` on status, region, category, price_tier |
| **Referential integrity** | `relationships` tests ensuring foreign keys in facts point to existing dimension keys |
| **Custom singular tests** | Revenue consistency (sum of line items matches order total); no future order dates; no negative quantities |
| **Custom generic tests** | `test_no_future_dates` macro available for any date column |
| **Source freshness** | Configured on staging sources with `warn_after: 24 hours` / `error_after: 48 hours` |
| **Snapshots** | SCD2 on customers to detect and track data drift |

Test severity is configured with `error` for critical checks (uniqueness, not-null) and `warn` for advisory checks (freshness).

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Staging views, marts tables** | Staging is cheap to recompute and benefits from always reflecting source state. Marts are materialized to serve BI queries efficiently. |
| **`fct_orders` is incremental** | Order tables grow over time; incremental materialization processes only new/changed rows based on `order_date`, reducing compute costs. |
| **SCD2 snapshot on customers** | Customer attributes (email, country) can change. SCD2 preserves history, enabling point-in-time analysis and attribution. |
| **Intermediate layer** | Isolates complex joins (items × products × orders) from marts. Marts stay readable; intermediate models are the "dirty work" layer. |
| **Cents stored as integers** | Prices in `unit_price_cents` avoid floating-point precision issues. Conversion via `cents_to_euros` macro at the presentation layer. |
| **Spanish regional data** | Regions correspond to Spanish autonomous communities, reflecting a Madrid-based e-commerce operation. Currency is EUR. |
| **`generate_schema_name` macro** | Dev targets get a prefixed schema (e.g., `dev_staging`) to avoid collisions in shared Snowflake environments. |
| **Batch ingestion over streaming** | Simpler to operate and debug; 5-minute windows are sufficient for analytics use cases. |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Data warehouse | Snowflake |
| Transformation | dbt Core 1.7 |
| Testing | dbt built-in tests, dbt-utils, dbt-expectations |
| Snapshots | dbt SCD2 snapshots |
| Data source | DummyJSON API (carts, users) |
| Batch ingestion | Python + requests |
| Seed generation | Python + Faker |
| Dashboard | Streamlit + Plotly |
| CI/CD | GitHub Actions |
| Version control | Git / GitHub |
| Orchestration | dbt CLI (compatible with dbt Cloud, Airflow, Dagster) |
| Language | SQL, Jinja2, Python |
| Package management | dbt deps (dbt-utils, dbt-expectations), pip |

## Local Development with Docker

Run the entire pipeline locally without Snowflake credentials. PostgreSQL stands in for Snowflake.

### Architecture

```
┌───────────────────────────────────────────────────┐
│  Local Docker Environment                         │
│                                                   │
│  ┌────────────┐   ┌─────┐   ┌──────────────┐    │
│  │ DummyJSON  │──►│batch│──►│  PostgreSQL   │    │
│  │   API      │   │ingst│   │  (Snowflake   │    │
│  └────────────┘   └─────┘   │   substitute) │    │
│                             └───────┬───────┘    │
│                                     │            │
│                             ┌───────▼───────┐    │
│                             │     dbt       │    │
│                             │  (transform)  │    │
│                             └───────┬───────┘    │
│                                     │            │
│                             ┌───────▼───────┐    │
│                             │  Streamlit    │    │
│                             │  Dashboard    │    │
│                             └───────────────┘    │
└───────────────────────────────────────────────────┘
```

### Prerequisites

- Docker Desktop

### Quick start

```bash
./scripts/start_local.sh
```

This starts PostgreSQL, loads seed data, runs all dbt models, runs tests, and launches the Streamlit dashboard.

### Run dbt commands

```bash
# Run models
./scripts/run_dbt.sh run

# Run tests
./scripts/run_dbt.sh test

# Generate docs
./scripts/run_dbt.sh "docs generate"

# Any dbt command
./scripts/run_dbt.sh [command]
```

### Access services

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| PostgreSQL | localhost:5432 (dbt_user / dbt_pass / ecommerce) |

### Start the batch ingestor

```bash
docker-compose --profile ingest up -d
```

### Stop everything

```bash
./scripts/stop_local.sh
```

### Notes

- The PostgreSQL profile (`profiles_local/profiles.yml`) mirrors the Snowflake schema using the `public` schema. Models are compatible, but Snowflake-specific SQL (e.g., `REPLACE`, `CURRENT_TIMESTAMP` nuances) may need minor adjustments — check `Dockerfile.dbt` for details.
- Seed CSVs are available inside PostgreSQL at startup, but dbt `seed` still needs to run to load them into the correct schema.
