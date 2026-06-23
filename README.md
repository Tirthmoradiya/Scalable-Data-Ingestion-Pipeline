# Scalable Data Ingestion Pipeline

A production-grade Python data pipeline that ingests unstructured and semi-structured datasets, cleans and validates them, and bulk-loads them into a normalized MySQL database — with comprehensive PyTest coverage and optimized SQL.

## Features

| Area | Details |
|------|---------|
| **Ingestion** | CSV, JSON, NDJSON, paginated REST API |
| **Cleaning** | Null sentinel replacement, Unicode normalization, field truncation, whitespace stripping |
| **Validation** | Pydantic v2 schemas — typed, strict, descriptive errors |
| **Transformation** | FK resolution, deduplication, date coercion across 6 formats |
| **Loading** | SQLAlchemy 2.x ORM, batched `session.merge()` upserts, configurable batch size |
| **Schema** | 3NF normalized MySQL — Categories → Products → Customers → Orders → OrderItems |
| **Indexing** | Composite indexes on `(customer_id, ordered_at)` and `(status, ordered_at)` |
| **Testing** | PyTest + pytest-cov — unit, integration, and E2E tests against in-memory SQLite |
| **Audit** | `pipeline_runs` table records every run's source, ingested/failed row counts, and duration |

---

## Project Structure

```
data-pipeline/
├── pipeline/
│   ├── config.py              # DB config, env loading
│   ├── models.py              # SQLAlchemy ORM models (3NF schema)
│   ├── ingestion/
│   │   ├── base_ingester.py   # Abstract base class
│   │   ├── csv_ingester.py    # CSV ingester
│   │   ├── json_ingester.py   # JSON / NDJSON ingester
│   │   └── api_ingester.py    # Paginated REST API ingester
│   ├── cleaning/
│   │   ├── cleaner.py         # Pre-validation data cleaning
│   │   └── validators.py      # Pydantic v2 schemas
│   ├── transformations/
│   │   └── transformer.py     # FK resolution + deduplication
│   ├── loader/
│   │   └── db_loader.py       # Bulk upsert + audit logging
│   └── utils/
│       ├── logger.py          # Structured logging
│       └── metrics.py         # Per-run metrics tracker
├── sql/
│   ├── schema.sql             # MySQL DDL + indexes
│   └── analytics_queries.sql  # Optimized analytical queries
├── tests/
│   ├── conftest.py            # Fixtures (SQLite engine, session, raw data)
│   ├── test_ingestion.py      # CSV / JSON / API ingester tests
│   ├── test_cleaning.py       # DataCleaner tests
│   ├── test_validators.py     # Pydantic validator tests
│   ├── test_transformations.py # DataTransformer tests
│   ├── test_loader.py         # DBLoader integration tests
│   └── test_pipeline_e2e.py   # Full E2E pipeline tests
├── data/
│   ├── sample_orders.csv
│   ├── sample_products.json
│   └── sample_events.ndjson
├── main.py                    # CLI entry point
├── requirements.txt
└── .env.example
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your MySQL credentials
```

### 3. Create MySQL schema

```bash
mysql -u root -p data_pipeline < sql/schema.sql
```

### 4. Run the pipeline

```bash
# Ingest a CSV file into MySQL
python main.py --source csv --file data/sample_orders.csv

# Ingest a JSON product feed
python main.py --source json --file data/sample_products.json

# Use SQLite for a quick local test (no MySQL needed)
python main.py --source csv --file data/sample_orders.csv --db-url sqlite
```

---

## Running Tests

```bash
# Run all tests with coverage
pytest tests/ -v --cov=pipeline --cov-report=term-missing

# Run only unit tests
pytest tests/ -v -k "not e2e"

# Run only E2E tests
pytest tests/test_pipeline_e2e.py -v
```

No MySQL server is required — all tests run against in-memory SQLite.

---

## SQL Optimizations

### Indexes
```sql
-- Customer history queries — uses (customer_id, ordered_at)
INDEX ix_orders_customer_ordered_at (customer_id, ordered_at)

-- Status dashboard queries — uses (status, ordered_at)
INDEX ix_orders_status_ordered_at (status, ordered_at)

-- Product category lookups
INDEX ix_products_category (category_id)
```

### Bulk Loading Strategy
- Records are loaded in configurable batches (default: 500 rows)
- `session.merge()` provides upsert semantics for entities with unique constraints
- FK maps are resolved with a single `SELECT` per entity type after initial load

---

## Schema (3NF)

```
categories    → id, name, parent_id
products      → id, sku, name, category_id (FK), price
customers     → id, name, email, phone
orders        → id, customer_id (FK), status, total_amount, ordered_at
order_items   → id, order_id (FK), product_id (FK), quantity, unit_price
pipeline_runs → id, source, rows_ingested, rows_failed, error_log, started_at, finished_at
```

---

## Edge Cases Handled

| Edge Case | Handling |
|-----------|---------|
| Null sentinels (`N/A`, `null`, `none`, `""`, `-`) | Replaced with `None` before validation |
| Duplicate emails / SKUs | Deduplicated in-memory; first occurrence wins |
| Malformed NDJSON lines | Logged and skipped; pipeline continues |
| Multiple date formats | 6 formats tried in order; raises `ValueError` if none match |
| Oversized fields | Truncated to DB column max length |
| Unknown FK references | Row skipped, failure counted in metrics |
| Control characters / mojibake | Stripped / NFC-normalized before validation |
| Empty CSV files | Returns `[]` without error |
| API pagination | Follows `next` URL until `None` |
