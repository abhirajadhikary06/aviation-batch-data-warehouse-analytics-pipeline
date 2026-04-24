# aviation-batch-data-warehouse-analytics-pipeline

Batch data engineering pipeline for aviation analytics using Kaggle ingestion, Delta Lake (Bronze/Silver), Supabase Postgres serving, dbt Gold models, and Dagster orchestration.

<p align="center">
  <img src="assets/flight%20batch%20data%20warehouse%20project.png" alt="Aviation Data Warehouse Architecture" width="1100" />
</p>

## Project Overview

This project implements a batch analytics platform for aviation data, following a layered medallion-style design and strong production engineering practices.

The pipeline solves four practical problems:

1. Ingesting raw external data in a repeatable way.
2. Creating reliable curated datasets for analytics.
3. Enforcing data quality before BI consumption.
4. Observing and troubleshooting pipeline runs with traces.

## Business Problem and Goal

Airline datasets are noisy, large, and arrive as raw files. Analytics teams need trusted, modeled tables for dashboards and KPI tracking.

The goal of this project is to deliver an orchestrated and testable pipeline that:

- ingests airline data,
- transforms it into clean Silver and dimensional Gold models,
- validates quality gates,
- powers BI dashboards,
- and exposes observability for operational confidence.

## Architecture and Data Flow

End-to-end flow:

1. **Ingestion**: download CSV dataset from Kaggle to `data/raw/`.
2. **Bronze**: store raw data as Delta Lake (`data/bronze/airline`) with ingestion metadata and row hash.
3. **Silver**: clean, standardize, and deduplicate into `data/silver/airline`.
4. **Warehouse Load**: bulk `COPY` from Silver to Supabase/Postgres table `public.raw_airline`.
5. **Gold Transformations**: dbt builds marts in Postgres (`dim_airport`, `fact_passenger_flights`, `fct_flight_metrics`).
6. **Data Quality Gate**:
   - custom SQL quality checks,
   - Soda Core checks from YAML rules.
7. **Analytics**: Metabase consumes validated warehouse tables.
8. **Observability**: Dagster assets emit spans to OpenObserve for traceability and failure diagnosis.

## Why This Design

- **Delta Lake in Bronze/Silver** gives reproducible, file-based lakehouse processing.
- **Postgres/Supabase in Gold** gives BI-friendly serving and SQL ergonomics.
- **dbt** provides maintainable transformation modeling and testable semantic SQL.
- **Dagster** provides explicit asset dependency graph and schedulable orchestration.
- **Soda Core** provides declarative quality governance and fail-fast data gate.
- **OpenObserve** provides operational observability and run-level trace context.

## Tech Stack

- Python (ETL + orchestration glue)
- Polars + PyArrow + DeltaLake
- Supabase Postgres
- dbt Core + dbt-postgres
- Dagster + Dagit
- Soda Core
- OpenObserve
- Metabase
- Pytest

## Repository Structure

```text
src/
  ingestion/
    download_kaggle_dataset.py
  lakehouse/
    bronze_airline.py
    silver_airline.py
  warehouse/
    load_silver_to_postgres.py
    run_dbt.py
  quality/
    checks_postgres.py
    run_soda_checks.py
  observability/
    openobserve.py
    check_openobserve.py
  orchestration/
    definitions.py

dbt/
  dbt_project.yml
  profiles.yml
  models/marts/
    dim_airport.sql
    fact_passenger_flights.sql
    fct_flight_metrics.sql
    schema.yml
    sources.yml

soda/
  configuration.yml
  checks/warehouse.yml

metabase/
  starter_queries.sql

assets/
  flight batch data warehouse project.png
```

## Data Model Layers

### Bronze

- Purpose: immutable raw landing zone.
- Key additions: `source_file`, `ingested_at`, `row_hash`.

### Silver

- Purpose: cleaned and standardized canonical records.
- Key actions: schema normalization, null filtering, status enforcement, deduplication on `row_hash`.

### Gold (dbt)

- `dim_airport`
- `fact_passenger_flights`
- `fct_flight_metrics`

Gold is the analytics contract for dashboards and downstream consumers.

## Orchestration in Dagster

Pipeline assets in execution order:

1. `openobserve_health_asset`
2. `bronze_airline_asset`
3. `silver_airline_asset`
4. `load_silver_to_postgres_asset`
5. `dbt_gold_asset`
6. `data_quality_asset`

`data_quality_asset` runs both quality engines:

- `run_postgres_quality_checks()`
- `run_soda_quality_checks()`

Schedule configured:

- `daily_aviation_pipeline_schedule`
- Cron: `0 2 * * *`
- Timezone: `Asia/Kolkata`

## Soda Core in This Project

Conceptual placement:

- **between warehouse-ready data and BI trust boundary**.

Technical placement:

- called from Dagster `data_quality_asset` after dbt models run.
- connection config: `soda/configuration.yml`.
- rules: `soda/checks/warehouse.yml`.

Current checks include:

- non-empty tables,
- key null checks,
- metric range checks (`on_time_percentage`).

## OpenObserve in This Project

Conceptual placement:

- **cross-cutting observability plane**, not in the data path.

Technical placement:

- spans emitted by Dagster asset executions.
- OTLP endpoint: `OPENOBSERVE_OTLP_ENDPOINT`.
- health gate: `openobserve_health_asset` validates OpenObserve reachability before the pipeline proceeds.

## Prerequisites

- Windows PowerShell
- Python 3.12+ recommended
- Docker Desktop (for OpenObserve and Metabase)
- Supabase project with Postgres access
- Kaggle API credentials

## Environment Setup

From project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If Soda CLI version mismatch occurs (v4 command behavior), install Soda Core v3 explicitly:

```powershell
python -m pip uninstall -y soda soda-postgres
python -m pip install soda-core-postgres==3.5.6
```

## Required `.env` Variables

```env
KAGGLE_USERNAME=...
KAGGLE_KEY=...

SUPABASE_URL=...
SUPABASE_HOST=...
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=...
SUPABASE_SSLMODE=require

OPENOBSERVE_ENABLED=true
OPENOBSERVE_OTLP_ENDPOINT=http://localhost:5080/api/default/v1/traces
OPENOBSERVE_USERNAME=root@example.com
OPENOBSERVE_PASSWORD=Complexpass#123
OPENOBSERVE_STREAM_NAME=default
```

## Runbook (Step by Step)

### 1) Ingestion

```powershell
.\.venv\Scripts\python.exe src\ingestion\download_kaggle_dataset.py --dataset iamsouravbanerjee/airline-dataset --provider kagglehub
```

### 2) Bronze

```powershell
.\.venv\Scripts\python.exe src\lakehouse\bronze_airline.py --overwrite
```

### 3) Silver

```powershell
.\.venv\Scripts\python.exe src\lakehouse\silver_airline.py --overwrite --enforce-flight-status
```

### 4) Load to Supabase/Postgres

```powershell
.\.venv\Scripts\python.exe src\warehouse\load_silver_to_postgres.py
```

### 5) dbt Run

```powershell
.\.venv\Scripts\python.exe src\warehouse\run_dbt.py
```

### 6) dbt Tests

```powershell
.\.venv\Scripts\python.exe -m dbt.cli.main test --profiles-dir dbt --project-dir dbt --threads 1
```

### 7) Soda Quality Checks

```powershell
.\.venv\Scripts\python.exe -m src.quality.run_soda_checks
```

### 8) Pytests

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests
```

### 9) Run Dagster End-to-End

```powershell
dagster dev -m src.orchestration.definitions
```

### 10) Run OpenObserve

```powershell
docker compose -f docker-compose.openobserve.yml up -d
```

Open UI at: `http://localhost:5080`.

In **Traces**:

- select stream `default`,
- filter with `service_name='aviation-dagster'`,
- run query.

### 11) Run Metabase

```powershell
docker compose -f docker-compose.metabase.yml up -d
```

Open UI at: `http://localhost:3001`.

## Interview-Ready Talking Points

- **Data reliability**: introduced layered lakehouse + warehouse model with dedup and controlled schema transitions.
- **Scalability**: used bulk Postgres `COPY` for performant loads.
- **Governance**: combined dbt tests + custom SQL checks + Soda rules for stronger trust.
- **Operational excellence**: added observability traces and pre-run OpenObserve health check in orchestration graph.
- **Modularity**: ingestion, transform, warehouse, quality, and observability are separated into explicit Python modules.
- **Reproducibility**: deterministic CLI runbook and Dagster asset graph with schedule.

## Known Notes

- dbt may show deprecation warnings around `data-paths` versus `seed-paths`; functional but recommended to modernize.
- If running Python 3.13, validate Soda package compatibility and pin to supported version when needed.

## Future Enhancements

1. Add CI pipeline for pytest + dbt + Soda checks.
2. Add alerting for failed quality gates and trace anomalies.
3. Add data contracts for source schema drift handling.
4. Expand BI dashboards with segment filters and trend drill-downs.
5. Add incremental model strategy and partition-aware optimization.
