# aviation-batch-data-warehouse-analytics-pipeline

Batch data engineering pipeline for aviation analytics using Kaggle ingestion, Delta Lake (Bronze/Silver), Supabase Postgres serving, dbt Gold models, and Dagster orchestration.

## 1. Current Status (as of 2026-04-24)

The core pipeline is working end-to-end.

- Kaggle dataset ingestion is implemented.
- Bronze Delta table creation is implemented.
- Silver Delta cleaning and deduplication is implemented.
- Silver -> Supabase/Postgres load is implemented and optimized using bulk `COPY`.
- dbt Gold models are implemented and running successfully:
  - `dim_airport`
  - `fact_passenger_flights`
  - `fct_flight_metrics`
- dbt tests are implemented (`schema.yml` + `sources.yml`) and discovered by dbt.
- Dagster assets are wired for full orchestration with absolute repo-root paths.

## 2. Pipeline Flow

1. Download raw airline data from Kaggle into `data/raw/`.
2. Build Bronze Delta table in `data/bronze/airline`.
3. Build Silver Delta table in `data/silver/airline`.
4. Load Silver data into Supabase table `public.raw_airline`.
5. Run dbt models to build Gold tables in Postgres.
6. Validate quality with dbt tests.

## 3. Repository Structure

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
  orchestration/
    definitions.py
  quality/
    checks_postgres.py

dbt/
  dbt_project.yml
  profiles.yml
  models/
    marts/
      dim_airport.sql
      fact_passenger_flights.sql
      fct_flight_metrics.sql
      schema.yml
      sources.yml

data/
  raw/
  bronze/
  silver/
```

## 4. Environment Setup

From project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create `.env` with at least:

- `KAGGLE_USERNAME`
- `KAGGLE_API_KEY` (or `KAGGLE_KEY`)
- `SUPABASE_URL`
- `SUPABASE_USER`
- `SUPABASE_DB`
- `SUPABASE_PASSWORD` (or `SUPABASE_DB_PASSWORD`)
- `SUPABASE_PORT` (optional, defaults to `5432` for Supabase Cloud)

## 5. Runbook (Manual)

### Step 1: Ingestion

```powershell
.\.venv\Scripts\python.exe src\ingestion\download_kaggle_dataset.py --dataset iamsouravbanerjee/airline-dataset --provider kagglehub
```

### Step 2: Bronze

```powershell
.\.venv\Scripts\python.exe src\lakehouse\bronze_airline.py --overwrite
```

### Step 3: Silver

```powershell
.\.venv\Scripts\python.exe src\lakehouse\silver_airline.py --overwrite --enforce-flight-status
```

### Step 4: Load to Supabase

```powershell
.\.venv\Scripts\python.exe src\warehouse\load_silver_to_postgres.py
```

### Step 5: Run dbt Gold Models

```powershell
.\.venv\Scripts\python.exe src\warehouse\run_dbt.py
```

### Step 6: Run dbt Tests

```powershell
.\.venv\Scripts\python.exe -m dbt.cli.main test --profiles-dir dbt --project-dir dbt --threads 1
```

## 6. Orchestration (Dagster)

Start Dagster:

```powershell
dagster dev -m src.orchestration.definitions
```

Assets defined:

- `bronze_airline_asset`
- `silver_airline_asset`
- `load_silver_to_postgres_asset`
- `dbt_gold_asset`
- `data_quality_asset`

Execution order is enforced through asset dependencies.

### Daily automation

Dagster schedule configured:

- Schedule: `daily_aviation_pipeline_schedule`
- Job: `daily_aviation_pipeline_job`
- Cron: `0 2 * * *`
- Timezone: `Asia/Kolkata`

This runs the full pipeline daily at 2:00 AM IST:

`bronze -> silver -> load to Postgres -> dbt gold -> quality checks`

`data_quality_asset` runs after dbt and validates:

- required table existence
- non-zero row counts
- `on_time_percentage` range sanity (0 to 100)

## 7. dbt Models and Tests

Gold models:

- `dbt/models/marts/dim_airport.sql`
- `dbt/models/marts/fact_passenger_flights.sql`
- `dbt/models/marts/fct_flight_metrics.sql`

Tests configured in:

- `dbt/models/marts/schema.yml`
- `dbt/models/marts/sources.yml`

Current test coverage includes:

- `not_null`
- `unique`
- `accepted_values`
- source-level `not_null`

Note: Do not edit files under `dbt/target/`; those are generated artifacts.

## 8. Quick Validation Queries

Run in Supabase SQL editor:

```sql
select count(*) as raw_count from public.raw_airline;
select count(*) as dim_airport_count from public_public.dim_airport;
select count(*) as fact_passenger_flights_count from public_public.fact_passenger_flights;
select count(*) as fct_flight_metrics_count from public_public.fct_flight_metrics;
```

## 9. Known Warnings / Follow-ups

- `dbt_project.yml` still uses `data-paths`; dbt recommends `seed-paths`.
- There are currently unused config paths in `dbt_project.yml`.
- Gold models are currently materialized to `public_public` (can be cleaned to `public` via dbt schema config refinement).

## 10. Metabase (Local) Visualization Setup

### Start Metabase locally

```powershell
docker compose -f docker-compose.metabase.yml up -d
```

Metabase URL:

- `http://localhost:3001`

### Connect Metabase to Supabase Postgres

Use Database type: `PostgreSQL`

- Host: `db.<your-project-ref>.supabase.co` (or your `SUPABASE_DB_HOST`)
- Port: `5432`
- Database: `postgres` (or `SUPABASE_DB`)
- Username: `postgres` (or `SUPABASE_USER`)
- Password: `SUPABASE_PASSWORD` (or `SUPABASE_DB_PASSWORD`)
- SSL mode: `require`

### Starter dashboard SQL

Use queries from:

- `metabase/starter_queries.sql`

### Stop Metabase

```powershell
docker compose -f docker-compose.metabase.yml down
```

## 11. Next Improvements

1. Normalize dbt schema config to land Gold tables in `public`.
2. Replace deprecated dbt config keys and remove unused paths.
3. Add relationship tests and business-rule tests (age ranges, date constraints).
4. Add richer Metabase dashboards (filters, date drilldowns, segments).
5. Add CI pipeline for `dbt parse`, `dbt run`, and `dbt test`.
