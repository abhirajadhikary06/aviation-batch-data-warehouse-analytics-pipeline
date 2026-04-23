from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import AssetSelection, Definitions, ScheduleDefinition, asset, define_asset_job

from src.lakehouse.bronze_airline import run_bronze_load
from src.lakehouse.silver_airline import run_silver_transform
from src.quality.checks_postgres import run_postgres_quality_checks
from src.warehouse.load_silver_to_postgres import run_load_silver_to_postgres
from src.warehouse.run_dbt import run_dbt_models

REPO_ROOT = Path(__file__).resolve().parents[2]


@asset(group_name="lakehouse")
def bronze_airline_asset() -> dict[str, int]:
    frame = run_bronze_load(
        input_path=REPO_ROOT / "data" / "raw" / "Airline Dataset.csv",
        output_path=REPO_ROOT / "data" / "bronze" / "airline",
        overwrite=True,
    )
    return {"rows": frame.height, "columns": frame.width}


@asset(group_name="lakehouse", deps=[bronze_airline_asset])
def silver_airline_asset() -> dict[str, int]:
    frame = run_silver_transform(
        input_path=REPO_ROOT / "data" / "bronze" / "airline",
        output_path=REPO_ROOT / "data" / "silver" / "airline",
        overwrite=True,
        enforce_flight_status=True,
    )
    return {"rows": frame.height, "columns": frame.width}


@asset(group_name="warehouse", deps=[silver_airline_asset])
def load_silver_to_postgres_asset() -> dict[str, Any]:
    """Load Silver Delta table to Postgres raw_airline table."""
    result = run_load_silver_to_postgres(
        input_path=REPO_ROOT / "data" / "silver" / "airline",
        output_table="raw_airline",
        schema="public",
        replace=True,
    )
    return result


@asset(group_name="gold", deps=[load_silver_to_postgres_asset])
def dbt_gold_asset() -> dict[str, Any]:
    """Run dbt transformations to create Gold layer analytics tables."""
    result = run_dbt_models(
        profiles_dir=REPO_ROOT / "dbt",
        project_dir=REPO_ROOT / "dbt",
    )
    return result


@asset(group_name="quality", deps=[dbt_gold_asset])
def data_quality_asset() -> dict[str, Any]:
    """Run warehouse-level data quality checks on Postgres/Supabase."""
    return run_postgres_quality_checks()


daily_aviation_pipeline_job = define_asset_job(
    name="daily_aviation_pipeline_job",
    selection=AssetSelection.assets(
        bronze_airline_asset,
        silver_airline_asset,
        load_silver_to_postgres_asset,
        dbt_gold_asset,
        data_quality_asset,
    ),
)


daily_aviation_pipeline_schedule = ScheduleDefinition(
    job=daily_aviation_pipeline_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Asia/Kolkata",
    name="daily_aviation_pipeline_schedule",
)


defs = Definitions(
    assets=[
        bronze_airline_asset,
        silver_airline_asset,
        load_silver_to_postgres_asset,
        dbt_gold_asset,
        data_quality_asset,
    ],
    schedules=[daily_aviation_pipeline_schedule],
)
