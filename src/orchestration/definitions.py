from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import AssetSelection, Definitions, ScheduleDefinition, asset, define_asset_job

from src.lakehouse.bronze_airline import run_bronze_load
from src.lakehouse.silver_airline import run_silver_transform
from src.observability.check_openobserve import run_openobserve_health_check
from src.observability.openobserve import configure_observability, set_span_attributes, traced_span
from src.quality.checks_postgres import run_postgres_quality_checks
from src.quality.run_soda_checks import run_soda_quality_checks
from src.warehouse.load_silver_to_postgres import run_load_silver_to_postgres
from src.warehouse.run_dbt import run_dbt_models

REPO_ROOT = Path(__file__).resolve().parents[2]
configure_observability(service_name="aviation-dagster")


@asset(group_name="observability")
def openobserve_health_asset() -> dict[str, str]:
    with traced_span(
        "openobserve_health_asset",
        {
            "asset.group": "observability",
            "asset.name": "openobserve_health_asset",
        },
    ) as span:
        result = run_openobserve_health_check()
        set_span_attributes(
            span,
            {
                "observability.status": result.get("status"),
                "observability.endpoint": result.get("endpoint"),
            },
        )
        return result


@asset(group_name="lakehouse", deps=[openobserve_health_asset])
def bronze_airline_asset() -> dict[str, int]:
    with traced_span(
        "bronze_airline_asset",
        {
            "asset.group": "lakehouse",
            "asset.name": "bronze_airline_asset",
            "asset.input_path": str(REPO_ROOT / "data" / "raw" / "Airline Dataset.csv"),
            "asset.output_path": str(REPO_ROOT / "data" / "bronze" / "airline"),
        },
    ) as span:
        frame = run_bronze_load(
            input_path=REPO_ROOT / "data" / "raw" / "Airline Dataset.csv",
            output_path=REPO_ROOT / "data" / "bronze" / "airline",
            overwrite=True,
        )
        set_span_attributes(
            span,
            {
                "data.rows": frame.height,
                "data.columns": frame.width,
            },
        )
        return {"rows": frame.height, "columns": frame.width}


@asset(group_name="lakehouse", deps=[bronze_airline_asset])
def silver_airline_asset() -> dict[str, int]:
    with traced_span(
        "silver_airline_asset",
        {
            "asset.group": "lakehouse",
            "asset.name": "silver_airline_asset",
            "asset.input_path": str(REPO_ROOT / "data" / "bronze" / "airline"),
            "asset.output_path": str(REPO_ROOT / "data" / "silver" / "airline"),
            "asset.enforce_flight_status": True,
        },
    ) as span:
        frame = run_silver_transform(
            input_path=REPO_ROOT / "data" / "bronze" / "airline",
            output_path=REPO_ROOT / "data" / "silver" / "airline",
            overwrite=True,
            enforce_flight_status=True,
        )
        set_span_attributes(
            span,
            {
                "data.rows": frame.height,
                "data.columns": frame.width,
            },
        )
        return {"rows": frame.height, "columns": frame.width}


@asset(group_name="warehouse", deps=[silver_airline_asset])
def load_silver_to_postgres_asset() -> dict[str, Any]:
    """Load Silver Delta table to Postgres raw_airline table."""
    with traced_span(
        "load_silver_to_postgres_asset",
        {
            "asset.group": "warehouse",
            "asset.name": "load_silver_to_postgres_asset",
            "warehouse.schema": "public",
            "warehouse.table": "raw_airline",
        },
    ) as span:
        result = run_load_silver_to_postgres(
            input_path=REPO_ROOT / "data" / "silver" / "airline",
            output_table="raw_airline",
            schema="public",
            replace=True,
        )
        set_span_attributes(
            span,
            {
                "data.rows_loaded": result.get("rows_loaded"),
                "data.columns": result.get("columns"),
                "warehouse.full_table": result.get("table"),
                "warehouse.data_version": result.get("data_version"),
            },
        )
        return result


@asset(group_name="gold", deps=[load_silver_to_postgres_asset])
def dbt_gold_asset() -> dict[str, Any]:
    """Run dbt transformations to create Gold layer analytics tables."""
    with traced_span(
        "dbt_gold_asset",
        {
            "asset.group": "gold",
            "asset.name": "dbt_gold_asset",
            "dbt.project_dir": str(REPO_ROOT / "dbt"),
            "dbt.profiles_dir": str(REPO_ROOT / "dbt"),
        },
    ) as span:
        result = run_dbt_models(
            profiles_dir=REPO_ROOT / "dbt",
            project_dir=REPO_ROOT / "dbt",
        )
        set_span_attributes(
            span,
            {
                "dbt.status": result.get("status"),
                "dbt.message": result.get("message"),
                "dbt.models": result.get("models"),
            },
        )
        return result


@asset(group_name="quality", deps=[dbt_gold_asset])
def data_quality_asset() -> dict[str, Any]:
    """Run warehouse-level quality checks (SQL checks + Soda scan)."""
    with traced_span(
        "data_quality_asset",
        {
            "asset.group": "quality",
            "asset.name": "data_quality_asset",
            "quality.engine.custom": True,
            "quality.engine.soda": True,
        },
    ) as span:
        custom_checks = run_postgres_quality_checks()
        soda_checks = run_soda_quality_checks(
            configuration_path=REPO_ROOT / "soda" / "configuration.yml",
            checks_path=REPO_ROOT / "soda" / "checks" / "warehouse.yml",
        )
        set_span_attributes(
            span,
            {
                "quality.custom.status": custom_checks.get("status"),
                "quality.custom.total_checks": custom_checks.get("total_checks"),
                "quality.custom.failed_checks": custom_checks.get("failed_checks"),
                "quality.soda.status": soda_checks.get("status"),
                "quality.soda.checks_file": soda_checks.get("checks_file"),
            },
        )
        return {"postgres_checks": custom_checks, "soda_checks": soda_checks}


daily_aviation_pipeline_job = define_asset_job(
    name="daily_aviation_pipeline_job",
    selection=AssetSelection.assets(
        openobserve_health_asset,
        bronze_airline_asset,
        silver_airline_asset,
        load_silver_to_postgres_asset,
        dbt_gold_asset,
        data_quality_asset,
    ),
)


daily_aviation_pipeline_schedule = ScheduleDefinition(
    job=daily_aviation_pipeline_job,
    cron_schedule="20 11 * * *",
    execution_timezone="Asia/Kolkata",
    name="daily_aviation_pipeline_schedule",
)


defs = Definitions(
    assets=[
        openobserve_health_asset,
        bronze_airline_asset,
        silver_airline_asset,
        load_silver_to_postgres_asset,
        dbt_gold_asset,
        data_quality_asset,
    ],
    schedules=[daily_aviation_pipeline_schedule],
)
