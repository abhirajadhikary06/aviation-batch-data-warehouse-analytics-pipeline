from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


load_dotenv(dotenv_path=_repo_root() / ".env", override=True)


def _resolve_supabase_db_host() -> str:
    explicit_host = os.getenv("SUPABASE_DB_HOST", "").strip()
    if explicit_host:
        return explicit_host

    host = os.getenv("SUPABASE_HOST", "").strip()
    if host:
        if host.startswith("db."):
            return host
        if host.endswith(".supabase.co") and not host.startswith("db."):
            return f"db.{host}"
        return host

    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    if supabase_url:
        parsed = urlparse(supabase_url)
        hostname = parsed.hostname or ""
        if hostname.endswith(".supabase.co"):
            return f"db.{hostname}"

    return "localhost"


def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=_resolve_supabase_db_host(),
        port=int(os.getenv("SUPABASE_PORT", 5432)),
        database=os.getenv("SUPABASE_DB", "postgres"),
        user=os.getenv("SUPABASE_USER", "postgres"),
        password=os.getenv("SUPABASE_PASSWORD", os.getenv("SUPABASE_DB_PASSWORD", "")),
        sslmode=os.getenv("SUPABASE_SSLMODE", "require"),
        connect_timeout=int(os.getenv("SUPABASE_CONNECT_TIMEOUT", 20)),
    )


def run_postgres_quality_checks() -> dict[str, object]:
    """
    Run lightweight warehouse quality checks against Supabase Postgres.

    Raises:
        RuntimeError: when one or more checks fail.
    """
    checks: list[dict[str, object]] = []
    required_tables = ["raw_airline", "dim_airport", "fact_passenger_flights", "fct_flight_metrics"]

    with _get_connection() as connection:
        with connection.cursor() as cursor:
            # 1) required tables exist
            for table_name in required_tables:
                cursor.execute(
                    """
                    SELECT table_schema
                    FROM information_schema.tables
                    WHERE table_name = %s
                      AND table_schema IN ('public', 'public_public')
                    ORDER BY CASE WHEN table_schema = 'public_public' THEN 1 ELSE 2 END
                    LIMIT 1
                    """,
                    (table_name,),
                )
                row = cursor.fetchone()
                checks.append(
                    {
                        "check": f"table_exists:{table_name}",
                        "passed": row is not None,
                        "details": f"{row[0]}.{table_name}" if row else "missing",
                    }
                )

            # 2) non-zero row counts
            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_name IN ('raw_airline', 'dim_airport', 'fact_passenger_flights', 'fct_flight_metrics')
                  AND table_schema IN ('public', 'public_public')
                """
            )
            located_tables = cursor.fetchall()
            for schema_name, table_name in located_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                row_count = cursor.fetchone()[0]
                checks.append(
                    {
                        "check": f"row_count_positive:{schema_name}.{table_name}",
                        "passed": row_count > 0,
                        "details": row_count,
                    }
                )

            # 3) metric sanity check (0-100 range)
            metric_schema = None
            cursor.execute(
                """
                SELECT table_schema
                FROM information_schema.tables
                WHERE table_name = 'fct_flight_metrics'
                  AND table_schema IN ('public', 'public_public')
                LIMIT 1
                """
            )
            metric_row = cursor.fetchone()
            if metric_row:
                metric_schema = metric_row[0]
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {metric_schema}.fct_flight_metrics
                    WHERE on_time_percentage < 0 OR on_time_percentage > 100
                    """
                )
                invalid_pct_count = cursor.fetchone()[0]
                checks.append(
                    {
                        "check": f"on_time_percentage_range:{metric_schema}.fct_flight_metrics",
                        "passed": invalid_pct_count == 0,
                        "details": {"invalid_rows": invalid_pct_count},
                    }
                )

    failed = [c for c in checks if not c["passed"]]
    result = {
        "status": "success" if not failed else "failed",
        "total_checks": len(checks),
        "passed_checks": len(checks) - len(failed),
        "failed_checks": len(failed),
        "checks": checks,
    }
    if failed:
        raise RuntimeError(f"Quality checks failed: {failed}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Postgres/Supabase quality checks.")
    parser.parse_args()
    output = run_postgres_quality_checks()
    print(output)
