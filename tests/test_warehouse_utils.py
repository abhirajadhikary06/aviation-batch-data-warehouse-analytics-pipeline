from __future__ import annotations

import polars as pl

from src.warehouse.load_silver_to_postgres import _build_create_table_sql, _resolve_supabase_db_host
from src.warehouse.run_dbt import _is_connection_issue


def test_resolve_supabase_db_host_prefers_explicit_host(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_DB_HOST", "db.custom.supabase.co")
    monkeypatch.setenv("SUPABASE_HOST", "ignored.supabase.co")
    monkeypatch.setenv("SUPABASE_URL", "https://also-ignored.supabase.co")
    assert _resolve_supabase_db_host() == "db.custom.supabase.co"


def test_resolve_supabase_db_host_derives_from_api_host(monkeypatch) -> None:
    monkeypatch.delenv("SUPABASE_DB_HOST", raising=False)
    monkeypatch.setenv("SUPABASE_HOST", "fkrosxuntfbebeldzzvt.supabase.co")
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert _resolve_supabase_db_host() == "db.fkrosxuntfbebeldzzvt.supabase.co"


def test_build_create_table_sql_maps_common_types() -> None:
    frame = pl.DataFrame(
        {
            "name": ["a"],
            "age": [1],
            "score": [10.5],
            "is_active": [True],
        }
    )
    sql = _build_create_table_sql(frame, "public.test_table")

    assert '"name" VARCHAR' in sql
    assert '"age" BIGINT' in sql
    assert '"score" DOUBLE PRECISION' in sql
    assert '"is_active" BOOLEAN' in sql


def test_is_connection_issue_detects_known_patterns() -> None:
    assert _is_connection_issue("could not connect to server")
    assert _is_connection_issue("connection refused")
    assert not _is_connection_issue("column does not exist")
