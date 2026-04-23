"""Load Silver Delta table to Postgres/Supabase warehouse."""
from __future__ import annotations

import argparse
import io
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import psycopg2
from deltalake import DeltaTable
from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


# Load environment variables from repo-root .env file
load_dotenv(dotenv_path=_repo_root() / ".env", override=True)


def _resolve_supabase_db_host() -> str:
    """Resolve Supabase Postgres host from env vars.

    Priority:
    1) SUPABASE_DB_HOST
    2) SUPABASE_HOST (if already db.* or transformed from API host)
    3) Derived from SUPABASE_URL: https://<project_ref>.supabase.co -> db.<project_ref>.supabase.co
    """
    explicit_host = os.getenv("SUPABASE_DB_HOST", "").strip()
    if explicit_host:
        return explicit_host

    host = os.getenv("SUPABASE_HOST", "").strip()
    if host:
        if host.startswith("db."):
            return host
        # If user passed API host (<ref>.supabase.co), convert to DB host.
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


def load_silver_to_postgres(
    input_path: str | Path = "data/silver/airline",
    output_table: str = "raw_airline",
    schema: str = "public",
    replace: bool = True,
) -> dict[str, str | int]:
    """
    Load Silver Delta table to Postgres warehouse.
    
    Args:
        input_path: Path to Silver Delta table
        output_table: Target table name in Postgres
        schema: Postgres schema name
        replace: If True, drop and recreate table; if False, append
    
    Returns:
        Dict with load metadata: rows_loaded, columns, load_timestamp, data_version
    """
    # Read Silver Delta table
    print(f"[READ] Reading Silver Delta from {input_path}...")
    delta_table = DeltaTable(str(input_path))
    arrow_table = delta_table.to_pyarrow_table()
    silver_df = pl.from_arrow(arrow_table)
    
    print(f"[OK] Read {silver_df.height} rows x {silver_df.width} columns")
    
    # Connect to Postgres/Supabase
    db_host = _resolve_supabase_db_host()
    db_port = int(os.getenv("SUPABASE_PORT", 6543))

    print(f"[CONNECT] Connecting to Postgres at {db_host}:{db_port}...")
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=os.getenv("SUPABASE_DB", "postgres"),
            user=os.getenv("SUPABASE_USER", "postgres"),
            password=os.getenv("SUPABASE_PASSWORD", os.getenv("SUPABASE_DB_PASSWORD", "")),
            sslmode=os.getenv("SUPABASE_SSLMODE", "require"),
            connect_timeout=int(os.getenv("SUPABASE_CONNECT_TIMEOUT", 20)),
        )
        cursor = connection.cursor()
        print("[OK] Connected to Postgres")
    except psycopg2.Error as e:
        raise RuntimeError(f"Failed to connect to Postgres: {e}") from e
    
    # Prepare table name
    full_table_name = f"{schema}.{output_table}"
    
    try:
        # Drop table if replace=True
        if replace:
            print(f"[DROP] Dropping existing table {full_table_name}...")
            cursor.execute(f"DROP TABLE IF EXISTS {full_table_name};")
            connection.commit()
        
        # Create table with schema
        print(f"[CREATE] Creating table {full_table_name}...")
        columns_sql = _build_create_table_sql(silver_df, full_table_name)
        cursor.execute(columns_sql)
        connection.commit()
        print("[OK] Table created")
        
        # Bulk insert with COPY (significantly faster than row-by-row execute)
        print(f"[INSERT] Bulk loading {silver_df.height} rows with COPY...")
        _copy_df_to_postgres(cursor, silver_df, full_table_name)
        connection.commit()
        print(f"[OK] Bulk load complete: {silver_df.height} rows")
        
        # Add metadata columns (update)
        load_timestamp = datetime.utcnow().isoformat()
        data_version = "v1.0"
        metadata_sql = f"""
        ALTER TABLE {full_table_name} 
        ADD COLUMN IF NOT EXISTS load_timestamp VARCHAR DEFAULT '{load_timestamp}',
        ADD COLUMN IF NOT EXISTS data_version VARCHAR DEFAULT '{data_version}';
        """
        cursor.execute(metadata_sql)
        connection.commit()
        print(f"[OK] Added metadata: load_timestamp={load_timestamp}, data_version={data_version}")
        
        # Verify load
        cursor.execute(f"SELECT COUNT(*) FROM {full_table_name};")
        final_count = cursor.fetchone()[0]
        print(f"[OK] Verification: {final_count} rows in Postgres")
        
        return {
            "rows_loaded": final_count,
            "columns": silver_df.width,
            "load_timestamp": load_timestamp,
            "data_version": data_version,
            "table": full_table_name,
        }
    
    finally:
        cursor.close()
        connection.close()


def _build_create_table_sql(df: pl.DataFrame, table_name: str) -> str:
    """Build CREATE TABLE SQL from Polars dataframe schema."""
    polars_to_postgres = {
        "String": "VARCHAR",
        "Int64": "BIGINT",
        "Int32": "INTEGER",
        "Float64": "DOUBLE PRECISION",
        "Date": "DATE",
        "Boolean": "BOOLEAN",
    }
    
    columns = []
    for col_name, col_dtype in zip(df.columns, df.dtypes):
        pg_type = polars_to_postgres.get(str(col_dtype), "VARCHAR")
        columns.append(f'"{col_name}" {pg_type}')
    
    columns_sql = ",\n  ".join(columns)
    return f"""
    CREATE TABLE {table_name} (
      {columns_sql}
    );
    """


def _copy_df_to_postgres(cursor: psycopg2.extensions.cursor, df: pl.DataFrame, table_name: str) -> None:
    """Load a Polars DataFrame into Postgres using COPY FROM STDIN."""
    buffer = io.StringIO()
    df.write_csv(buffer, include_header=True)
    buffer.seek(0)

    col_names = ", ".join([f'"{col}"' for col in df.columns])
    copy_sql = f"COPY {table_name} ({col_names}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    cursor.copy_expert(copy_sql, buffer)


def run_load_silver_to_postgres(
    input_path: str | Path = "data/silver/airline",
    output_table: str = "raw_airline",
    schema: str = "public",
    replace: bool = True,
) -> dict[str, str | int]:
    """Orchestrator function for use in Dagster assets."""
    return load_silver_to_postgres(
        input_path=input_path,
        output_table=output_table,
        schema=schema,
        replace=replace,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Silver Delta to Postgres")
    parser.add_argument("--input", default="data/silver/airline", help="Silver Delta path")
    parser.add_argument("--table", default="raw_airline", help="Target table name")
    parser.add_argument("--schema", default="public", help="Postgres schema")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append instead of replace",
    )
    
    args = parser.parse_args()
    
    result = run_load_silver_to_postgres(
        input_path=args.input,
        output_table=args.table,
        schema=args.schema,
        replace=not args.append,
    )
    
    print("\n" + "=" * 60)
    print("LOAD COMPLETE")
    print("=" * 60)
    for key, value in result.items():
        print(f"{key}: {value}")
