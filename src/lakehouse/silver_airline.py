from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from deltalake import DeltaTable
from deltalake.writer import write_deltalake


DEFAULT_BRONZE_PATH = Path("data/bronze/airline")
DEFAULT_SILVER_PATH = Path("data/silver/airline")
VALID_FLIGHT_STATUS = {"On Time", "Delayed", "Cancelled"}


LOGGER = logging.getLogger("silver_airline")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Bronze airline data into a cleaned Silver Delta table."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_BRONZE_PATH),
        help="Path to the Bronze Delta table.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_SILVER_PATH),
        help="Path to the Silver Delta output directory.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the Silver Delta table if it already exists.",
    )
    parser.add_argument(
        "--enforce-flight-status",
        action="store_true",
        help="Keep only rows with flight_status in [On Time, Delayed, Cancelled].",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def to_snake_case(name: str) -> str:
    name = name.strip().replace("-", " ")
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"_+", "_", name).strip("_").lower()


def load_bronze_delta(input_path: Path) -> pl.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Bronze Delta path not found: {input_path}")
    LOGGER.info("Loading Bronze Delta table from %s", input_path)
    table = DeltaTable(str(input_path))
    return pl.from_arrow(table.to_pyarrow_table())


def standardize_schema(frame: pl.DataFrame) -> pl.DataFrame:
    rename_mapping = {column: to_snake_case(column) for column in frame.columns}
    frame = frame.rename(rename_mapping)

    return frame.with_columns(
        [
            pl.col("passenger_id").cast(pl.Int64, strict=False),
            pl.col("age").cast(pl.Int64, strict=False),
            pl.col("departure_date").str.to_date(format="%-m/%-d/%Y", strict=False),
            # Keep ISO timestamp as string; lexical sorting preserves chronology for UTC offsets.
            pl.col("ingested_at").cast(pl.Utf8, strict=False),
        ]
    )


def clean_records(frame: pl.DataFrame, enforce_flight_status: bool) -> pl.DataFrame:
    cleaned = frame.filter(
        pl.col("passenger_id").is_not_null() & pl.col("departure_date").is_not_null()
    )

    if enforce_flight_status:
        cleaned = cleaned.filter(pl.col("flight_status").is_in(list(VALID_FLIGHT_STATUS)))

    return cleaned


def deduplicate_records(frame: pl.DataFrame) -> pl.DataFrame:
    if "row_hash" not in frame.columns:
        raise ValueError("Expected 'row_hash' column is missing in Bronze data")

    sort_cols = ["row_hash"]
    descending = [False]

    if "ingested_at" in frame.columns:
        sort_cols.append("ingested_at")
        descending.append(True)

    latest = frame.sort(by=sort_cols, descending=descending)
    return latest.unique(subset=["row_hash"], keep="first")


def add_silver_metadata(frame: pl.DataFrame) -> pl.DataFrame:
    processed_at = datetime.now(timezone.utc).isoformat()
    return frame.with_columns(pl.lit(processed_at).alias("silver_processed_at"))


def write_silver_delta(frame: pl.DataFrame, output_path: Path, overwrite: bool) -> None:
    output_path.mkdir(parents=True, exist_ok=True)
    mode = "overwrite" if overwrite else "append"
    LOGGER.info("Writing Silver Delta table to %s using mode=%s", output_path, mode)
    write_deltalake(str(output_path), frame.to_arrow(), mode=mode)


def validate_silver(frame: pl.DataFrame) -> None:
    LOGGER.info("Silver validation | shape=%s", frame.shape)
    null_counts = frame.select(
        [
            pl.col("passenger_id").is_null().sum().alias("passenger_id_nulls"),
            pl.col("departure_date").is_null().sum().alias("departure_date_nulls"),
        ]
    )
    LOGGER.info("Silver validation | null checks=%s", null_counts.to_dicts()[0])
    LOGGER.info("Silver validation | columns=%s", frame.columns)
    LOGGER.info("Silver validation | sample=%s", frame.head(5).to_dicts())


def run_silver_transform(
    input_path: Path,
    output_path: Path,
    overwrite: bool = False,
    enforce_flight_status: bool = False,
) -> pl.DataFrame:
    bronze = load_bronze_delta(input_path)
    LOGGER.info("Loaded Bronze frame with shape=%s", bronze.shape)

    standardized = standardize_schema(bronze)
    cleaned = clean_records(standardized, enforce_flight_status=enforce_flight_status)
    deduped = deduplicate_records(cleaned)
    silver = add_silver_metadata(deduped)

    write_silver_delta(silver, output_path, overwrite=overwrite)
    validate_silver(silver)
    return silver


def main() -> None:
    configure_logging()
    args = parse_args()

    run_silver_transform(
        input_path=Path(args.input),
        output_path=Path(args.output),
        overwrite=args.overwrite,
        enforce_flight_status=args.enforce_flight_status,
    )


if __name__ == "__main__":
    main()
