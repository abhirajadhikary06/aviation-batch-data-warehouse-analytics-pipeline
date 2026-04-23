from __future__ import annotations

import argparse
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import polars as pl
from deltalake.writer import write_deltalake


DEFAULT_RAW_PATH = Path("data/raw/Airline Dataset.csv")
DEFAULT_BRONZE_PATH = Path("data/bronze/airline")


LOGGER = logging.getLogger("bronze_airline")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Read the airline CSV from raw storage and write a Bronze Delta table."
	)
	parser.add_argument(
		"--input",
		default=str(DEFAULT_RAW_PATH),
		help="Path to the raw CSV input file.",
	)
	parser.add_argument(
		"--output",
		default=str(DEFAULT_BRONZE_PATH),
		help="Path to the Bronze Delta output directory.",
	)
	parser.add_argument(
		"--overwrite",
		action="store_true",
		help="Overwrite the Bronze Delta table if it already exists.",
	)
	return parser.parse_args()


def configure_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
	)


def validate_input_file(input_path: Path) -> None:
	if not input_path.exists():
		raise FileNotFoundError(f"Input CSV file not found: {input_path}")
	if not input_path.is_file():
		raise ValueError(f"Input path is not a file: {input_path}")


def normalize_for_hash(value: object) -> str:
	if value is None:
		return ""
	if isinstance(value, float) and value.is_integer():
		return str(int(value))
	return str(value).strip()


def build_row_hash_expression(columns: Iterable[str]) -> pl.Expr:
	return pl.concat_str(
		[pl.col(column).cast(pl.Utf8, strict=False).fill_null("") for column in columns],
		separator="||",
	).map_elements(
		lambda row_value: hashlib.sha256(row_value.encode("utf-8")).hexdigest(),
		return_dtype=pl.Utf8,
	).alias("row_hash")


def load_raw_csv(input_path: Path) -> pl.DataFrame:
	LOGGER.info("Reading raw CSV from %s", input_path)
	return pl.read_csv(
		input_path,
		infer_schema_length=1000,
		try_parse_dates=False,
		ignore_errors=False,
	)


def add_bronze_metadata(frame: pl.DataFrame, input_path: Path) -> pl.DataFrame:
	raw_columns = frame.columns
	ingested_at = datetime.now(timezone.utc).isoformat()

	LOGGER.info("Adding Bronze metadata columns")
	return frame.with_columns(
		[
			pl.lit(str(input_path)).alias("source_file"),
			pl.lit(ingested_at).alias("ingested_at"),
			build_row_hash_expression(raw_columns),
		]
	)


def write_bronze_delta(frame: pl.DataFrame, output_path: Path, overwrite: bool) -> None:
	output_path.mkdir(parents=True, exist_ok=True)

	mode = "overwrite" if overwrite else "append"
	LOGGER.info("Writing Bronze Delta table to %s using mode=%s", output_path, mode)
	write_deltalake(
		str(output_path),
		frame.to_arrow(),
		mode=mode,
	)


def run_bronze_load(input_path: Path, output_path: Path, overwrite: bool = False) -> pl.DataFrame:
	validate_input_file(input_path)
	raw_frame = load_raw_csv(input_path)
	LOGGER.info("Loaded raw frame with shape=%s and columns=%s", raw_frame.shape, raw_frame.columns)

	bronze_frame = add_bronze_metadata(raw_frame, input_path)
	LOGGER.info("Prepared Bronze frame with shape=%s", bronze_frame.shape)

	write_bronze_delta(bronze_frame, output_path, overwrite=overwrite)
	LOGGER.info("Bronze load complete")
	return bronze_frame


def main() -> None:
	configure_logging()
	args = parse_args()

	input_path = Path(args.input)
	output_path = Path(args.output)

	LOGGER.info("Starting Bronze load")
	run_bronze_load(input_path=input_path, output_path=output_path, overwrite=args.overwrite)


if __name__ == "__main__":
	main()
