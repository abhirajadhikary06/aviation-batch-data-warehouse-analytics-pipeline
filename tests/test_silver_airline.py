from __future__ import annotations

import polars as pl
import pytest

from src.lakehouse.silver_airline import clean_records, deduplicate_records, to_snake_case


def test_to_snake_case_handles_spaces_and_camel_case() -> None:
    assert to_snake_case("Passenger ID") == "passenger_id"
    assert to_snake_case("Airport-Continent") == "airport_continent"
    assert to_snake_case("flightStatusCode") == "flight_status_code"


def test_clean_records_filters_nulls_and_enforces_status() -> None:
    frame = pl.DataFrame(
        {
            "passenger_id": [1, None, 3, 4],
            "departure_date": [pl.date(2024, 1, 1), pl.date(2024, 1, 1), None, pl.date(2024, 1, 2)],
            "flight_status": ["On Time", "Delayed", "Cancelled", "Unknown"],
        }
    )

    cleaned = clean_records(frame, enforce_flight_status=True)
    assert cleaned.height == 1
    assert cleaned["passenger_id"].to_list() == [1]


def test_deduplicate_records_keeps_latest_ingested_at() -> None:
    frame = pl.DataFrame(
        {
            "row_hash": ["h1", "h1", "h2"],
            "ingested_at": [
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
                "2026-01-01T05:00:00+00:00",
            ],
            "value": [10, 20, 30],
        }
    )

    deduped = deduplicate_records(frame).sort("row_hash")
    assert deduped.height == 2
    assert deduped["value"].to_list() == [20, 30]


def test_deduplicate_records_requires_row_hash() -> None:
    frame = pl.DataFrame({"passenger_id": [1, 2]})
    with pytest.raises(ValueError, match="row_hash"):
        deduplicate_records(frame)
