from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from pipeline import (
    PARQUET_BASE_DIR,
    WEATHER_ARROW_SCHEMA,
    save_processed_parquet,
    transform_weather_data,
)

from upsert import idempotent_upsert_parquet


def test_idempotency_multiple_runs(mock_weather_payload, tmp_path):
    """Verify executing the upsert twice with the same data produces zero duplicate rows."""
    test_dir = tmp_path / "idempotent_lake"

    df_first_run = transform_weather_data(mock_weather_payload)
    count_1 = idempotent_upsert_parquet(df_first_run, parquet_dir=test_dir)
    assert count_1 == 3

    # Exact replay
    count_2 = idempotent_upsert_parquet(df_first_run, parquet_dir=test_dir)
    assert count_2 == 3

    # Overlapping batch (2 duplicates + 1 new hour)
    overlapping_payload = {
        "latitude": 43.8971,
        "longitude": -78.8658,
        "hourly": {
            "time": [
                "2026-10-21T23:00",
                "2026-10-22T00:00",
                "2026-10-22T02:00",
            ],
            "temperature_2m": [11.2, -1.0, 3.5],
            "relative_humidity_2m": [70, 80, 65],
            "precipitation": [0.0, 0.4, 0.0],
        },
    }
    df_third_run = transform_weather_data(overlapping_payload)
    count_3 = idempotent_upsert_parquet(df_third_run, parquet_dir=test_dir)
    assert count_3 == 4

@pytest.fixture
def mock_weather_payload():
    return {
        "latitude": 43.8971,
        "longitude": -78.8658,
        "hourly": {
            "time": [
                "2026-10-21T23:00",
                "2026-10-22T00:00",
                "2026-10-22T01:00",
            ],
            "temperature_2m": [11.2, -1.0, 0.0],
            "relative_humidity_2m": [70, 80, 85],
            "precipitation": [0.0, 0.4, 0.0],
        },
    }


def test_schema_and_partition_columns(mock_weather_payload):
    """Verify DataFrame contains all required metrics and partition keys."""
    df = transform_weather_data(mock_weather_payload)
    expected_cols = {"timestamp", "temperature_c", "relative_humidity_pct", "precipitation_mm", "year", "month", "date", "hour", "is_freezing"}
    assert expected_cols.issubset(set(df.columns))
    assert len(df) == 3


def test_parquet_dataset_read_and_metadata(mock_weather_payload, tmp_path):
    """Verify writing partitioned dataset preserves row count and compression."""
    df = transform_weather_data(mock_weather_payload)
    test_parquet_dir = tmp_path / "test_parquet"

    # Write to temporary directory
    save_processed_parquet(df, destination_dir=test_parquet_dir)

    # Read back as a unified Arrow dataset
    dataset = ds.dataset(test_parquet_dir, format="parquet", partitioning="hive")
    reloaded_table = dataset.to_table()

    assert reloaded_table.num_rows == 3

    # Check that individual parquet files are using SNAPPY compression
    parquet_files = list(test_parquet_dir.glob("**/*.parquet"))
    assert len(parquet_files) > 0

    first_file_meta = pq.read_metadata(parquet_files[0])
    assert first_file_meta.row_group(0).column(0).compression == "SNAPPY"


def test_live_parquet_output_exists():
    """Verify live pipeline output directory contains parquet files."""
    parquet_files = list(PARQUET_BASE_DIR.glob("**/*.parquet"))
    assert len(parquet_files) > 0, "No partitioned parquet files found in data/processed/weather_parquet."

from analytics import create_gold_marts, fetch_gold_summary


def test_gold_daily_summary_mart(tmp_path):
    """Verify DuckDB accurately computes daily metrics from parquet."""
    test_db = tmp_path / "test_analytics.duckdb"
    create_gold_marts(db_path=test_db)

    gold_df = fetch_gold_summary(db_path=test_db)

    # Basic validations on materialized table
    assert len(gold_df) > 0, "Gold summary table has no rows."
    assert "rolling_2day_avg_temp_c" in gold_df.columns
    assert (gold_df["min_temperature_c"] <= gold_df["max_temperature_c"]).all(), (
        "Min temp is greater than Max temp."
    )
    assert (gold_df["total_precipitation_mm"] >= 0.0).all(), (
        "Negative precipitation found."
    )