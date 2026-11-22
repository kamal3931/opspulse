from pathlib import Path
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from pipeline import PARQUET_BASE_DIR, WEATHER_ARROW_SCHEMA


def idempotent_upsert_parquet(
    new_df: pd.DataFrame, parquet_dir: Path = PARQUET_BASE_DIR
) -> int:
    """Merges new_df into existing parquet partitions, deduplicating by record_id.

    Returns the total deduplicated record count.
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)
    existing_files = list(parquet_dir.glob("**/*.parquet"))

    con = duckdb.connect()

    incoming_arrow = pa.Table.from_pandas(
        new_df, schema=WEATHER_ARROW_SCHEMA, preserve_index=False
    )
    con.register("incoming_batch", incoming_arrow)

    if not existing_files:
        ds.write_dataset(
            data=incoming_arrow,
            base_dir=parquet_dir,
            format="parquet",
            partitioning=["year", "month", "date"],
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
            file_options=ds.ParquetFileFormat().make_write_options(compression="snappy"),
        )
        con.close()
        return len(new_df)

    # Explicit column lists + casts avoid Hive partition column reordering & type inference drift
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE consolidated_records AS
    WITH combined AS (
        SELECT
            timestamp,
            temperature_c,
            relative_humidity_pct,
            precipitation_mm,
            CAST(year AS INTEGER) AS year,
            CAST(month AS INTEGER) AS month,
            CAST(date AS VARCHAR) AS date,
            CAST(hour AS INTEGER) AS hour,
            is_freezing,
            record_id,
            1 AS source_priority
        FROM incoming_batch
        UNION ALL
        SELECT
            timestamp,
            temperature_c,
            relative_humidity_pct,
            precipitation_mm,
            CAST(year AS INTEGER) AS year,
            CAST(month AS INTEGER) AS month,
            CAST(date AS VARCHAR) AS date,
            CAST(hour AS INTEGER) AS hour,
            is_freezing,
            record_id,
            2 AS source_priority
        FROM read_parquet('{parquet_dir}/*/*/*/*.parquet', hive_partitioning=true)
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY record_id
                ORDER BY source_priority ASC, timestamp DESC
            ) AS row_num
        FROM combined
    )
    SELECT
        timestamp,
        temperature_c,
        relative_humidity_pct,
        precipitation_mm,
        year,
        month,
        date,
        hour,
        is_freezing,
        record_id
    FROM deduped
    WHERE row_num = 1;
    """)

    deduped_arrow = con.sql("SELECT * FROM consolidated_records").to_arrow_table()
    deduped_arrow = deduped_arrow.cast(WEATHER_ARROW_SCHEMA)
    total_count = deduped_arrow.num_rows

    ds.write_dataset(
        data=deduped_arrow,
        base_dir=parquet_dir,
        format="parquet",
        partitioning=["year", "month", "date"],
        partitioning_flavor="hive",
        existing_data_behavior="delete_matching",
        file_options=ds.ParquetFileFormat().make_write_options(compression="snappy"),
    )

    con.close()
    return total_count