from pathlib import Path
import duckdb

DUCKDB_PATH = Path("data/analytics.duckdb")
PARQUET_DIR = Path("data/processed/weather_parquet")


def create_gold_marts(db_path: Path = DUCKDB_PATH) -> None:
    """Read partitioned Parquet lakehouse files and materialize a Gold summary mart."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # 1. Create a view on top of the external Parquet dataset (Silver layer)
    conn.execute(f"""
    CREATE OR REPLACE VIEW silver_weather AS
    SELECT *
    FROM read_parquet('{PARQUET_DIR}/*/*/*/*.parquet', hive_partitioning=true);
    """)

    # 2. Materialize the Gold dimensional mart
    conn.execute("""
    CREATE OR REPLACE TABLE gold_daily_weather_summary AS
    SELECT  
        date,
        COUNT(*) AS total_recorded_hours,
        ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
        ROUND(MIN(temperature_c), 2) AS min_temperature_c,
        ROUND(MAX(temperature_c), 2) AS max_temperature_c,
        ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
        COUNT(CASE WHEN is_freezing THEN 1 END) AS sub_zero_hours,
        ROUND(AVG(AVG(temperature_c)) OVER (
            ORDER BY date 
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_2day_avg_temp_c
    FROM silver_weather
    GROUP BY date
    ORDER BY date ASC;
    """)

    conn.close()


def fetch_gold_summary(db_path: Path = DUCKDB_PATH):
    """Retrieve the Gold mart as a Pandas DataFrame."""
    conn = duckdb.connect(str(db_path), read_only=True)
    df = conn.execute("SELECT * FROM gold_daily_weather_summary").df()
    conn.close()
    return df


if __name__ == "__main__":
    create_gold_marts()
    print("Gold Daily Summary Table:")
    print(fetch_gold_summary())