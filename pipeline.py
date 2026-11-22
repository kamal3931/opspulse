import hashlib
import json
from pathlib import Path
from typing import Any, Dict
import pandas as pd
import pyarrow as pa
import requests
import pyarrow.dataset as ds  # Add to imports at the top of pipeline.py


RAW_PATH = Path("data/raw/weather_raw.json")
PROCESSED_PATH = Path("data/processed/weather_clean.csv")
PARQUET_BASE_DIR = Path("data/processed/weather_parquet")

WEATHER_ARROW_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("us")),
    ("temperature_c", pa.float64()),
    ("relative_humidity_pct", pa.float64()),
    ("precipitation_mm", pa.float64()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("date", pa.string()),
    ("hour", pa.int32()),
    ("is_freezing", pa.bool_()),
    ("record_id", pa.string()),
])



def save_processed_parquet(df: pd.DataFrame, destination_dir: Path = PARQUET_BASE_DIR) -> None:
    """Persist DataFrame to date-partitioned Snappy Parquet files using PyArrow."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=WEATHER_ARROW_SCHEMA, preserve_index=False)

    ds.write_dataset(
        data=table,
        base_dir=destination_dir,
        format="parquet",
        partitioning=["year", "month", "date"],
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        file_options=ds.ParquetFileFormat().make_write_options(compression="snappy"),
    )


def generate_record_id(lat: float, lon: float, timestamp_str: str) -> str:
    """Generate a deterministic 16-character SHA-256 hash natural key."""
    payload = f"{lat:.4f}_{lon:.4f}_{timestamp_str}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def extract_weather_data(latitude: float = 43.8971, longitude: float = -78.8658) -> Dict[str, Any]:
    """Fetch raw forecast and historical observations from Open-Meteo."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation"],
        "past_days": 1,
        "forecast_days": 1,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def save_raw_json(data: Dict[str, Any], destination: Path = RAW_PATH) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with open(destination, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_raw_json(source: Path = RAW_PATH) -> Dict[str, Any]:
    with open(source, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_weather_data(raw_data: Dict[str, Any]) -> pd.DataFrame:
    if "hourly" not in raw_data:
        raise ValueError("Payload missing required 'hourly' block.")

    hourly = raw_data["hourly"]
    lat = raw_data.get("latitude", 43.8971)
    lon = raw_data.get("longitude", -78.8658)

    required_fields = ["time", "temperature_2m", "relative_humidity_2m", "precipitation"]
    for field in required_fields:
        if field not in hourly:
            raise KeyError(f"Missing required metric field: '{field}'")

    df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly["time"]),
        "temperature_c": pd.to_numeric(hourly["temperature_2m"], errors="coerce"),
        "relative_humidity_pct": pd.to_numeric(hourly["relative_humidity_2m"], errors="coerce"),
        "precipitation_mm": pd.to_numeric(hourly["precipitation"], errors="coerce"),
    })

    df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    # Partition keys & features
    df["year"] = df["timestamp"].dt.year.astype("int32")
    df["month"] = df["timestamp"].dt.month.astype("int32")
    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["timestamp"].dt.hour.astype("int32")
    df["is_freezing"] = df["temperature_c"] <= 0.0

    # Deterministic natural key
    df["record_id"] = [
        generate_record_id(lat, lon, ts.isoformat()) for ts in df["timestamp"]
    ]

    return df


def save_processed_csv(df: pd.DataFrame, destination: Path = PROCESSED_PATH) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, index=False)

def run_pipeline() -> None:
    """Orchestrate extract, raw save, transform, and idempotent upsert."""
    from upsert import idempotent_upsert_parquet

    print("1. Extracting data from Open-Meteo API...")
    raw_payload = extract_weather_data()

    print(f"2. Persisting raw payload -> {RAW_PATH}...")
    save_raw_json(raw_payload)

    print("3. Transforming raw payload...")
    clean_df = transform_weather_data(raw_payload)

    print(f"4. Performing idempotent upsert -> {PARQUET_BASE_DIR}...")
    total_records = idempotent_upsert_parquet(clean_df)

    print(f"Pipeline finished successfully. Total unique records in lakehouse: {total_records}")


if __name__ == "__main__":
    run_pipeline()