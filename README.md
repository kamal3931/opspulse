# OpsPulse — Weather Data Lakehouse & Analytics Pipeline

[![OpsPulse CI](https://github.com/kamal3931/opspulse/actions/workflows/ci.yml/badge.svg)](https://github.com/kamal3931/opspulse/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-FFF000?logo=duckdb&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Pytest](https://img.shields.io/badge/Tested_with-Pytest-0A9EDC?logo=pytest&logoColor=white)

**OpsPulse** is an end-to-end, containerized local data lakehouse and analytics pipeline built in Python. It ingests live hourly weather telemetry from the Open-Meteo API, organizes data across a **Medallion Architecture (Bronze → Silver → Gold)** using partitioned Parquet and DuckDB, enforces **idempotent upserts**, and validates every code change through automated **Pytest quality gates** and **GitHub Actions CI/CD**.

---

## Architecture Overview

```text
+-------------------+
|  Open-Meteo API   |  (Live Hourly Weather Telemetry)
+---------+---------+
          |
          v  pipeline.py (Extract)
+---------+-----------------------------------------------+
|  BRONZE LAYER  |  data/raw/                             |
|  Raw JSON      |  Immutable source API payloads         |
+---------+-----------------------------------------------+
          |
          v  pipeline.py + upsert.py (Transform & Idempotent Upsert)
+---------+-----------------------------------------------+
|  SILVER LAYER  |  data/processed/weather_parquet/       |
|  Parquet       |  Cleaned, typed & date-partitioned     |
+---------+-----------------------------------------------+
          |
          v  analytics.py (DuckDB SQL Aggregations & Window Functions)
+---------+-----------------------------------------------+
|  GOLD LAYER    |  DuckDB Analytical Mart                |
|  Daily Summary |  Daily KPIs & rolling 2-day temp avgs  |
+---------+-----------------------------------------------+
          |
          v  test_pipeline.py + .github/workflows/ci.yml
+---------+-----------------------------------------------+
|  QUALITY & CI  |  Pytest Contracts + Docker + GH Actions|
|  Verification  |  Idempotency, schema & container tests |
+---------------------------------------------------------+
```

### Medallion Layers

| Layer | Format & Storage | Core Script | Responsibility |
| :--- | :--- | :--- | :--- |
| **Bronze** | Raw JSON (`data/raw/`) | `pipeline.py` | Captures unmodified API responses for auditability and replayability. |
| **Silver** | Date-Partitioned Parquet (`data/processed/weather_parquet/`) | `pipeline.py`, `upsert.py` | Enforces schema typing, cleans timestamps, and deduplicates overlapping pulls via idempotent upserts. |
| **Gold** | DuckDB Analytical Views/Tables | `analytics.py` | Queries Silver Parquet directly to compute daily summary metrics and rolling 2-day temperature averages. |

---

## Idempotent Upsert Design

A common failure mode in batch ETL pipelines is **duplicate record inflation** when jobs retry after a failure or fetch overlapping time windows from an upstream API.

OpsPulse prevents this in `upsert.py` by treating every Silver write as an **idempotent merge**:

1. **Partition-Aware Reads:** When a new batch arrives, the upsert engine checks existing Parquet partitions overlapping the incoming date range.
2. **Deterministic Deduplication:** Incoming records are merged with existing partition records and deduplicated on the natural record key (timestamp), keeping the freshest observation for any overlapping window.
3. **Atomic Partition Rewrite:** Only affected date partitions are written back to `data/processed/weather_parquet/`, leaving historical partitions untouched.

**Result:** Running `python pipeline.py` once, twice, or ten times back-to-back yields the exact same unique record count and analytical output. This behavior is actively verified in CI by executing `pipeline.py` twice consecutively before running the `pytest` suite.

---

## Tech Stack & Repository Structure

* **Ingestion & Transformation:** Python 3.12, `requests`, `pandas`, `pyarrow`
* **Analytical Engine:** DuckDB
* **Testing & Data Quality:** `pytest`
* **Containerization:** Docker, Docker Compose
* **CI/CD:** GitHub Actions (multi-stage native Python + Docker container verification)

```text
opspulse/
├── .github/
│   └── workflows/
│       └── ci.yml             # 2-stage CI: Python ETL/Pytest -> Docker verification
├── pipeline.py                # Bronze extraction + Silver transformation orchestration
├── upsert.py                  # Partition-aware idempotent Parquet upsert logic
├── analytics.py               # DuckDB Gold Daily Summary & rolling averages
├── test_pipeline.py           # Automated Pytest data quality & idempotency suite
├── requirements.txt           # Pinned Python dependencies
├── Dockerfile                 # Lightweight python:3.12-slim container image
├── docker-compose.yml         # Pipeline & test services with ./data bind mount
├── .dockerignore              # Keeps build context minimal
└── .gitignore                 # Excludes .venv/, local data/, and DuckDB artifacts
```

---

## Quickstart

### Option 1: Docker (Recommended)

Run the entire lakehouse pipeline and test suite in an isolated Linux container with zero local Python setup. Persistent Bronze, Silver, and Gold outputs are mounted to `./data`.

**Prerequisites:** Docker Desktop (or OrbStack on macOS)

```bash
# 1. Clone the repository
git clone [https://github.com/kamal3931/opspulse.git](https://github.com/kamal3931/opspulse.git)
cd opspulse

# 2. Build the image and run the end-to-end ETL + Gold analytics pipeline
docker compose up --build opspulse-pipeline

# 3. Execute the automated Pytest suite inside the container
docker compose run --rm opspulse-tests
```

---

### Option 2: Local Python Environment

**Prerequisites:** Python 3.12+

```bash
# 1. Clone the repository
git clone [https://github.com/kamal3931/opspulse.git](https://github.com/kamal3931/opspulse.git)
cd opspulse

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Run the Bronze -> Silver ETL pipeline and Gold DuckDB analytics
python pipeline.py
python analytics.py

# 5. Run all Pytest quality gates
pytest -v test_pipeline.py
```

---

## Automated Quality Gates & CI/CD

Every push or pull request to `main` triggers **OpsPulse CI** (`.github/workflows/ci.yml`), structured into two dependent jobs on `ubuntu-latest`:

1. **`Python Pipeline & Pytest`**:
   * Installs dependencies in a clean Python 3.12 environment.
   * Executes `pipeline.py` **twice back-to-back** to prove live idempotency on a clean runner, followed by `analytics.py`.
   * Runs `pytest -v test_pipeline.py` to enforce schema, transformation, idempotency, and live Parquet output contracts.
2. **`Docker Build & Container Tests`** *(runs only after Job 1 passes)*:
   * Validates `docker-compose.yml` syntax and builds the production Docker image.
   * Executes both `opspulse-pipeline` and `opspulse-tests` containers end-to-end to guarantee environment parity.

---

## Roadmap

* [ ] Add a scheduled `cron` trigger in GitHub Actions for automated daily ingestion.
* [ ] Add workflow orchestration and retry observability with Prefect or Dagster.
* [ ] Connect the Gold DuckDB mart to an interactive Streamlit or Power BI dashboard.
