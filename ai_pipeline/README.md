# 🦆 AI Data Pipeline with DuckDB, Polars, and Parquet

A production-style, single-machine AI data pipeline inspired by the article
*"Building an AI Data Pipeline with DuckDB, Polars, and Parquet"*.

No Spark cluster. No warehouse. No Docker. Just Parquet files, analytical SQL,
and expression-based feature engineering.

> "Most AI pipelines don't fail because the models are bad. They fail because
> the data pipeline slowly becomes impossible to reason about."

---

## Overview

This project implements a medallion-style (**Raw → Bronze → Gold**) pipeline that:

1. **Generates** realistic fake e-commerce events as NDJSON (landing zone).
2. **Ingests** them lazily with Polars and writes a standardized **Bronze** Parquet layer.
3. **Filters** purchase events with DuckDB SQL directly over the Parquet files
   (column pruning + predicate pushdown).
4. **Transfers** the result to Polars **zero-copy** via Apache Arrow.
5. **Engineers** customer-level features using Polars expressions.
6. **Exports** a model-ready **Gold** training dataset as compressed Parquet.
7. **Validates** every stage with an automated `pytest` suite.

---

## Architecture

```text
generate_fake_data.py
        │  writes NDJSON events
        ▼
data/raw/*.json
        │  Polars scan_ndjson (lazy, streaming)
        ▼
data/bronze/events.parquet            ← Bronze: raw reality, standardized format
        │  DuckDB SQL: WHERE event = 'purchase' AND amount > 0
        │  (column pruning + predicate pushdown on Parquet)
        ▼  Apache Arrow (zero-copy — no CSVs, no temp files)
Polars: timestamp parsing, feature engineering, aggregation
        ▼
data/gold/customer_features.parquet   ← Gold: model-ready training dataset
```

### Division of labor

| Tool        | Responsibility                                            |
|-------------|-----------------------------------------------------------|
| **Parquet** | Columnar storage: compression, pruning, pushdown          |
| **DuckDB**  | SQL filtering/aggregation directly over files             |
| **Polars**  | Expression-based cleaning, features, and aggregations     |
| **Arrow**   | Zero-copy data exchange between DuckDB and Polars         |
| **Pydantic**| Configuration as code (no hardcoded paths)                |
| **Loguru**  | Structured logging                                        |
| **pytest**  | Automated schema + data-quality validation                |

---

## Project Structure

```text
ai_pipeline/
├── data/                  # created at runtime
│   ├── raw/               # fake NDJSON events (landing zone)
│   ├── bronze/            # standardized raw Parquet
│   └── gold/              # model-ready customer features
├── pipeline/
│   ├── __init__.py
│   ├── settings.py        # single configuration source (Pydantic)
│   ├── ingest.py          # Raw → Bronze (Polars lazy streaming)
│   ├── transform.py       # Bronze → purchases (DuckDB + Arrow)
│   ├── features.py        # feature engineering (Polars expressions)
│   └── export.py          # Gold Parquet writer
├── tests/
│   └── test_pipeline.py   # end-to-end validation suite
├── generate_fake_data.py  # fake NDJSON event generator
├── main.py                # orchestration entry point
├── pyproject.toml         # project + pytest configuration
├── requirements.txt
└── README.md
```

---

## Requirements

- Python **3.9+** (tested on 3.14)
- **No Docker, no cluster, no external services**

```text
duckdb==1.4.0
polars==1.32.0
pyarrow==21.0.0
pydantic==2.11.7
loguru==0.7.3
orjson==3.11.1
pytest==8.3.0
```

---

## Installation

### Option A — `uv` (recommended)

```bash
uv venv
uv pip install -r requirements.txt
```

Or, if you manage dependencies via `pyproject.toml`:

```bash
uv sync
```

### Option B — `pip` + venv

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
pip install -r requirements.txt
```

---

## Quickstart

### 1. Generate fake data

```bash
uv run python generate_fake_data.py
```

Writes ~10,000 NDJSON events to `data/raw/events.json`:

```json
{"user_id": 192, "country": "IN", "event": "purchase", "amount": 74.12, "timestamp": "2026-07-03T08:14:02Z"}
```

### 2. Run the pipeline

```bash
uv run python main.py
```

Expected log output:

```text
INFO | Starting ingestion from raw NDJSON to Bronze Parquet
INFO | Bronze layer written to data/bronze/events.parquet
INFO | Filtering purchases via DuckDB
INFO | Building customer-level features with Polars
INFO | Gold dataset exported to data/gold/customer_features.parquet
```

### 3. Inspect the Gold dataset

```bash
uv run python -c "import polars as pl; print(pl.read_parquet('data/gold/customer_features.parquet'))"
```

```text
shape: (…, 5)
┌─────────┬────────┬────────────────┬───────────────┬───────────────┐
│ user_id ┆ orders ┆ lifetime_value  average_order ┆ largest_order │
│ ---     ┆ ---    ┆ ---            ┆ ---           ┆ ---           │
│ i64     ┆ u32    ┆ f64            ┆ f64           ┆ f64           │
╞═════════╪════════╪════════════════╪═══════════════╪═══════════════╡
│ 1       ┆ 8      ┆ 2143.55        ┆ 267.94        ┆ 497.21        │
│ 2       ┆ 5      ┆ 1012.10        ┆ 202.42        ┆ 431.87        │
└─────────┴────────┴────────────────┴───────────────┴───────────────┘
```

---

## Running Tests

```bash
uv run pytest tests/ -v
```

The test suite:

- regenerates a fresh 1,000-event dataset in a clean `data/` directory,
- runs the full pipeline end-to-end,
- validates Bronze/Gold artifacts exist,
- checks the Gold schema, row counts, uniqueness of `user_id`,
- asserts no nulls and sane value ranges.

> **Note:** `pyproject.toml` contains `pythonpath = ["."]` under
> `[tool.pytest.ini_options]` so that `main` and `generate_fake_data`
> are importable from `tests/`.

---

## Configuration

All paths and options live in one place — `pipeline/settings.py`:

```python
from pathlib import Path
from pydantic import BaseModel

class Settings(BaseModel):
    raw_dir: Path = Path("data/raw")
    bronze_dir: Path = Path("data/bronze")
    gold_dir: Path = Path("data/gold")
    database: str = "pipeline.duckdb"

    def setup_dirs(self):
        for d in [self.raw_dir, self.bronze_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

settings = Settings()
```

Every stage imports this single source of truth — no hardcoded paths anywhere.

---

## Pipeline Stages

### Ingest (`pipeline/ingest.py`)
Streams NDJSON → Parquet **lazily** (nothing is loaded into memory):

```python
events = pl.scan_ndjson(str(settings.raw_dir / "*.json")) \
             .select(["user_id", "country", "event", "amount", "timestamp"])
events.sink_parquet(out_path, compression="zstd")
```

The Bronze layer **preserves reality**: no cleaning, no dedup, no "fixing".

### Transform (`pipeline/transform.py`)
DuckDB filters where the data lives; results cross into Polars via Arrow:

```python
arrow_table = conn.execute(query, [bronze_path]).arrow()
purchases = pl.from_arrow(arrow_table)
```

No CSVs. No temporary files. No unnecessary serialization. **SQL → Arrow → Polars.**

### Features (`pipeline/features.py`)
Expression-based feature engineering (note the explicit timezone handling):

```python
pl.col("timestamp")
  .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", time_zone="UTC")
  .dt.replace_time_zone(None)
```

followed by a customer-level aggregation (`orders`, `lifetime_value`,
`average_order`, `largest_order`).

### Export (`pipeline/export.py`)
Persists the Gold dataset with ZSTD compression so training jobs never
rebuild expensive transformations.

---

## Design Principles (from the article)

- **Store everything as Parquet.**
- **Push filtering into DuckDB** (SQL for joins/filters).
- **Use Polars for feature engineering** (expressions, not Python loops).
- **Stay lazy as long as possible** — one optimized execution.
- **Keep data columnar** — zero-copy Arrow between engines.
- **Configuration as code** — one Pydantic `Settings` object.
- **Validation is a pipeline stage** — automated by the test suite.
- **Log every important event** — logs are for developers three months later.

---

## Troubleshooting

### `ComputeError: strptime / to_datetime was called with no format and no time zone…`
Timestamps contain a `Z` (UTC) marker. Newer Polars refuses implicit parsing.
**Fix:** pass `time_zone="UTC"` (and optionally an explicit `format=`), as done
in `pipeline/features.py`.

### `ModuleNotFoundError: No module named 'generate_fake_data'` (under pytest)
The project root is not on `sys.path` when pytest collects `tests/`.
**Fix:** `pythonpath = ["."]` in `[tool.pytest.ini_options]` (already configured),
or run `uv run python -m pytest tests/ -v`.

### `TypeError: the truth value of a Series is ambiguous`
A Polars `Series` was used directly in `assert`/`if`.
**Fix:** reduce it to a scalar, e.g.
`df.select((pl.col("orders") > 0).all()).item() is True`.

---

## Ideas for Extension

- Add a **Silver** layer (cleaned, deduplicated, typed) between Bronze and Gold.
- **Partition** the Gold dataset (`year=2026/month=07/day=03/`) as it grows.
- **Incremental processing** with a timestamp checkpoint (`WHERE timestamp > LAST_BATCH`).
- Move business logic into a `sql/` directory (`staging.sql`, `features.sql`, `validation.sql`).
- Profile before optimizing — most problems come from moving too much data.

---

## License

Educational project built from the article
*"Building an AI Data Pipeline with DuckDB, Polars, and Parquet"*.
