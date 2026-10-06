"""Shared configuration for the DuckDB / Polars / Spark benchmark.

This project is a self-hosted re-implementation of the benchmark described in
``duckdb-polars-spark-performance-comparision.pdf`` (originally: PySpark on
Databricks Serverless vs DuckDB vs Polars over a Backblaze SMART dataset).

Deltas from the article, as requested:

1. Databricks/Serverless compute is replaced by a local **Docker** Spark cluster.
2. The Backblaze dataset is **generated synthetically** instead of downloaded,
   with a default of **5,000,000 rows** instead of the article's 285,339,435.

The generated table keeps the article's schema: 11 base columns plus, for every
SMART attribute id in ``SMART_IDS``, a ``smart_<id>_normalized`` (int32) and a
``smart_<id>_raw`` (int64) column.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw) if raw else default


DATA_DIR = _env_path("BENCH_DATA_DIR", PROJECT_ROOT / "data")
RESULTS_DIR = _env_path("BENCH_RESULTS_DIR", PROJECT_ROOT / "results")

RAW_DIR = DATA_DIR / "raw_data"
MODELS_DIR = DATA_DIR / "models"

DEFAULT_ROWS = int(os.environ.get("BENCH_ROWS", 5_000_000))
DEFAULT_SEED = int(os.environ.get("BENCH_SEED", 20251007))

# SMART attribute ids used by the article's schema (91 ids -> 182 SMART columns).
SMART_IDS: tuple[int, ...] = (
    1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18,
    22, 23, 24, 27, 71, 82, 90,
    160, 161, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172,
    173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184,
    187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198,
    199, 200, 201, 202, 206, 210, 218, 220, 222, 223, 224, 225,
    226, 230, 231, 232, 233, 234, 235, 240, 241, 242, 244, 245,
    246, 247, 248, 250, 251, 252, 254, 255,
)

BASE_COLUMNS: tuple[str, ...] = (
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "datacenter",
    "cluster_id",
    "vault_id",
    "pod_id",
    "pod_slot_num",
    "is_legacy_format",
)

# The article appends normalized/raw per id, interleaved.
SMART_COLUMNS: tuple[str, ...] = tuple(
    name
    for smart_id in SMART_IDS
    for name in (f"smart_{smart_id}_normalized", f"smart_{smart_id}_raw")
)

RAW_COLUMNS: tuple[str, ...] = BASE_COLUMNS + SMART_COLUMNS

# The single SMART counter the benchmark actually computes over.
BENCH_SMART_ID = 5
BENCH_SMART_RAW = f"smart_{BENCH_SMART_ID}_raw"
BENCH_SMART_NORMALIZED = f"smart_{BENCH_SMART_ID}_normalized"

# Rolling window used by the article: AVG over 29 preceding rows + current row.
ROLLING_WINDOW = 30


def raw_glob() -> str:
    """Glob that every engine uses to read the large fact table."""
    return str(RAW_DIR / "*.parquet")


def models_glob() -> str:
    """Glob that every engine uses to read the small dimension table."""
    return str(MODELS_DIR / "*.parquet")


if __name__ == "__main__":  # pragma: no cover - quick introspection helper
    print(f"project_root      : {PROJECT_ROOT}")
    print(f"data_dir          : {DATA_DIR}")
    print(f"raw_dir           : {RAW_DIR}")
    print(f"models_dir        : {MODELS_DIR}")
    print(f"default_rows      : {DEFAULT_ROWS:,}")
    print(f"smart ids         : {len(SMART_IDS)}")
    print(f"raw column count  : {len(RAW_COLUMNS)}")
