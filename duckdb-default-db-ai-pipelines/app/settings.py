"""Centralised configuration for the DuckDB AI pipeline."""

from __future__ import annotations

from pathlib import Path

ROOT: Path = Path(__file__).resolve().parent.parent

DATA_RAW: Path = ROOT / "data" / "raw"
DATA_PROCESSED: Path = ROOT / "data" / "processed"
DATA_EMBEDDINGS: Path = ROOT / "data" / "embeddings"
DATABASE_PATH: Path = ROOT / "data" / "pipeline.duckdb"

# ── Data generation ──────────────────────────────────────────────────
NUM_DOCUMENTS: int = 1_000_000
NUM_USERS: int = 50_000
PARALLEL_WORKERS: int = 8       # multiprocessing workers for data gen
DOCS_PER_FILE: int = 50_000     # ~10 Parquet files for documents
USERS_PER_FILE: int = 50_000    # single file for users

# ── Processing ───────────────────────────────────────────────────────
BATCH_SIZE: int = 5_000          # streaming batch size for pipeline stages
EMBEDDING_BATCH_SIZE: int = 128  # sentence-transformer batch size

# ── API ──────────────────────────────────────────────────────────────
API_HOST: str = "127.0.0.1"
API_PORT: int = 8000
API_READ_ONLY: bool = True   # Set False only if API needs write access
                               # (then limit uvicorn to --workers 1)
