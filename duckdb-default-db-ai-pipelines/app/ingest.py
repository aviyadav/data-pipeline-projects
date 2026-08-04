"""Ingestion service — scans raw Parquet, populates the metadata store.

Points covered:
  - Direct Parquet reading without imported into Python first
  - In-process DuckDB (no ingestion server needed)
  - Schema creation on startup
"""

from __future__ import annotations

import time

import duckdb
import pandas as pd

from app.settings import DATA_RAW
from app.repository import DuckRepository


class DocumentIngestionService:
    """Scans raw Parquet files and loads metadata into DuckDB tables."""

    def __init__(self, repo: DuckRepository) -> None:
        self.repo = repo

    def scan_documents(self) -> pd.DataFrame:
        """Read raw Parquet files *directly* with DuckDB SQL.

        This is the key insight: we never load GB of data into Python first.
        DuckDB does the heavy analytical work, materialising only results.
        """
        return self.repo.execute(f"""
            SELECT
                id,
                filename,
                language,
                source,
                user_id,
                word_count,
                page_count,
                content
            FROM read_parquet('{DATA_RAW}/documents_*.parquet')
            ORDER BY id
        """).fetchdf()

    def load_users(self) -> pd.DataFrame:
        return self.repo.execute(f"""
            SELECT *
            FROM read_parquet('{DATA_RAW}/users_*.parquet')
            ORDER BY id
        """).fetchdf()

    def populate_metadata(self) -> None:
        """Create tables and bulk-load metadata from raw Parquet files."""
        t0 = time.perf_counter()
        self.repo.create_metadata_store()
        self.repo.create_users_table()

        # Load documents metadata from raw Parquet into the metadata table
        self.repo.execute(f"""
            INSERT OR IGNORE INTO documents
            SELECT
                id, filename, language, source, user_id,
                word_count, page_count, token_count,
                FALSE AS embedding_status,
                checksum,
                content,
                created_at
            FROM read_parquet('{DATA_RAW}/documents_*.parquet')
        """)

        # Load users
        self.repo.execute(f"""
            INSERT OR IGNORE INTO users
            SELECT *
            FROM read_parquet('{DATA_RAW}/users_*.parquet')
        """)

        elapsed = time.perf_counter() - t0
        doc_count = self.repo.execute(
            "SELECT COUNT(*) FROM documents"
        ).fetchone()[0]  # type: ignore[index]
        user_count = self.repo.execute(
            "SELECT COUNT(*) FROM users"
        ).fetchone()[0]  # type: ignore[index]
        print(f"  ✓ Populated metadata: {doc_count:,} docs, {user_count:,} users "
              f"({elapsed:.3f}s)")

    def show_summary(self) -> None:
        """Print a summary of what's in the raw data."""
        result = self.repo.execute(f"""
            SELECT
                language,
                COUNT(*) AS doc_count,
                SUM(word_count) AS total_words,
                AVG(page_count) AS avg_pages
            FROM read_parquet('{DATA_RAW}/documents_*.parquet')
            GROUP BY language
            ORDER BY doc_count DESC
        """).fetchdf()
        print("\n  Raw data summary:")
        print(result.to_string(index=False))
