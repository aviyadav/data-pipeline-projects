"""Repository layer — encapsulates all DuckDB interactions.

Points covered:
  - Embedded, in-process DuckDB (no server, no connection strings)
  - Repository pattern for architectural discipline
  - Direct Parquet queries via ``read_parquet()``
  - Metadata store with CREATE TABLE
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.settings import DATABASE_PATH


class DuckRepository:
    """Thin wrapper around an *embedded* DuckDB connection.

    No connection strings, no TCP sockets, no Docker — the database lives
    inside the Python process.
    """

    def __init__(
        self,
        database: str | Path = DATABASE_PATH,
        read_only: bool = False,
    ) -> None:
        config: dict[str, str] = {}
        if read_only:
            config["access_mode"] = "READ_ONLY"
        self.connection: duckdb.DuckDBPyConnection = duckdb.connect(
            database=str(database),
            config=config,
        )

    def execute(self, query: str, params: Any = None) -> duckdb.DuckDBPyRelation:
        if params is not None:
            return self.connection.execute(query, params)
        return self.connection.execute(query)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        return self.connection.sql(query)

    def close(self) -> None:
        self.connection.close()

    # ── Schema helpers ───────────────────────────────────────────────

    def create_metadata_store(self) -> None:
        """Create the documents metadata table (point 13 in the article)."""
        self.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id          BIGINT PRIMARY KEY,
                filename    TEXT,
                language    TEXT,
                source      TEXT,
                user_id     BIGINT,
                word_count  INTEGER,
                page_count  INTEGER,
                token_count INTEGER,
                embedding_status BOOLEAN DEFAULT FALSE,
                checksum    TEXT,
                content     TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def create_users_table(self) -> None:
        self.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id           BIGINT PRIMARY KEY,
                name         TEXT,
                email        TEXT,
                subscription TEXT,   -- 'free', 'pro', 'enterprise'
                signup_date  DATE,
                country      TEXT
            )
        """)


class DocumentRepository:
    """Domain repository exposing analytical queries over documents."""

    def __init__(self, db: DuckRepository) -> None:
        self.db = db

    def pending_embeddings(self, limit: int = 128) -> pd.DataFrame:
        """Return documents that still need embeddings generated."""
        return self.db.execute(
            """
            SELECT id, content
            FROM documents
            WHERE embedding_status = FALSE
            ORDER BY id
            LIMIT ?
            """,
            [limit],
        ).fetchdf()

    def mark_completed(self, ids: list[int]) -> None:
        """Mark a batch of document IDs as having embeddings completed."""
        self.db.execute(
            """
            UPDATE documents
            SET embedding_status = TRUE
            WHERE id IN (SELECT UNNEST(?))
            """,
            [ids],
        )

    def insert_documents(self, df: pd.DataFrame) -> None:
        """Bulk-insert document metadata (used during ingestion)."""
        # Hash content for dedup
        df["checksum"] = df["content"].apply(
            lambda c: hashlib.sha256(c.encode()).hexdigest()[:16]
        )
        self.db.connection.register("_tmp_docs", df)
        self.db.execute("""
            INSERT OR IGNORE INTO documents
            SELECT * FROM _tmp_docs
        """)
        self.db.connection.unregister("_tmp_docs")

    def scan_raw_parquet(self, glob: str = "data/raw/documents_*.parquet") -> pd.DataFrame:
        """Query Parquet files *directly* — no import step (point 2)."""
        return self.db.execute(
            f"""
            SELECT
                filename,
                language,
                COUNT(*) AS chunks
            FROM read_parquet('{glob}')
            GROUP BY filename, language
            """
        ).fetchdf()

    def stream_documents(self, glob: str, offset: int, limit: int) -> pd.DataFrame:
        """Streaming batch with OFFSET/LIMIT for memory control (point 7)."""
        return self.db.execute(
            f"""
            SELECT *
            FROM read_parquet('{glob}')
            ORDER BY id
            LIMIT {limit} OFFSET {offset}
            """
        ).fetchdf()
