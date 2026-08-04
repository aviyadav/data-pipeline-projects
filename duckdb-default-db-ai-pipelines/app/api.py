"""FastAPI service — serves processed data with embedded analytics.

Points covered:
  - FastAPI + DuckDB (no analytics warehouse, no separate read replica)
  - Embedded DuckDB inside the API process
  - Direct Parquet queries from the API

Concurrency model
-----------------
DuckDB uses file-level locking: only ONE process may hold a read-write
connection to the same ``.duckdb`` file at a time. However, MULTIPLE
processes may open read-only connections concurrently alongside a writer.

This API opens the database in READ_ONLY mode by default, so you can
run it with multiple uvicorn workers::

    uvicorn app.api:app --workers 4   # works — all read-only

And still run the pipeline (which writes) in another process::

    uv run python main.py pipeline    # writer — coexists with readers

If you need the API to write, set ``API_READ_ONLY=False`` in settings.py
and use a single worker (uvicorn's default).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.repository import DuckRepository
from app.settings import API_HOST, API_PORT, API_READ_ONLY

repo: DuckRepository


@asynccontextmanager
async def lifespan(app: FastAPI):
    global repo
    repo = DuckRepository(read_only=API_READ_ONLY)
    yield
    repo.close()


app = FastAPI(
    title="DuckDB AI Pipeline API",
    description="Serve analytical queries over Parquet data with embedded DuckDB.",
    lifespan=lifespan,
)


class DocumentResponse(BaseModel):
    id: int
    filename: str
    language: str
    source: str
    user_id: int
    word_count: int
    page_count: int
    token_count: int
    checksum: str


class StatsResponse(BaseModel):
    total_documents: int
    languages: dict[str, int]
    by_length: dict[str, int]
    by_subscription: dict[str, int]


@app.get("/documents/{document_id}", response_model=DocumentResponse)
def get_document(document_id: int) -> dict[str, Any]:
    """Fetch a single document by ID from the embedded DuckDB."""
    row = repo.execute(
        "SELECT id, filename, language, source, user_id, word_count, "
        "page_count, token_count, checksum "
        "FROM documents WHERE id = ?",
        [document_id],
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return dict(zip(
        ["id", "filename", "language", "source", "user_id",
         "word_count", "page_count", "token_count", "checksum"],
        row,
    ))


@app.get("/stats", response_model=StatsResponse)
def get_stats() -> dict[str, Any]:
    """Aggregate statistics using DuckDB analytical queries over Parquet."""
    total = repo.execute(
        "SELECT COUNT(*) FROM documents"
    ).fetchone()[0]  # type: ignore[index]

    langs = repo.execute(
        "SELECT language, COUNT(*) AS cnt FROM documents GROUP BY language"
    ).fetchall()
    languages = {row[0]: row[1] for row in langs}

    lengths = repo.execute("""
        SELECT
            CASE
                WHEN word_count < 500 THEN 'short'
                WHEN word_count < 2000 THEN 'medium'
                ELSE 'long'
            END AS cat,
            COUNT(*) AS cnt
        FROM documents
        GROUP BY cat
    """).fetchall()
    by_length = {row[0]: row[1] for row in lengths}

    subs = repo.execute("""
        SELECT u.subscription, COUNT(*)
        FROM documents d
        JOIN users u ON d.user_id = u.id
        GROUP BY u.subscription
    """).fetchall()
    by_subscription = {row[0]: row[1] for row in subs}

    return {
        "total_documents": total,
        "languages": languages,
        "by_length": by_length,
        "by_subscription": by_subscription,
    }


@app.get("/features/{document_id}")
def get_features(document_id: int) -> dict[str, Any]:
    """Query features directly from the processed Parquet file."""
    row = repo.execute("""
        SELECT *
        FROM read_parquet('data/processed/features.parquet')
        WHERE id = ?
    """, [document_id]).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Features not found")
    cols = repo.execute(
        "DESCRIBE SELECT * FROM read_parquet('data/processed/features.parquet')"
    ).fetchall()
    return dict(zip([c[0] for c in cols], row))


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "read_only" if API_READ_ONLY else "read_write"}


def run():
    import uvicorn
    uvicorn.run("app.api:app", host=API_HOST, port=API_PORT, reload=False)
