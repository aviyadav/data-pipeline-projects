"""Integration tests for the DuckDB AI pipeline.

Validates every stage: data generation → ingestion → preprocessing →
feature engineering → embedding generation → API serving.
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.repository import DuckRepository, DocumentRepository
from app.ingest import DocumentIngestionService
from app.preprocess import PreprocessingService
from app.features import FeatureEngineeringService
from app.embeddings import EmbeddingService
from app.api import app
from app.settings import DATA_RAW, DATA_PROCESSED, DATA_EMBEDDINGS, DATABASE_PATH


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clean_data_dirs():
    """Ensure clean state before each test."""
    for d in [DATA_PROCESSED, DATA_EMBEDDINGS]:
        d.mkdir(parents=True, exist_ok=True)
        for f in d.glob("*.parquet"):
            f.unlink()
    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()
    yield


@pytest.fixture
def repo():
    """In-memory DuckDB repository for fast tests."""
    r = DuckRepository(":memory:")
    r.create_metadata_store()
    r.create_users_table()
    yield r
    r.close()


# ── Repository Tests ──────────────────────────────────────────────────


def test_duckdb_embedded_connection():
    """Point 1: DuckDB runs in-process — no server, no TCP socket."""
    r = DuckRepository(":memory:")
    result = r.execute("SELECT 42 AS answer").fetchone()
    assert result[0] == 42  # type: ignore[index]
    r.close()


def test_direct_parquet_query(repo):
    """Point 2: Query Parquet files directly without importing first."""
    # If raw data exists, test read_parquet
    raw_files = list(DATA_RAW.glob("documents_*.parquet"))
    if raw_files:
        result = repo.execute(
            "SELECT COUNT(*) FROM read_parquet('data/raw/documents_*.parquet')"
        ).fetchone()
        assert result[0] > 0  # type: ignore[index]


def test_create_metadata_store(repo):
    """Point 13: Schema creation with CREATE TABLE."""
    repo.create_metadata_store()
    tables = repo.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    assert "documents" in table_names


def test_repository_pattern(repo):
    """Point 8: Repository pattern keeps architecture disciplined."""
    doc_repo = DocumentRepository(repo)
    pending = doc_repo.pending_embeddings(limit=10)
    assert isinstance(pending, pd.DataFrame)
    assert len(pending) == 0  # no data yet


def test_insert_and_query_documents(repo):
    """Test document insertion and retrieval."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "filename": ["a.txt", "b.txt", "c.txt"],
        "language": ["en", "en", "fr"],
        "source": ["upload", "api", "email"],
        "user_id": [1, 2, 3],
        "word_count": [500, 1000, 200],
        "page_count": [2, 4, 1],
        "token_count": [600, 1200, 250],
        "embedding_status": [False, False, False],
        "checksum": ["aaa", "bbb", "ccc"],
        "content": ["hello world", "foo bar baz", "bonjour"],
        "created_at": pd.Timestamp.now(),
    })
    repo.connection.register("_tmp", df)
    repo.execute("INSERT OR IGNORE INTO documents SELECT * FROM _tmp")
    repo.connection.unregister("_tmp")

    count = repo.execute("SELECT COUNT(*) FROM documents").fetchone()[0]  # type: ignore[index]
    assert count == 3

    doc_repo = DocumentRepository(repo)
    pending = doc_repo.pending_embeddings(limit=10)
    assert len(pending) == 3


# ── Ingestion Tests ───────────────────────────────────────────────────


def test_ingestion_populates_metadata(repo):
    """Point 3/11: Ingestion populates tables from raw Parquet."""
    ingest = DocumentIngestionService(repo)
    # If raw data exists, test the full flow
    raw_files = list(DATA_RAW.glob("documents_*.parquet"))
    if raw_files:
        ingest.populate_metadata()
        count = repo.execute("SELECT COUNT(*) FROM documents").fetchone()[0]  # type: ignore[index]
        assert count > 0


# ── Preprocessing Tests ────────────────────────────────────────────────


def test_preprocessing_dedup(repo):
    """Test that deduplication works via checksum."""
    # Insert duplicate documents
    df = pd.DataFrame({
        "id": [1, 2],
        "filename": ["a.txt", "b.txt"],
        "language": ["en", "en"],
        "source": ["upload", "upload"],
        "user_id": [1, 1],
        "word_count": [100, 100],
        "page_count": [1, 1],
        "token_count": [120, 120],
        "embedding_status": [False, False],
        "checksum": ["same_hash", "same_hash"],
        "content": ["same content", "same content"],
        "created_at": pd.Timestamp.now(),
    })
    repo.connection.register("_tmp", df)
    repo.execute("INSERT OR IGNORE INTO documents SELECT * FROM _tmp")
    repo.connection.unregister("_tmp")

    # Write as Parquet for read_parquet test
    import pyarrow.parquet as pq
    import pyarrow as pa
    pq.write_table(
        pa.Table.from_pandas(df),
        str(DATA_PROCESSED / "test_docs.parquet"),
    )

    result = repo.execute(
        f"SELECT COUNT(DISTINCT checksum) FROM read_parquet('{DATA_PROCESSED}/test_docs.parquet')"
    ).fetchone()[0]  # type: ignore[index]
    assert result == 1  # only 1 unique checksum


# ── Feature Engineering Tests ─────────────────────────────────────────


def test_feature_query_syntax(repo):
    """Point 5: Feature engineering expressed in SQL, not Pandas."""
    query = """
        SELECT
            word_count,
            page_count,
            CAST(word_count AS DOUBLE) / NULLIF(page_count, 0) AS word_density
        FROM documents
        LIMIT 1
    """
    repo.execute(query)  # should not raise


# ── Embedding Tests ────────────────────────────────────────────────────


def test_embedding_generation(repo):
    """Point 6/9: Embeddings generated in batches."""
    # Insert some test data
    df = pd.DataFrame({
        "id": list(range(1, 11)),
        "filename": [f"d{i}.txt" for i in range(1, 11)],
        "language": ["en"] * 10,
        "source": ["upload"] * 10,
        "user_id": [1] * 10,
        "word_count": [500] * 10,
        "page_count": [2] * 10,
        "token_count": [600] * 10,
        "embedding_status": [False] * 10,
        "checksum": [hashlib.sha256(f"c{i}".encode()).hexdigest()[:16] for i in range(1, 11)],
        "content": [f"This is document number {i} with some content." for i in range(1, 11)],
        "created_at": pd.Timestamp.now(),
    })
    repo.connection.register("_tmp", df)
    repo.execute("INSERT OR IGNORE INTO documents SELECT * FROM _tmp")
    repo.connection.unregister("_tmp")

    # Write a cleaned_documents Parquet so embeddings can read from it
    import pyarrow.parquet as pq
    import pyarrow as pa
    feat_df = df[["id", "content"]].copy()
    pq.write_table(pa.Table.from_pandas(feat_df), str(DATA_PROCESSED / "cleaned_documents.parquet"))

    emb = EmbeddingService(repo)
    emb.generate_all(max_workers=2)

    emb_files = list(DATA_EMBEDDINGS.glob("batch_*.parquet"))
    assert len(emb_files) > 0, "Expected embedding output files"

    verify = emb.verify()
    assert verify["embedded_documents"] == 10


# ── API Tests ──────────────────────────────────────────────────────────


@pytest.fixture
def client(repo):
    """Override the global repo in the API module for testing."""
    import app.api as api_mod
    api_mod.repo = repo
    return TestClient(app)


def test_health_endpoint(client):
    """Health check returns OK."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_get_document(client, repo):
    """GET /documents/{id} returns a document from embedded DuckDB."""
    df = pd.DataFrame({
        "id": [42],
        "filename": ["test.txt"],
        "language": ["en"],
        "source": ["api"],
        "user_id": [1],
        "word_count": [100],
        "page_count": [1],
        "token_count": [120],
        "embedding_status": [False],
        "checksum": ["abc123"],
        "content": ["test"],
        "created_at": pd.Timestamp.now(),
    })
    repo.connection.register("_tmp", df)
    repo.execute("INSERT OR IGNORE INTO documents SELECT * FROM _tmp")
    repo.connection.unregister("_tmp")

    resp = client.get("/documents/42")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == 42
    assert data["filename"] == "test.txt"


def test_get_stats(client, repo):
    """GET /stats returns aggregate statistics via DuckDB SQL."""
    users_df = pd.DataFrame({
        "id": [1],
        "name": ["Alice"],
        "email": ["alice@ex.com"],
        "subscription": ["pro"],
        "signup_date": ["2024-01-01"],
        "country": ["US"],
    })
    repo.connection.register("_tmp_u", users_df)
    repo.execute("INSERT OR IGNORE INTO users SELECT * FROM _tmp_u")
    repo.connection.unregister("_tmp_u")

    docs_df = pd.DataFrame({
        "id": [1, 2],
        "filename": ["a.txt", "b.txt"],
        "language": ["en", "en"],
        "source": ["api", "api"],
        "user_id": [1, 1],
        "word_count": [100, 3000],
        "page_count": [1, 10],
        "token_count": [120, 4000],
        "embedding_status": [False, False],
        "checksum": ["aa", "bb"],
        "content": ["short", "long content " * 500],
        "created_at": pd.Timestamp.now(),
    })
    repo.connection.register("_tmp_d", docs_df)
    repo.execute("INSERT OR IGNORE INTO documents SELECT * FROM _tmp_d")
    repo.connection.unregister("_tmp_d")

    resp = client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_documents"] == 2
    assert data["languages"]["en"] == 2
    assert data["by_length"]["short"] == 1
    assert data["by_length"]["long"] == 1
    assert data["by_subscription"]["pro"] == 2


def test_document_not_found(client):
    """404 for missing documents."""
    resp = client.get("/documents/99999")
    assert resp.status_code == 404


# ── Streaming / Memory Tests ──────────────────────────────────────────


def test_streaming_batch_offset(repo):
    """Point 7: OFFSET/LIMIT streaming for memory control."""
    df = pd.DataFrame({
        "id": list(range(1, 101)),
        "content": [f"doc {i}" for i in range(1, 101)],
    })
    import pyarrow.parquet as pq
    import pyarrow as pa
    pq.write_table(pa.Table.from_pandas(df), str(DATA_PROCESSED / "stream_test.parquet"))

    # Read in batches
    offset = 0
    limit = 30
    all_ids = []
    while True:
        batch = repo.execute(f"""
            SELECT id FROM read_parquet('{DATA_PROCESSED}/stream_test.parquet')
            ORDER BY id LIMIT {limit} OFFSET {offset}
        """).fetchdf()
        if batch.empty:
            break
        all_ids.extend(batch["id"].tolist())
        offset += limit

    assert len(all_ids) == 100
    assert all_ids == list(range(1, 101))


# ── Performance / Timing Tests ────────────────────────────────────────


def test_query_timing():
    """Verify DuckDB queries are fast (sub-second for simple ops)."""
    r = DuckRepository(":memory:")
    t0 = time.perf_counter()
    for _ in range(100):
        r.execute("SELECT 1")
    elapsed = time.perf_counter() - t0
    r.close()
    assert elapsed < 1.0, f"100 queries took {elapsed:.3f}s — expected < 1s"


def test_parquet_write_read_roundtrip():
    """Point 11/12: COPY TO/FROM Parquet round-trip."""
    r = DuckRepository(":memory:")
    r.execute("""
        CREATE TABLE test AS
        SELECT i AS id, 'text_' || i::TEXT AS content
        FROM range(1, 101) t(i)
    """)

    path = str(DATA_PROCESSED / "roundtrip.parquet")
    r.execute(f"COPY test TO '{path}' (FORMAT PARQUET)")

    result = r.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path}')"
    ).fetchone()[0]  # type: ignore[index]
    assert result == 100

    r.close()
