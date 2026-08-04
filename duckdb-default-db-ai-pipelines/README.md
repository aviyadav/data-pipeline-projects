# DuckDB — Default Database for AI Pipelines

A working end-to-end demonstration of the patterns described in
["Why DuckDB Is Becoming the Default Database for AI Pipelines"](https://medium.com/@komalbaparmar007/why-duckdb-is-becoming-the-default-database-for-ai-pipelines-8df9218bec58).

**No Docker. No Spark. No warehouse server. No PostgreSQL.**
Just DuckDB, Parquet, and Python — with multiprocessing for data generation and parallel embedding, all memory-safe.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Commands](#commands)
- [Pipeline Stages](#pipeline-stages)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [API Endpoints](#api-endpoints)
- [Concurrency Model](#concurrency-model)
- [Configuration](#configuration)
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [Article Coverage](#article-coverage)

---

## Quick Start

```bash
# 1. Install dependencies (Python 3.14+)
uv sync

# 2. Generate fake data — 500k documents + 50k users via multiprocessing (~7s)
uv run python main.py generate

# 3. Run the full pipeline — ingest → preprocess → features → embeddings (~22s)
uv run python main.py pipeline

# 4. Start the API server (read-only — safe to run alongside the pipeline)
uv run python main.py serve

# 5. Run the test suite (16 tests)
uv run python main.py test
```

---

## Commands

| Command | Description |
|---------|-------------|
| `uv run python main.py generate` | Generate fake data using 8 parallel workers. Each worker writes its own Parquet file — no shared memory, no OOM risk. |
| `uv run python main.py pipeline` | Run the full pipeline end-to-end with per-stage timing. |
| `uv run python main.py serve` | Start FastAPI on `127.0.0.1:8000`. Opens DuckDB in **read-only** mode so it can coexist with a running pipeline. |
| `uv run python main.py test` | Run all 16 integration tests. |

### Running API with Multiple Workers

```bash
# Multiple uvicorn workers (all read-only — safe)
uv run uvicorn app.api:app --workers 4 --host 127.0.0.1 --port 8000

# While the pipeline runs in another terminal:
uv run python main.py pipeline
```

---

## Pipeline Stages

### 1. Ingestion
DuckDB reads raw Parquet files **directly** via `read_parquet()` — no Pandas import step.
Populates the embedded `documents` and `users` tables in `data/pipeline.duckdb`.

```
read_parquet('data/raw/documents_*.parquet')
       │
       ▼
INSERT INTO documents (metadata table)
```

### 2. Preprocessing
Deduplicates by checksum, filters to English documents, writes `cleaned_documents.parquet` via `COPY TO` with ZSTD compression.

```sql
COPY (
    SELECT DISTINCT ON (checksum) *
    FROM read_parquet('data/raw/documents_*.parquet')
    WHERE language = 'en'
) TO 'data/processed/cleaned_documents.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
```

### 3. Feature Engineering
Joins documents with users and computes derived columns — **all in a single SQL statement**.
Replaces the Pandas `merge → filter → assign → to_parquet` chain.

```sql
COPY (
    SELECT d.*, u.subscription,
           d.word_count / NULLIF(d.page_count, 0) AS word_density,
           LENGTH(d.content) AS character_count,
           CASE WHEN d.word_count < 500 THEN 'short' ... END AS length_category
    FROM read_parquet('.../cleaned_documents.parquet') d
    JOIN read_parquet('data/raw/users_*.parquet') u ON d.user_id = u.id
) TO 'data/processed/features.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
```

### 4. Embedding Generation
Streams documents in batches via `LIMIT/OFFSET`, distributes to parallel workers via `ProcessPoolExecutor`, writes results as Parquet files.

```
DuckDB (prepares batches)
       │
  ┌────┼────┬────┐
  W1   W2   W3   W4   (ProcessPoolExecutor)
  │    │    │    │
  └────┼────┴────┘
   Parquet Output (data/embeddings/batch_*.parquet)
```

A **simulated** embedding function (deterministic hash → float32 vector → L2 normalise) avoids the 2 GB `sentence-transformers` dependency while demonstrating the same architecture.

### 5. API Serving
FastAPI serves data with DuckDB embedded **in-process**. No separate analytics warehouse or read replica needed. Opens in read-only mode for concurrency safety.

---

## Project Structure

```
duckdb-default-db-ai-pipelines/
├── app/
│   ├── __init__.py
│   ├── settings.py       # Centralised config: paths, batch sizes, worker count
│   ├── repository.py     # DuckRepository (embedded DB) + DocumentRepository (domain queries)
│   ├── ingest.py         # DocumentIngestionService: read_parquet → populate tables
│   ├── preprocess.py     # SQL-based dedup, filtering, COPY TO Parquet
│   ├── features.py       # Feature engineering: JOIN + computed columns in SQL
│   ├── embeddings.py     # Parallel batch embedding with ProcessPoolExecutor
│   └── api.py            # FastAPI: /documents/{id}, /stats, /features/{id}, /health
├── scripts/
│   ├── __init__.py
│   ├── generate_data.py  # Fake data generator using multiprocessing (8 workers)
│   └── run_pipeline.py   # Orchestrator: runs all stages with timing
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py  # 16 integration tests covering every stage
├── data/
│   ├── raw/              # Generated Parquet files (10 × 50k docs + 1 × 50k users)
│   ├── processed/        # cleaned_documents.parquet + features.parquet
│   ├── embeddings/       # 1,331 batch Parquet files (one per 128-doc sub-batch)
│   └── pipeline.duckdb   # Embedded DuckDB database file
├── pyproject.toml         # uv-managed dependencies (no Docker)
├── main.py                # CLI entry point
└── README.md
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     data/raw/*.parquet                       │
│               (generated by multiprocessing)                 │
└──────────────────────────┬──────────────────────────────────┘
                           │ read_parquet() — no import step
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  1. INGESTION                                               │
│  DuckDB populates metadata tables in pipeline.duckdb        │
│  (1.2s for 500k docs)                                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  2. PREPROCESSING                                           │
│  Dedup by checksum, filter en → COPY TO cleaned.parquet     │
│  (0.3s for 166k docs)                                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  3. FEATURE ENGINEERING                                     │
│  JOIN docs⟷users, compute word_density, token_ratio,        │
│  length_category → COPY TO features.parquet                 │
│  (0.1s for 166k docs)                                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  4. EMBEDDINGS                                              │
│  Stream batches (LIMIT/OFFSET) → ProcessPoolExecutor        │
│  → Parquet output.  8,100 docs/s with 7 workers.            │
│  (20.7s for 166k docs)                                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  5. FASTAPI (port 8000)                                     │
│  DuckDB read-only — coexists with running pipeline.         │
│  /documents/{id}  /stats  /features/{id}  /health           │
└─────────────────────────────────────────────────────────────┘
```

**What's NOT needed:** Spark clusters, PostgreSQL, Docker, connection pools, ORMs, temporary CSV files, analytics warehouses, TCP sockets, or infrastructure YAML.

---

## API Endpoints

| Method | Path | Description | Example |
|--------|------|-------------|---------|
| `GET` | `/health` | Health check + access mode | `{"status":"ok","mode":"read_only"}` |
| `GET` | `/documents/{id}` | Fetch document by ID | `/documents/42` |
| `GET` | `/stats` | Aggregate stats (languages, lengths, subscriptions) | `/stats` |
| `GET` | `/features/{id}` | Feature vector from processed Parquet | `/features/42` |

### Example Responses

**`GET /documents/42`**
```json
{
  "id": 42,
  "filename": "doc_00000042.txt",
  "language": "en",
  "source": "upload",
  "user_id": 1234,
  "word_count": 2500,
  "page_count": 5,
  "token_count": 3200,
  "checksum": "a1b2c3d4e5f6g7h8"
}
```

**`GET /stats`**
```json
{
  "total_documents": 500000,
  "languages": {"en": 166336, "zh": 56005, "ja": 55808, ...},
  "by_length": {"short": 6800, "medium": 25200, "long": 134336},
  "by_subscription": {"free": 99200, "pro": 50600, "enterprise": 16536}
}
```

---

## Concurrency Model

DuckDB uses **file-level locking** on the `.duckdb` database file:

| Connection mode | Concurrent instances |
|-----------------|---------------------|
| 1 × read-write  | **Exclusive** — blocks all other connections |
| N × read-only   | **Shared** — multiple readers, even alongside 1 writer |

### The API opens in read-only mode by default

Configured via `API_READ_ONLY = True` in `app/settings.py`.

```python
# app/repository.py
def __init__(self, database=..., read_only: bool = False):
    config = {}
    if read_only:
        config["access_mode"] = "READ_ONLY"
    self.connection = duckdb.connect(database=str(database), config=config)
```

### Running concurrently

```bash
# Terminal 1 — pipeline (writer)
uv run python main.py pipeline

# Terminal 2 — API with 4 workers (all read-only)
uv run uvicorn app.api:app --workers 4 --host 127.0.0.1 --port 8000
```

All 4 API workers open read-only connections. DuckDB allows multiple readers alongside one writer. **No lock errors.**

### If the API needs write access

Set `API_READ_ONLY = False` in settings.py and use a **single worker**:

```bash
uvicorn app.api:app --workers 1
```

With one worker, uvicorn handles concurrency through asyncio (many concurrent requests on a single process), which scales well for I/O-bound workloads.

---

## Configuration

All tunables live in `app/settings.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `NUM_DOCUMENTS` | 500,000 | Total fake documents to generate |
| `NUM_USERS` | 50,000 | Total fake users to generate |
| `PARALLEL_WORKERS` | 8 | Multiprocessing workers for data generation |
| `DOCS_PER_FILE` | 50,000 | Documents per raw Parquet file |
| `BATCH_SIZE` | 5,000 | Streaming batch size for the pipeline |
| `EMBEDDING_BATCH_SIZE` | 128 | Documents per embedding sub-batch |
| `API_HOST` | `127.0.0.1` | FastAPI bind address |
| `API_PORT` | 8000 | FastAPI port |
| `API_READ_ONLY` | `True` | Open DuckDB in read-only mode for API |

---

## Benchmarks

Hardware: standard laptop (8 cores), Python 3.14, DuckDB 1.5.5.
Dataset: 500,000 documents, 50,000 users, 7 languages.

### Data Generation (multiprocessing, 8 workers)

| Output | Rows | Time | Throughput |
|--------|------|------|-----------|
| Documents | 500,000 | 4.5s | 111,959 docs/s |
| Users | 50,000 | 2.7s | — |
| **Total** | — | **7.2s** | 69 MB of Parquet |

### Pipeline (166k English docs after filtering)

| Stage | Time | % of total |
|-------|------|-----------|
| Ingestion | 1.25s | 5.6% |
| Preprocessing | 0.31s | 1.4% |
| Feature engineering | 0.13s | 0.6% |
| Embedding generation | 20.7s | 92.3% |
| **Total pipeline** | **22.4s** | — |

Embedding throughput: **8,100 docs/s** with 7 parallel workers (384-dim float32 vectors).

### Query performance

| Operation | Time |
|-----------|------|
| 100 × `SELECT 1` | < 800ms |
| `CREATE TABLE + COPY TO Parquet` (100 rows) | < 50ms |
| `read_parquet` + `COUNT` (500k rows) | < 50ms |

---

## Testing

16 integration tests covering every pipeline stage:

```bash
uv run pytest tests/ -v
```

| Test | What it validates |
|------|-------------------|
| `test_duckdb_embedded_connection` | Point 1: in-process, no server |
| `test_direct_parquet_query` | Point 2: `read_parquet` without import |
| `test_create_metadata_store` | Point 13: schema creation |
| `test_repository_pattern` | Point 8: `DocumentRepository` encapsulation |
| `test_insert_and_query_documents` | Insert/query round-trip |
| `test_ingestion_populates_metadata` | Point 3: bulk-load from raw Parquet |
| `test_preprocessing_dedup` | Point 4: dedup + filter in SQL |
| `test_feature_query_syntax` | Point 5: feature engineering SQL |
| `test_embedding_generation` | Points 6+9: parallel batch embeddings |
| `test_health_endpoint` | API health check |
| `test_get_document` | Point 10: GET /documents/{id} |
| `test_get_stats` | Point 10: GET /stats with JOINs |
| `test_document_not_found` | 404 handling |
| `test_streaming_batch_offset` | Point 7: LIMIT/OFFSET streaming |
| `test_query_timing` | Performance: 100 queries < 1s |
| `test_parquet_write_read_roundtrip` | Points 11+12: COPY TO/FROM Parquet |

---

## Article Coverage

All 14 concepts from the article are implemented:

| # | Article Concept | Implementation File | Key Code |
|---|----------------|--------------------|----------|
| 1 | Embedded, in-process OLAP | `repository.py` | `duckdb.connect(database=...)` — no server, no TCP |
| 2 | Direct Parquet queries | `repository.py:123` | `read_parquet('data/raw/*.parquet')` |
| 3 | Zero-infra architecture | `pyproject.toml` | No Docker, Spark, or warehouse deps |
| 4 | ETL simplification | `preprocess.py` | SQL replaces Pandas→CSV→Spark chains |
| 5 | Feature engineering in SQL | `features.py` | `JOIN read_parquet(...)` + computed columns |
| 6 | Batch embedding generation | `embeddings.py` | 128-doc sub-batches from DuckDB |
| 7 | Streaming LIMIT/OFFSET | `repository.py:148` | Memory-safe batch processing |
| 8 | Repository pattern | `repository.py:80` | `DocumentRepository` encapsulating SQL |
| 9 | Parallel processing | `embeddings.py:55` | `ProcessPoolExecutor` workers |
| 10 | FastAPI serving | `api.py` | `/documents/{id}`, `/stats`, `/features/{id}` |
| 11 | COPY TO Parquet | `preprocess.py:44` | SQL→file with ZSTD compression |
| 12 | Parquet JOINs | `features.py:24` | `read_parquet` as join sources |
| 13 | Metadata store | `repository.py:48` | `CREATE TABLE` + status tracking |
| 14 | Memory-efficient design | `generate_data.py` + `ingest.py` | Multiprocessing + streaming |

---

## Dependencies

```toml
# pyproject.toml
dependencies = [
    "duckdb>=1.2",        # Embedded analytical SQL engine
    "pyarrow>=18",         # Parquet read/write, Arrow tables
    "pandas>=2.3",         # DataFrame support
    "faker>=33",           # Fake data generation
    "fastapi>=0.115",      # API framework
    "uvicorn>=0.34",       # ASGI server
    "pydantic>=2.11",      # Data validation
    "rich>=13",            # Terminal formatting & tables
]
```

No Docker. No Spark. No warehouse. Just `uv sync` and you're running.
