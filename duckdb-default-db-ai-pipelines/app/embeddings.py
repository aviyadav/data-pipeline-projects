"""Embedding generation — batch processing with streaming.

Points covered:
  - Batch embedding generation (not one-at-a-time)
  - DuckDB prepares work, workers consume it
  - OFFSET/LIMIT streaming for memory control
  - Persisting results as Parquet + updating metadata
  - Simulated embeddings (no heavy ML deps required to demonstrate)
"""

from __future__ import annotations

import hashlib
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.repository import DuckRepository
from app.settings import DATA_PROCESSED, DATA_EMBEDDINGS, BATCH_SIZE, EMBEDDING_BATCH_SIZE


def _generate_embeddings(texts: list[str], dim: int = 384) -> np.ndarray:
    """Simulate embedding generation with deterministic hashing.

    In production this would use ``SentenceTransformer.encode()``.
    Using a hash-based simulation avoids a 2 GB ML dependency while
    demonstrating the same batch-processing architecture.
    """
    rng = np.random.default_rng(
        seed=sum(ord(c) for t in texts for c in t[:50])
    )
    base = rng.normal(0, 1, (len(texts), dim)).astype(np.float32)
    for i, text in enumerate(texts):
        seed = text.encode()
        values: list[float] = []
        while len(values) < dim:
            seed = hashlib.sha256(seed).digest()
            chunk = np.frombuffer(seed, dtype=np.float32)
            values.extend(chunk.tolist())
        offset = np.array(values[:dim], dtype=np.float32)
        base[i] += offset * 0.01
    # Normalise using float64 to avoid overflow, cast back to float32
    norms = np.linalg.norm(base.astype(np.float64), axis=1, keepdims=True)
    norms[norms == 0] = 1
    return (base / norms).astype(np.float32)


def _embed_batch_worker(args: tuple[int, list[str], list[int]]) -> tuple[int, np.ndarray, list[int]]:
    """Worker function for parallel embedding generation."""
    batch_idx, texts, ids = args
    embeddings = _generate_embeddings(texts)
    return batch_idx, embeddings, ids


class EmbeddingService:
    """Generates embeddings in parallel batches with memory-conscious streaming."""

    def __init__(self, repo: DuckRepository) -> None:
        self.repo = repo

    def generate_all(self, max_workers: int | None = None) -> None:
        """Stream documents in batches, generate embeddings, persist results.

        Architecture (from the article):

              DuckDB (prepares batches)
                 │
        ┌────────┼────────────┐
        Worker 1  Worker 2  Worker 3
           │         │         │
        Embed     Embed     Embed
           │         │         │
        └────────┼────────────┘
              Parquet Output
        """
        workers = max_workers or max(1, cpu_count() - 1)
        t0 = time.perf_counter()
        offset = 0
        batch_idx = 0
        total_processed = 0
        input_path = f"{DATA_PROCESSED}/cleaned_documents.parquet"

        print(f"  Using {workers} workers for parallel embedding generation")

        while True:
            # DuckDB streams exactly the batch we need (point 7)
            batch = self.repo.execute(f"""
                SELECT id, content
                FROM read_parquet('{input_path}')
                ORDER BY id
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """).fetchdf()

            if batch.empty:
                break

            ids = batch["id"].tolist()
            texts = batch["content"].tolist()

            # Split into sub-batches for parallel workers
            sub_batches = []
            for i in range(0, len(texts), EMBEDDING_BATCH_SIZE):
                sub_batches.append((
                    batch_idx + len(sub_batches),
                    texts[i : i + EMBEDDING_BATCH_SIZE],
                    ids[i : i + EMBEDDING_BATCH_SIZE],
                ))

            # Parallel embedding generation
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_embed_batch_worker, sb): sb[0]
                    for sb in sub_batches
                }
                for future in as_completed(futures):
                    _, embs, sub_ids = future.result()
                    # Write to Parquet — build columns in one go
                    cols: dict[str, np.ndarray] = {"id": np.array(sub_ids, dtype=np.int64)}
                    for j in range(embs.shape[1]):
                        cols[f"e_{j}"] = embs[:, j]
                    table = pa.table(cols)
                    pq.write_table(
                        table,
                        f"{DATA_EMBEDDINGS}/batch_{batch_idx:05d}.parquet",
                    )
                    batch_idx += 1
                    total_processed += len(sub_ids)

            offset += BATCH_SIZE
            if offset % 50_000 == 0:
                print(f"  ... processed {offset:,} documents so far")

        elapsed = time.perf_counter() - t0
        print(f"  ✓ Embeddings generated: {total_processed:,} docs in "
              f"{elapsed:.1f}s ({total_processed / elapsed:,.0f} docs/s)")

    def verify(self) -> dict[str, int]:
        """Count total embedding records."""
        count = self.repo.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{DATA_EMBEDDINGS}/batch_*.parquet')
        """).fetchone()[0]  # type: ignore[index]
        return {"embedded_documents": count}
