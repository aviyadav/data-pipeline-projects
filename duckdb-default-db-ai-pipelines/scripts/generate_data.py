#!/usr/bin/env python3
"""Generate fake data using multiprocessing with memory-safe streaming.

Key design decisions:
  - Each worker writes its own Parquet file(s) — no shared memory state.
  - Batches are streamed to disk, never held entirely in memory.
  - File counts are configurable so output size is predictable.
  - Uses Faker for realistic-but-fake document/user data.
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from rich.console import Console
from rich.table import Table

from app.settings import (
    DATA_RAW,
    NUM_DOCUMENTS,
    NUM_USERS,
    PARALLEL_WORKERS,
    DOCS_PER_FILE,
    USERS_PER_FILE,
)

console = Console()
DATA_RAW.mkdir(parents=True, exist_ok=True)

LANGS = ["en", "en", "en", "fr", "de", "es", "ja", "zh", "pt"]
SOURCES = ["upload", "api", "email", "scraper", "import"]


def _generate_users_chunk(args: tuple[int, int, int]) -> tuple[int, str]:
    """Generate a chunk of user records → write a Parquet file."""
    chunk_idx, start_id, count = args
    fake = Faker()
    # Deterministic per-chunk seed for reproducibility
    Faker.seed(42 + chunk_idx)
    rng = np.random.default_rng(seed=42 + chunk_idx)

    ids = list(range(start_id, start_id + count))
    names = [fake.unique.name() for _ in range(count)]
    emails = [f"{n.replace(' ', '.').lower()}@example.com" for n in names]
    subscriptions = rng.choice(["free", "pro", "enterprise"], size=count, p=[0.6, 0.3, 0.1])
    signup_dates = pd.date_range("2020-01-01", periods=count, freq="h").strftime("%Y-%m-%d")
    countries = rng.choice(
        ["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "ES"],
        size=count,
    )

    df = pd.DataFrame({
        "id": ids,
        "name": names,
        "email": emails,
        "subscription": subscriptions,
        "signup_date": signup_dates[:count],
        "country": countries,
    })

    path = str(DATA_RAW / f"users_{chunk_idx:03d}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression="zstd")
    return count, path


def _generate_documents_chunk(args: tuple[int, int, int]) -> tuple[int, str]:
    """Generate a chunk of document records → write a Parquet file.

    Each document has a realistic-ish content field (generated text),
    metadata columns, and a checksum for dedup testing.
    """
    chunk_idx, start_id, count = args
    fake = Faker()
    Faker.seed(100 + chunk_idx)
    rng = np.random.default_rng(seed=100 + chunk_idx)

    ids = list(range(start_id, start_id + count))
    filenames = [f"doc_{i:08d}.txt" for i in ids]
    languages = rng.choice(LANGS, size=count)
    sources = rng.choice(SOURCES, size=count)
    user_ids = rng.integers(1, NUM_USERS + 1, size=count)
    word_counts = rng.integers(100, 10_000, size=count)
    page_counts = np.maximum(1, word_counts // rng.integers(200, 600, size=count))
    token_counts = (word_counts * rng.uniform(1.1, 1.8, size=count)).astype(int)

    # Generate content: paragraphs of fake text
    contents: list[str] = []
    for wc in word_counts:
        # ~5 words per fake sentence, target word count
        n_sents = max(1, wc // 5)
        paras = fake.paragraphs(nb=min(3, max(1, n_sents // 3)))
        text = " ".join(paras)
        # Trim/pad to roughly match word_count
        words = text.split()
        if len(words) > wc:
            text = " ".join(words[:wc])
        contents.append(text)

    timestamps = pd.date_range("2024-01-01", periods=count, freq="37s").strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    checksums = [
        hashlib.sha256(c.encode()).hexdigest()[:16] for c in contents
    ]

    df = pd.DataFrame({
        "id": ids,
        "filename": filenames,
        "language": languages,
        "source": sources,
        "user_id": user_ids,
        "word_count": word_counts,
        "page_count": page_counts,
        "token_count": token_counts,
        "content": contents,
        "created_at": timestamps[:count],
        "checksum": checksums,
    })

    path = str(DATA_RAW / f"documents_{chunk_idx:03d}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression="zstd")
    return count, path


def generate_users() -> None:
    """Generate user records in parallel chunks."""
    console.print("\n[bold cyan]Generating Users[/bold cyan]")
    t0 = time.perf_counter()

    num_chunks = max(1, NUM_USERS // USERS_PER_FILE)
    chunk_size = NUM_USERS // num_chunks
    tasks: list[tuple[int, int, int]] = []
    for i in range(num_chunks):
        start = i * chunk_size + 1
        size = chunk_size if i < num_chunks - 1 else NUM_USERS - (i * chunk_size)
        tasks.append((i, start, size))

    total = 0
    with mp.Pool(processes=min(PARALLEL_WORKERS, len(tasks))) as pool:
        for count, path in pool.imap_unordered(_generate_users_chunk, tasks):
            total += count
            console.print(f"    [dim]{path} ({count:,} rows)[/dim]")

    elapsed = time.perf_counter() - t0
    console.print(f"  ✓ {total:,} users in {elapsed:.1f}s")


def generate_documents() -> None:
    """Generate document records in parallel chunks.

    Using multiprocessing ensures the system never holds all data in memory
    simultaneously — each worker writes its own Parquet file independently.
    """
    console.print("\n[bold cyan]Generating Documents[/bold cyan]")
    t0 = time.perf_counter()

    num_chunks = max(1, NUM_DOCUMENTS // DOCS_PER_FILE)
    chunk_size = NUM_DOCUMENTS // num_chunks
    tasks: list[tuple[int, int, int]] = []
    for i in range(num_chunks):
        start = i * chunk_size + 1
        size = chunk_size if i < num_chunks - 1 else NUM_DOCUMENTS - (i * chunk_size)
        tasks.append((i, start, size))

    total = 0
    with mp.Pool(processes=min(PARALLEL_WORKERS, len(tasks))) as pool:
        for count, path in pool.imap_unordered(_generate_documents_chunk, tasks):
            total += count
            console.print(f"    [dim]{path} ({count:,} rows)[/dim]")

    elapsed = time.perf_counter() - t0
    console.print(f"  ✓ {total:,} documents in {elapsed:.1f}s "
                  f"({total / elapsed:,.0f} docs/s)")


def show_summary() -> None:
    """Print a quick summary of generated data files."""
    raw_files = sorted(DATA_RAW.glob("*.parquet"))
    table = Table(title="Generated Raw Data")
    table.add_column("File", style="dim")
    table.add_column("Size", justify="right")
    total_size = 0
    for f in raw_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        total_size += size_mb
        table.add_row(f.name, f"{size_mb:.1f} MB")
    table.add_section()
    table.add_row(
        f"[bold]{len(raw_files)} files[/bold]",
        f"[bold]{total_size:.1f} MB[/bold]",
    )
    console.print(table)


def main() -> None:
    console.print("[bold green]═══ Fake Data Generator ═══[/bold green]")
    console.print(f"  Documents: {NUM_DOCUMENTS:,}  |  Users: {NUM_USERS:,}")
    console.print(f"  Parallel workers: {PARALLEL_WORKERS}")
    console.print(f"  Output dir: {DATA_RAW}")

    # Clean any previous data
    for f in DATA_RAW.glob("*.parquet"):
        f.unlink()

    # Generate users first (needed for user_id FK references)
    t_start = time.perf_counter()
    generate_users()
    generate_documents()
    total_elapsed = time.perf_counter() - t_start

    console.print(f"\n[bold]Total generation time: {total_elapsed:.1f}s[/bold]")
    show_summary()


if __name__ == "__main__":
    mp.freeze_support()
    main()
