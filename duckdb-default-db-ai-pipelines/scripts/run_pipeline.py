#!/usr/bin/env python3
"""Orchestrate the full AI pipeline end-to-end with timing.

Stages (matching the article):
  1. Ingest     — scan raw Parquet → populate metadata store
  2. Preprocess — clean, deduplicate, filter
  3. Features   — join, compute derived columns
  4. Embeddings — parallel batch generation + persistence
"""

from __future__ import annotations

import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from app.repository import DuckRepository
from app.ingest import DocumentIngestionService
from app.preprocess import PreprocessingService
from app.features import FeatureEngineeringService
from app.embeddings import EmbeddingService
from app.settings import DATABASE_PATH, DATA_RAW

console = Console()


def clean_previous_runs() -> None:
    """Remove previous pipeline outputs for a clean run."""
    for pattern in [
        "data/processed/*.parquet",
        "data/embeddings/*.parquet",
    ]:
        for f in Path().glob(pattern):
            f.unlink()
    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()


def main() -> None:
    console.print(Panel.fit(
        "[bold green]DuckDB AI Pipeline[/bold green]\n"
        "Embedded OLAP → Direct Parquet → Streaming → Parallel Embeddings",
        border_style="green",
    ))

    timings: dict[str, float] = {}
    overall_start = time.perf_counter()

    # ── Setup ────────────────────────────────────────────────────────
    console.print("\n[bold]1. Setup[/bold]")
    clean_previous_runs()
    repo = DuckRepository()
    console.print("  ✓ DuckDB connected (no server, no connection strings)")

    # ── Ingest ───────────────────────────────────────────────────────
    console.print("\n[bold]2. Ingestion[/bold]")
    t0 = time.perf_counter()
    ingest = DocumentIngestionService(repo)
    ingest.populate_metadata()
    ingest.show_summary()
    timings["ingest"] = time.perf_counter() - t0

    # ── Preprocess ───────────────────────────────────────────────────
    console.print("\n[bold]3. Preprocessing[/bold]")
    t0 = time.perf_counter()
    preprocess = PreprocessingService(repo)
    preprocess.clean_and_deduplicate()
    stats = preprocess.validate()
    console.print(f"  Validation: {stats}")
    timings["preprocess"] = time.perf_counter() - t0

    # ── Feature Engineering ──────────────────────────────────────────
    console.print("\n[bold]4. Feature Engineering[/bold]")
    t0 = time.perf_counter()
    features = FeatureEngineeringService(repo)
    features.build_features()
    features.show_feature_stats()
    timings["features"] = time.perf_counter() - t0

    # ── Embeddings ───────────────────────────────────────────────────
    console.print("\n[bold]5. Embedding Generation[/bold]")
    t0 = time.perf_counter()
    emb = EmbeddingService(repo)
    emb.generate_all()
    emb_stats = emb.verify()
    console.print(f"  Verification: {emb_stats}")
    timings["embeddings"] = time.perf_counter() - t0

    # ── Summary ──────────────────────────────────────────────────────
    overall = time.perf_counter() - overall_start
    timings["total"] = overall

    console.print("\n")
    table = Table(title="Pipeline Timings", header_style="bold cyan")
    table.add_column("Stage", style="bold")
    table.add_column("Time (s)", justify="right")
    table.add_column("%", justify="right")
    for name, secs in timings.items():
        pct = f"{secs / overall * 100:.1f}%" if name != "total" else "—"
        table.add_row(name, f"{secs:.3f}", pct)
    console.print(table)

    repo.close()


if __name__ == "__main__":
    main()
