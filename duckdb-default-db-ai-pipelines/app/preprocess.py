"""Preprocessing service — cleaning, deduplication, filtering.

Points covered:
  - SQL-based cleaning instead of Pandas transforms
  - COPY TO Parquet for efficient output
  - Declarative pipelines that are easier to reason about
"""

from __future__ import annotations

import time

from app.repository import DuckRepository
from app.settings import DATA_RAW, DATA_PROCESSED


class PreprocessingService:
    """Deduplicate, clean, and filter documents — all in SQL."""

    def __init__(self, repo: DuckRepository) -> None:
        self.repo = repo

    def clean_and_deduplicate(self) -> None:
        """Remove duplicates (by checksum) and filter to English documents.

        The article emphasises this pattern:
          Instead of Pandas merge + filter + transform, express the whole
          pipeline declaratively in SQL and COPY the result to Parquet.
        """
        t0 = time.perf_counter()
        glob = f"{DATA_RAW}/documents_*.parquet"
        output = f"{DATA_PROCESSED}/cleaned_documents.parquet"

        self.repo.execute(f"""
            COPY (
                SELECT DISTINCT ON (checksum)
                    id, filename, language, source, user_id,
                    word_count, page_count, token_count,
                    content, checksum
                FROM read_parquet('{glob}')
                WHERE language = 'en'
                ORDER BY checksum, id
            )
            TO '{output}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        elapsed = time.perf_counter() - t0
        count = self.repo.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output}')"
        ).fetchone()[0]  # type: ignore[index]
        print(f"  ✓ Cleaned & deduplicated → {count:,} English docs "
              f"({elapsed:.3f}s)")

    def validate(self) -> dict[str, int]:
        """Return quick validation stats."""
        output = f"{DATA_PROCESSED}/cleaned_documents.parquet"
        return {
            "total": self.repo.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output}')"
            ).fetchone()[0],  # type: ignore[index]
            "unique_checksums": self.repo.execute(
                f"SELECT COUNT(DISTINCT checksum) FROM read_parquet('{output}')"
            ).fetchone()[0],  # type: ignore[index]
        }
