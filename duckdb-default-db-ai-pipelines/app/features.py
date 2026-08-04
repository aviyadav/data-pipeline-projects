"""Feature engineering — the analytical heart of the pipeline.

Points covered:
  - SQL-based feature engineering (JOINs, computed columns)
  - Direct Parquet JOINs (joining read_parquet sources)
  - COPY TO Parquet output
  - Declarative SQL replacing multiple Pandas transform steps
"""

from __future__ import annotations

import time

from app.repository import DuckRepository
from app.settings import DATA_PROCESSED, DATA_RAW


FEATURE_QUERY = """
COPY (
    SELECT
        d.id,
        d.filename,
        d.user_id,
        d.language,
        d.word_count,
        d.page_count,
        d.token_count,
        u.subscription,
        -- Computed features
        CAST(d.word_count AS DOUBLE) / NULLIF(d.page_count, 0)
            AS word_density,
        LENGTH(d.content)
            AS character_count,
        CAST(d.token_count AS DOUBLE) / NULLIF(d.word_count, 0)
            AS token_word_ratio,
        CASE
            WHEN d.word_count < 500  THEN 'short'
            WHEN d.word_count < 2000 THEN 'medium'
            ELSE 'long'
        END AS length_category
    FROM read_parquet('{cleaned}') d
    JOIN read_parquet('{users}') u
        ON d.user_id = u.id
    WHERE d.language = 'en'
)
TO '{output}'
(FORMAT PARQUET, COMPRESSION ZSTD)
"""


class FeatureEngineeringService:
    """Compute features with analytical SQL instead of Pandas."""

    def __init__(self, repo: DuckRepository) -> None:
        self.repo = repo

    def build_features(self) -> None:
        """Join documents ↔ users and compute derived columns.

        This is the article's key example — a single SQL statement replaces:
          pd.read_parquet → merge → filter → assign → to_parquet
        """
        t0 = time.perf_counter()

        cleaned = f"{DATA_PROCESSED}/cleaned_documents.parquet"
        users = f"{DATA_RAW}/users_*.parquet"
        output = f"{DATA_PROCESSED}/features.parquet"

        query = FEATURE_QUERY.format(
            cleaned=cleaned,
            users=users,
            output=output,
        )
        self.repo.execute(query)

        elapsed = time.perf_counter() - t0
        count = self.repo.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output}')"
        ).fetchone()[0]  # type: ignore[index]
        print(f"  ✓ Feature engineering complete → {count:,} rows "
              f"({elapsed:.3f}s)")

    def show_feature_stats(self) -> None:
        """Quick statistical summary of computed features."""
        output = f"{DATA_PROCESSED}/features.parquet"
        result = self.repo.execute(f"""
            SELECT
                subscription,
                length_category,
                COUNT(*) AS cnt,
                ROUND(AVG(word_density), 1) AS avg_word_density,
                ROUND(AVG(token_word_ratio), 3) AS avg_token_ratio
            FROM read_parquet('{output}')
            GROUP BY subscription, length_category
            ORDER BY subscription, length_category
        """).fetchdf()
        print("\n  Feature stats:")
        print(result.to_string(index=False))
