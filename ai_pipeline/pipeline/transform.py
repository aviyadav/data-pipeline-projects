import polars as pl
from loguru import logger
from pipeline.settings import settings
import duckdb


def get_purchases() -> pl.DataFrame:
    logger.info("Filtering purchases via Polars lazy scan")

    conn = duckdb.connect(settings.database)
    query = """
        SELECT user_id, country, amount, timestamp
        FROM read_parquet(?)
        WHERE event = 'purchase' AND amount > 0
        """

    arrow_table = conn.execute(query, [str(settings.bronze_dir / "events.parquet")]).arrow()
    return pl.from_arrow(arrow_table)
