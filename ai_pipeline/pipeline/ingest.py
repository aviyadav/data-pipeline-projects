import polars as pl
from loguru import logger
from pipeline.settings import settings

def ingest():
    logger.info("Starting ingestion from raw NDJSON to Bronze Parquet")

    events = (
        pl.scan_ndjson(str(settings.raw_dir / "*.json"))
        .select(["user_id", "country", "event", "amount", "timestamp"])
    )

    out_path = str(settings.bronze_dir / "events.parquet")
    events.sink_parquet(out_path, compression="zstd")

    logger.info(f"Bronze layer written to {out_path}")
