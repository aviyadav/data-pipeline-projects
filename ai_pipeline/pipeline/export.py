import polars as pl
from loguru import logger
from pipeline.settings import settings

def export(features: pl.DataFrame):
    out_path = str(settings.gold_dir / "customer_features.parquet")
    features.write_parquet(out_path, compression="zstd")
    logger.info(f"Gold dataset exported to {out_path}")
