import polars as pl
from loguru import logger


def build_features(purchases: pl.DataFrame) -> pl.DataFrame:
    logger.info("Building customer-level features with Polars")

    features = (
        purchases
        .with_columns(
            [
                pl.col("amount").cast(pl.Float64),
                pl.col("timestamp")
                    .str.to_datetime(time_zone="UTC")
                    .dt.replace_time_zone(None),
            ]
        )
        .with_columns(
            [
                pl.col("timestamp").dt.weekday().alias("weekday"),
                pl.col("timestamp").dt.hour().alias("purchase_hour"),
                (pl.col("amount") > 100).alias("high_value"),
            ]
        )
        .group_by("user_id")
        .agg(
            [
                pl.len().alias("orders"),
                pl.sum("amount").alias("lifetime_value"),
                pl.mean("amount").alias("average_order"),
                pl.max("amount").alias("largest_order"),
            ]
        )
    )

    logger.info(f"Built features for {features.height} customers")

    return features
