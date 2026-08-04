import shutil
from pathlib import Path

import polars as pl
import pytest

from generate_fake_data import generate_events
from main import main
from pipeline.settings import settings


@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    if Path("data").exists():
        shutil.rmtree("data")

    generate_events(1000)
    main()

    yield


def test_bronze_exists():
    assert (settings.bronze_dir / "events.parquet").exists()


def test_gold_exists():
    assert (settings.gold_dir / "customer_features.parquet").exists()


def test_gold_schema():
    df = pl.read_parquet(settings.gold_dir / "customer_features.parquet")

    expected_cols = {
        "user_id",
        "orders",
        "lifetime_value",
        "average_order",
        "largest_order",
    }

    assert set(df.columns) == expected_cols


def test_gold_data_quality():
    df = pl.read_parquet(settings.gold_dir / "customer_features.parquet")

    assert df.height > 0

    assert df.select((pl.col("orders") > 0).all()).item() is True
    assert df.select((pl.col("lifetime_value") >= 0).all()).item() is True
    assert df.select((pl.col("average_order") >= 0).all()).item() is True
    assert df.select((pl.col("largest_order") >= 0).all()).item() is True

    assert df["user_id"].n_unique() == df.height


def test_no_nulls_in_gold():
    df = pl.read_parquet(settings.gold_dir / "customer_features.parquet")

    null_counts = df.null_count()

    assert null_counts.sum_horizontal()[0] == 0
