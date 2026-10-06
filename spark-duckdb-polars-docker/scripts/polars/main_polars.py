import os
import time
from pathlib import Path
from datetime import date, timedelta
import random
import polars as pl

# Local data paths (configurable via environment variables)
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/spark/work-dir/data"))
RAW_DATA_PATH = Path(os.environ.get("RAW_DATA_PATH", DATA_DIR / "raw_data.parquet"))
MODELS_PATH = Path(os.environ.get("MODELS_PATH", DATA_DIR / "models.parquet"))


def ensure_local_data():
    """Generate synthetic benchmark dataset if local files do not exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if not RAW_DATA_PATH.exists() or not MODELS_PATH.exists():
        print(f"Local benchmark data not found at {DATA_DIR}. Generating synthetic dataset...")
        
        # 100 hard drives tracked over 90 days
        num_drives = 100
        days = 90
        start_date = date(2024, 1, 1)
        
        serial_numbers = [f"WDC-WD{i:05d}" for i in range(1, num_drives + 1)]
        models_list = [f"WDC WD100EFAX" if i % 2 == 0 else "ST12000NM0008" for i in range(1, num_drives + 1)]
        
        models_records = [
            {"serial_number": sn, "model": m}
            for sn, m in zip(serial_numbers, models_list)
        ]
        models_df = pl.DataFrame(models_records)
        models_df.write_parquet(MODELS_PATH)
        
        raw_records = []
        for sn, m in zip(serial_numbers, models_list):
            val = random.randint(0, 50)
            for d in range(days):
                val += random.randint(0, 2)
                raw_records.append({
                    "serial_number": sn,
                    "model": m,
                    "date": start_date + timedelta(days=d),
                    "smart_5_raw": val
                })
        
        raw_df = pl.DataFrame(raw_records)
        raw_df.write_parquet(RAW_DATA_PATH)
        print(f"Generated {len(raw_df)} raw records and {len(models_df)} models records.")


def main():
    ensure_local_data()
    
    print(f"Scanning raw data from: {RAW_DATA_PATH}")
    print(f"Scanning models from:   {MODELS_PATH}")
    
    raw = pl.scan_parquet(str(RAW_DATA_PATH))
    
    models = (
        pl.scan_parquet(str(MODELS_PATH))
        .select([
            "serial_number",
            "model"
        ])
        .unique(
            subset=["serial_number"]
        )
    )

    start = time.perf_counter()

    result = (
        raw
        .drop("model")
        .sort([
            "serial_number",
            "date"
        ])

        .with_columns([

            pl.col("date")
            .cum_count()
            .over("serial_number")
            .alias("row_num"),

            pl.col("smart_5_raw")
            .shift(1)
            .over("serial_number")
            .alias("previous_smart_5_raw"),

            pl.col("smart_5_raw")
            .rolling_mean(
                window_size=30,
                min_periods=1
            )
            .over("serial_number")
            .alias("rolling_avg_smart_5_raw")
        ])

        .join(
            models,
            on="serial_number",
            how="left"
        )

        .select([
            pl.len().alias("row_count"),

            pl.col("row_num")
            .sum()
            .alias("sum_row_num"),

            pl.col("previous_smart_5_raw")
            .sum()
            .alias("sum_previous"),

            pl.col("rolling_avg_smart_5_raw")
            .sum()
            .alias("sum_rolling_avg")
        ])
    )

    benchmark_result = result.collect()

    elapsed = time.perf_counter() - start

    print("\nBenchmark Result:")
    print(benchmark_result)

    print(
        f"\nPolars elapsed: {elapsed:.2f} seconds"
    )


if __name__ == "__main__":
    main()