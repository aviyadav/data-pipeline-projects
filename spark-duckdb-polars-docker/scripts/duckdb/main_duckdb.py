import os
import time
from pathlib import Path
from datetime import date, timedelta
import random
import duckdb
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

    con = duckdb.connect()

    # Register local parquet files as views
    con.execute(f"""
        CREATE OR REPLACE VIEW raw_data AS SELECT * FROM read_parquet('{RAW_DATA_PATH}');
        CREATE OR REPLACE VIEW models AS SELECT * FROM read_parquet('{MODELS_PATH}');
    """)

    query = """
    WITH models_deduped AS (
        SELECT
            serial_number,
            ANY_VALUE(model) AS model
        FROM models
        GROUP BY serial_number
    ),

    windowed AS (
        SELECT
            r.* EXCLUDE (model),

            ROW_NUMBER() OVER (
                PARTITION BY serial_number
                ORDER BY date
            ) AS row_num,

            LAG(smart_5_raw, 1) OVER (
                PARTITION BY serial_number
                ORDER BY date
            ) AS previous_smart_5_raw,

            AVG(smart_5_raw) OVER (
                PARTITION BY serial_number
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS rolling_avg_smart_5_raw

        FROM raw_data r
    ),

    joined AS (
        SELECT
            w.*,
            m.model
        FROM windowed w
        LEFT JOIN models_deduped m
            ON w.serial_number = m.serial_number
    )

    SELECT
        COUNT(*) AS row_count,
        SUM(row_num) AS sum_row_num,
        SUM(previous_smart_5_raw) AS sum_previous,
        SUM(rolling_avg_smart_5_raw) AS sum_rolling_avg
    FROM joined
    """

    start = time.perf_counter()

    benchmark_result = con.execute(query).fetchall()
    elapsed = time.perf_counter() - start

    print("\nBenchmark Result:")
    print(benchmark_result)
    print(f"\nDuckDB elapsed: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()