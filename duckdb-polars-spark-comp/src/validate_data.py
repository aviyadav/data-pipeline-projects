#!/usr/bin/env python3
"""Validate a generated dataset before it is benchmarked.

The generator writes millions of rows in chunks; a chunking mistake would be
invisible to the benchmarks and would silently invalidate the comparison. These
checks assert the structural invariants the benchmark relies on:

* the fact table has exactly the requested row count;
* every drive's observations are contiguous and exactly one day apart;
* the SMART counters are non-negative and in-range;
* ``models`` has exactly one row per ``serial_number`` and matches the fact table.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (  # noqa: E402
    BENCH_SMART_NORMALIZED,
    BENCH_SMART_RAW,
    DATA_DIR,
    DEFAULT_ROWS,
    RAW_COLUMNS,
    RESULTS_DIR,
)

FAILURES: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {label}{(' -- ' + detail) if detail else ''}", flush=True)
    if not ok:
        FAILURES.append(label)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=None, help="expected row count")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args(argv)

    raw_glob = str(args.data_dir / "raw_data" / "*.parquet")
    models_glob = str(args.data_dir / "models" / "*.parquet")

    expected = args.rows
    manifest_path = args.data_dir / "dataset.json"
    if expected is None and manifest_path.exists():
        expected = json.loads(manifest_path.read_text(encoding="utf-8"))["rows"]
    if expected is None:
        expected = DEFAULT_ROWS

    con = duckdb.connect()
    con.execute("SET threads TO 8")

    print("== dataset validation ==")
    print(f"  raw glob   : {raw_glob}")
    print(f"  models glob: {models_glob}")
    print(f"  expected   : {expected:,} rows x {len(RAW_COLUMNS)} columns")

    rows = con.execute(f"SELECT count(*) FROM read_parquet('{raw_glob}')").fetchone()[0]
    check("fact row count", rows == expected, f"got {rows:,}")

    drives = con.execute(
        f"SELECT count(DISTINCT serial_number) FROM read_parquet('{raw_glob}')"
    ).fetchone()[0]
    model_rows = con.execute(f"SELECT count(*) FROM read_parquet('{models_glob}')").fetchone()[0]
    model_distinct = con.execute(
        f"SELECT count(DISTINCT serial_number) FROM read_parquet('{models_glob}')"
    ).fetchone()[0]
    check("models row count == distinct drives", model_rows == drives, f"{model_rows:,} vs {drives:,}")
    check("models serial_number is unique", model_rows == model_distinct)

    missing = con.execute(
        f"""
        SELECT count(*) FROM read_parquet('{raw_glob}') r
        WHERE NOT EXISTS (
            SELECT 1 FROM read_parquet('{models_glob}') m
            WHERE m.serial_number = r.serial_number
        )
        """
    ).fetchone()[0]
    check("every fact drive exists in models", missing == 0, f"{missing:,} unmatched rows")

    nulls = con.execute(
        f"""
        SELECT count(*) FROM read_parquet('{raw_glob}')
        WHERE date IS NULL OR serial_number IS NULL OR model IS NULL
           OR capacity_bytes IS NULL OR failure IS NULL
        """
    ).fetchone()[0]
    check("no NULL key columns", nulls == 0, f"{nulls:,} rows")

    negatives = con.execute(
        f"""
        SELECT count(*) FROM read_parquet('{raw_glob}')
        WHERE {BENCH_SMART_RAW} < 0 OR {BENCH_SMART_NORMALIZED} < 0
           OR {BENCH_SMART_NORMALIZED} > 255
        """
    ).fetchone()[0]
    check("SMART counters in range", negatives == 0, f"{negatives:,} bad rows")

    bad_failure = con.execute(
        f"SELECT count(*) FROM read_parquet('{raw_glob}') WHERE failure NOT IN (0, 1)"
    ).fetchone()[0]
    check("failure flag is 0/1", bad_failure == 0, f"{bad_failure:,} rows")

    # Contiguity: a drive observed for n days must span exactly n-1 days and
    # must never share a date twice.
    gaps = con.execute(
        f"""
        SELECT count(*) FROM (
            SELECT serial_number
            FROM read_parquet('{raw_glob}')
            GROUP BY serial_number
            HAVING date_diff('day', min(date), max(date)) + 1 <> count(*)
                OR count(DISTINCT date) <> count(*)
        )
        """
    ).fetchone()[0]
    check("each drive is a contiguous daily series", gaps == 0, f"{gaps:,} drives broken")

    failures = con.execute(f"SELECT sum(failure) FROM read_parquet('{raw_glob}')").fetchone()[0]
    span = con.execute(
        f"SELECT min(date), max(date) FROM read_parquet('{raw_glob}')"
    ).fetchone()
    print(f"  [info] failures={failures:,}  date range={span[0]} .. {span[1]}")

    stats = con.execute(
        f"""
        SELECT avg({BENCH_SMART_RAW}), max({BENCH_SMART_RAW}),
               avg(CASE WHEN {BENCH_SMART_RAW} > 0 THEN 1.0 ELSE 0.0 END)
        FROM read_parquet('{raw_glob}')
        """
    ).fetchone()
    check("benchmark counter is sane", 0 <= stats[0] < 1000, f"avg={stats[0]:.3f} max={stats[1]}")
    print(f"  [info] {BENCH_SMART_RAW}: avg={stats[0]:.4f} max={stats[1]} nonzero={stats[2]:.2%}")

    summary = {
        "rows": rows,
        "drives": drives,
        "models_rows": model_rows,
        "failures": failures,
        "date_min": str(span[0]),
        "date_max": str(span[1]),
        "smart5_avg": float(stats[0]),
        "smart5_max": int(stats[1]),
        "checks_failed": FAILURES,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "validation.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if FAILURES:
        print(f"\nvalidation FAILED ({len(FAILURES)} check(s))", flush=True)
        return 1
    print("\nvalidation OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
