#!/usr/bin/env python3
"""Polars benchmark -- the article's lazy pipeline, run on generated parquet.

The article's Polars attempt used ``pl.Catalog`` against Unity Catalog and then
died. Here the same ``scan_table``-style lazy frame is built from local parquet
with ``pl.scan_parquet``; the window/join/aggregate plan is unchanged:

    row_num              = cumulative count within serial_number, ordered by date
    previous_smart_5_raw = smart_5_raw shifted by 1 within serial_number
    rolling_avg_...      = rolling mean of smart_5_raw, window 30, min_samples 1
    then a left join back onto the de-duplicated models dimension

``--engine streaming`` reproduces the article's final attempt
(``result.collect(engine="streaming")``).

    python benchmarks/polars_benchmark.py --variant full
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    BENCH_SMART_RAW,
    ROLLING_WINDOW,
    Stopwatch,
    base_payload,
    load_sql,
    models_glob,
    normalize_aggregates,
    peak_rss_mb,
    raw_glob,
    write_result,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("full", "pruned"), default="full")
    parser.add_argument("--engine", choices=("in-memory", "streaming", "auto"), default="in-memory")
    parser.add_argument("--threads", type=int, default=None, help="POLARS_MAX_THREADS")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--label", default=None)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.threads:
        os.environ["POLARS_MAX_THREADS"] = str(args.threads)

    import polars as pl

    label = args.label or ("polars" if args.variant == "full" else f"polars-{args.variant}")
    wall = Stopwatch().start()

    sql = load_sql(args.variant)
    payload = base_payload(
        "polars",
        pl.__version__,
        args.variant,
        sql,
        settings={
            "engine": args.engine,
            "threads": args.threads or "auto",
            "runs": args.runs,
            "warmup": args.warmup,
        },
    )

    raw = pl.scan_parquet(raw_glob())
    models = (
        pl.scan_parquet(models_glob())
        .select(["serial_number", "model"])
        .unique(subset=["serial_number"])
    )

    if args.variant == "full":
        scan = raw.drop("model").sort(["serial_number", "date"])
    else:
        scan = raw.select(["serial_number", "date", BENCH_SMART_RAW]).sort(
            ["serial_number", "date"]
        )

    windowed = scan.with_columns(
        [
            pl.col("date").cum_count().cast(pl.Int64).over("serial_number").alias("row_num"),
            pl.col(BENCH_SMART_RAW)
            .shift(1)
            .over("serial_number")
            .alias("previous_smart_5_raw"),
            pl.col(BENCH_SMART_RAW)
            .rolling_mean(window_size=ROLLING_WINDOW, min_samples=1)
            .over("serial_number")
            .alias("rolling_avg_smart_5_raw"),
        ]
    )

    plan = (
        windowed.join(models, on="serial_number", how="left")
        .select(
            [
                pl.len().alias("row_count"),
                pl.col("row_num").sum().alias("sum_row_num"),
                pl.col("previous_smart_5_raw").sum().alias("sum_previous"),
                pl.col("rolling_avg_smart_5_raw").sum().alias("sum_rolling_avg"),
            ]
        )
    )

    collect_kwargs = {} if args.engine == "in-memory" else {"engine": args.engine}

    print(f"[polars] variant={args.variant} engine={args.engine}", flush=True)
    timings = []
    result = None
    status, error = "ok", None
    try:
        for index in range(args.warmup + args.runs):
            sw = Stopwatch().start()
            frame = plan.collect(**collect_kwargs)
            elapsed = sw.stop()
            result = normalize_aggregates(frame.row(0))
            timed = index >= args.warmup
            if timed:
                timings.append(round(elapsed, 3))
            print(
                f"[polars] run {index + 1}/{args.warmup + args.runs} "
                f"{elapsed:.3f}s{'' if timed else ' (warmup)'}",
                flush=True,
            )
    except Exception as exc:
        status = "error"
        error = f"{type(exc).__name__}: {exc}"
        print(f"[polars] FAILED: {error}", flush=True)

    payload.update(
        {
            "status": status,
            "error": error,
            "result": result,
            "runs_seconds": timings,
            "elapsed_seconds": min(timings) if timings else None,
            "wall_seconds": round(wall.stop(), 3),
            "peak_rss_mb": peak_rss_mb(),
        }
    )
    path = write_result(label, payload)
    print(f"[polars] status={status} result={result}")
    print(f"[polars] wrote {path}")
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
