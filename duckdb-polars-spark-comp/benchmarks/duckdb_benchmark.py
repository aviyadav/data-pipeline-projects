#!/usr/bin/env python3
"""DuckDB benchmark -- same plan as the article, run on generated parquet.

The article's DuckDB attempt went through Unity Catalog and then through
``delta_scan()`` on S3. Here the data is local parquet, so the only change is
``read_parquet()``. Everything else (window functions, broadcast-style left
join, final four aggregates) is byte-for-byte the article's query.

    python benchmarks/duckdb_benchmark.py --variant full
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    Stopwatch,
    base_payload,
    load_sql,
    normalize_aggregates,
    peak_rss_mb,
    write_result,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("full", "pruned"), default="full")
    parser.add_argument("--threads", type=int, default=os.cpu_count())
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--runs", type=int, default=1, help="timed runs")
    parser.add_argument("--warmup", type=int, default=0, help="untimed runs first")
    parser.add_argument("--label", default=None, help="result file stem")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    label = args.label or ("duckdb" if args.variant == "full" else f"duckdb-{args.variant}")
    wall = Stopwatch().start()

    sql = load_sql(args.variant)
    payload = base_payload(
        "duckdb",
        duckdb.__version__,
        args.variant,
        sql,
        settings={
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "runs": args.runs,
            "warmup": args.warmup,
        },
    )

    print(f"[duckdb] variant={args.variant} threads={args.threads} limit={args.memory_limit}", flush=True)
    con = duckdb.connect()
    con.execute(f"SET threads TO {args.threads}")
    if args.memory_limit and args.memory_limit.lower() != "none":
        con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute("SET temp_directory = '/tmp/duckdb_spill'")

    timings = []
    result = None
    try:
        for index in range(args.warmup + args.runs):
            sw = Stopwatch().start()
            rows = con.execute(sql).fetchall()
            elapsed = sw.stop()
            result = normalize_aggregates(rows[0])
            timed = index >= args.warmup
            if timed:
                timings.append(round(elapsed, 3))
            print(
                f"[duckdb] run {index + 1}/{args.warmup + args.runs} "
                f"{elapsed:.3f}s{'' if timed else ' (warmup)'}",
                flush=True,
            )
        status, error = "ok", None
    except Exception as exc:  # pragma: no cover - depends on machine size
        status, error = "error", f"{type(exc).__name__}: {exc}"
        print(f"[duckdb] FAILED: {error}", flush=True)

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
    print(f"[duckdb] status={status} result={result}")
    print(f"[duckdb] wrote {path}")
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
