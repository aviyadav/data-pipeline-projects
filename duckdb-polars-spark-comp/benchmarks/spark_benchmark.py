#!/usr/bin/env python3
"""PySpark benchmark -- the article's PySpark job, on a local Docker cluster.

The article ran this on Databricks Serverless. Here the identical DataFrame
program is submitted to a Spark standalone cluster built from the
``apache/spark`` image (see docker-compose.yml). Reading parquet replaces
reading Delta, and that is the only change to the plan:

    row_number()  over (partition by serial_number order by date)
    lag(smart_5_raw, 1) over (the same window)
    avg(smart_5_raw) over (the same window, 29 preceding rows + current)
    left join the de-duplicated models dimension
    then count(*), sum(row_num), sum(previous_...), sum(rolling_avg_...)

    spark-submit benchmarks/spark_benchmark.py --variant full
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    BENCH_SMART_RAW,
    MODELS_DIR,
    RAW_DIR,
    ROLLING_WINDOW,
    Stopwatch,
    base_payload,
    load_sql,
    normalize_aggregates,
    write_result,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("full", "pruned"), default="full")
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER_URL"))
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--label", default=None)
    parser.add_argument("--explain", action="store_true", help="print the physical plan")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    from pyspark import __version__ as spark_version
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.window import Window

    label = args.label or ("spark" if args.variant == "full" else "spark-{0}".format(args.variant))
    wall = Stopwatch().start()

    sql = load_sql(args.variant)
    payload = base_payload(
        "spark",
        spark_version,
        args.variant,
        sql,
        settings={
            "master": args.master or "local[*]",
            "runs": args.runs,
            "warmup": args.warmup,
            "adaptive_query_execution": True,
        },
    )

    session_sw = Stopwatch().start()
    builder = (
        SparkSession.builder.appName("dpsk-bench-spark")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_path = str(RAW_DIR)
    models_path = str(MODELS_DIR)
    payload["settings"]["raw_path"] = raw_path
    payload["settings"]["models_path"] = models_path
    # Read the conf back instead of trusting what we asked for: the builder's
    # .config() wins over a spark-submit --conf, so a mismatch between
    # requested and effective means some other knob is being silently ignored.
    payload["settings"]["requested_shuffle_partitions"] = args.shuffle_partitions
    payload["settings"]["effective_shuffle_partitions"] = spark.conf.get(
        "spark.sql.shuffle.partitions", None
    )

    def build_aggregated():
        """Re-plan the whole job from the data source.

        Each timed run must be an independent execution. Reusing one DataFrame
        across actions lets Spark re-use already-materialised shuffle stages, so
        a "warm" second run finishes in ~0.04s instead of doing the work. DuckDB
        and Polars re-execute the query from the source on every run, so Spark
        is made to as well -- the per-run timing therefore also covers reading
        the parquet footers, which the other two engines pay inside their own
        timed section.
        """
        raw = spark.read.parquet(raw_path)
        models = (
            spark.read.parquet(models_path)
            .select("serial_number", "model")
            .dropDuplicates(["serial_number"])
        )

        if args.variant == "full":
            base = raw.drop("model")
        else:
            base = raw.select("serial_number", "date", BENCH_SMART_RAW)

        ordered_window = Window.partitionBy("serial_number").orderBy("date")
        rolling_window = (
            Window.partitionBy("serial_number")
            .orderBy("date")
            .rowsBetween(-(ROLLING_WINDOW - 1), 0)
        )

        plan = (
            base.withColumn("row_num", F.row_number().over(ordered_window))
            .withColumn(
                "previous_smart_5_raw",
                F.lag(BENCH_SMART_RAW, 1).over(ordered_window),
            )
            .withColumn(
                "rolling_avg_smart_5_raw",
                F.avg(BENCH_SMART_RAW).over(rolling_window),
            )
            .join(models, on="serial_number", how="left")
        )

        return plan.agg(
            F.count("*").alias("row_count"),
            F.sum("row_num").alias("sum_row_num"),
            F.sum("previous_smart_5_raw").alias("sum_previous"),
            F.sum("rolling_avg_smart_5_raw").alias("sum_rolling_avg"),
        )

    aggregated = build_aggregated()
    payload["session_start_seconds"] = round(session_sw.stop(), 3)

    # The master is usually supplied by spark-submit rather than --master, so
    # read the effective values back off the live context instead of guessing.
    context = spark.sparkContext
    payload["settings"]["master"] = context.master
    payload["settings"]["app_id"] = context.applicationId
    payload["settings"]["default_parallelism"] = context.defaultParallelism
    try:
        payload["settings"]["executor_count"] = context._jsc.sc().getExecutorMemoryStatus().size() - 1
    except Exception:
        pass

    if args.explain:
        aggregated.explain("formatted")

    print(
        "[spark] variant={0} master={1} partitions={2} executors={3}".format(
            args.variant,
            context.master,
            args.shuffle_partitions,
            payload["settings"].get("executor_count", "?"),
        ),
        flush=True,
    )

    timings = []
    result = None
    status, error = "ok", None
    try:
        for index in range(args.warmup + args.runs):
            sw = Stopwatch().start()
            rows = build_aggregated().collect()
            elapsed = sw.stop()
            result = normalize_aggregates(rows[0])
            timed = index >= args.warmup
            if timed:
                timings.append(round(elapsed, 3))
            print(
                "[spark] run {0}/{1} {2:.3f}s{3}".format(
                    index + 1, args.warmup + args.runs, elapsed, "" if timed else " (warmup)"
                ),
                flush=True,
            )
    except Exception as exc:
        status = "error"
        error = "{0}: {1}".format(type(exc).__name__, exc)
        print("[spark] FAILED: {0}".format(error), flush=True)

    payload.update(
        {
            "status": status,
            "error": error,
            "result": result,
            "runs_seconds": timings,
            "elapsed_seconds": min(timings) if timings else None,
            "wall_seconds": round(wall.stop(), 3),
        }
    )
    path = write_result(label, payload)
    print("[spark] status={0} result={1}".format(status, result))
    print("[spark] wrote {0}".format(path))

    spark.stop()
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
