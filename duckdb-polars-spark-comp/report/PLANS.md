# What the optimisers actually did

Captured evidence for the claims the benchmark report makes about physical
plans. All three were taken against the same 5,000,000-row dataset that the
committed results were produced from.

This matters because the article's query projects every column
(`r.* EXCLUDE (model)`), which *looks* like a wide-scan test. It is not: all
three engines apply column pruning and read three columns. The headline
difference is the join — DuckDB and Polars execute it, Spark removes it.

## Summary

| Engine | Columns read from `raw_data` | `LEFT JOIN models` executed | Mechanism |
| --- | --- | --- | --- |
| DuckDB 1.5.5 | 3 of 193 | **yes** (`HASH_JOIN`, `Join Type: LEFT`) | projection pushdown into `READ_PARQUET` |
| Polars 1.44.1 | 3 of 193 | **yes** (`LEFT JOIN ... ON serial_number`) | lazy projection pushdown, reported as `PROJECT 3/193 COLUMNS` |
| Spark 3.5.3 | 3 of 193 | **no — eliminated** | Catalyst column pruning removed the join and the `models` scan entirely |

Consequence for interpretation: the `full` and `pruned` variants differ far less
than their names suggest, and Spark's advantage is partly that it never does the
join. That is a real property of the engine, not a benchmark bug — the article's
PySpark ran the same shape of query on Databricks and would have been subject to
the same rule.

---

## DuckDB

`EXPLAIN` on `windowed_join_full.sql`. The join survives, and the fact-table scan
is narrowed to three columns (excerpt; box borders trimmed):

```text
HASH_JOIN
  Join Type: LEFT
  Conditions: serial_number = serial_number
  ├─ PROJECTION  (serial_number, row_num, previous_smart_5_raw, rolling_avg_smart_5_raw)
  │   └─ WINDOW
  │        Projections:
  │          ROW_NUMBER() OVER (PARTITION BY serial_number ORDER BY date ASC NULLS LAST)
  │          LAG(smart_5_raw, 1) OVER (PARTITION BY serial_number ORDER BY date ASC NULLS LAST)
  │          avg(smart_5_raw) OVER (PARTITION BY serial_number ORDER BY date ASC NULLS LAST
  │                                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
  │        └─ READ_PARQUET
  │             Function: READ_PARQUET
  │             Projections: date, serial_number, smart_5_raw     <-- 3 of 193
  │             ~5,029,936 rows
  └─ HASH_GROUP_BY  (Groups: serial_number)
       └─ READ_PARQUET
            Projections: serial_number
            ~7,676 rows
```

## Polars

`LazyFrame.explain()` on the equivalent lazy pipeline:

```text
SELECT [len().alias("row_count"), col("row_num").sum().alias("sum_row_num"), ...]
  simple π 3/3 ["row_num", ... 2 other columns]
    LEFT JOIN:
    LEFT PLAN ON: [col("serial_number")]
      simple π 4/4 ["row_num", ... 3 other columns]
         WITH_COLUMNS:
         [col("date").cum_count().cast(Int64).over([col("serial_number")]).alias("row_num"),
          col("smart_5_raw").shift([dyn int: 1]).over([col("serial_number")]).alias("previous_smart_5_raw"),
          col("smart_5_raw").rolling_mean().over([col("serial_number")]).alias("rolling_avg_smart_5_raw")]
          SORT BY [col("serial_number"), col("date")]
            Parquet SCAN [/data/raw_data/part-00000.parquet, ... 15 other sources]
            PROJECT 3/193 COLUMNS                                <-- 3 of 193
            ESTIMATED ROWS: 5000000
    RIGHT PLAN ON: [col("serial_number")]
      UNIQUE[maintain_order: false, keep_strategy: Any] BY Some(["serial_number"])
        Parquet SCAN [/data/models/part-00000.parquet]
        PROJECT 1/2 COLUMNS
        ESTIMATED ROWS: 7676
    END LEFT JOIN
```

## Spark

`spark_benchmark.py --explain` (Catalyst `explain("formatted")`). Note the
`Scan parquet` output schema and the absence of any second scan for `models`:

```text
== Physical Plan ==
AdaptiveSparkPlan (9)
+- HashAggregate (8)
   +- Exchange (7)
      +- HashAggregate (6)
         +- Project (5)
            +- Window (4)
               +- Sort (3)
                  +- Exchange (2)
                     +- Scan parquet  (1)

(1) Scan parquet
Output [3]: [date#0, serial_number#1, smart_5_raw#20L]
Batched: true
Location: InMemoryFileIndex [file:/data/raw_data]
ReadSchema: struct<date:date,serial_number:string,smart_5_raw:bigint>   <-- 3 of 193

(2) Exchange
Arguments: hashpartitioning(serial_number#1, 200), ENSURE_REQUIREMENTS

(3) Sort
Arguments: [serial_number#1 ASC NULLS FIRST, date#0 ASC NULLS FIRST], false, 0

(4) Window
Arguments: [row_number() windowspecdefinition(serial_number#1, date#0 ASC NULLS FIRST,
             specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())),
            lag(smart_5_raw#20L, -1, null) windowspecdefinition(serial_number#1, date#0 ASC NULLS FIRST,
             specifiedwindowframe(RowFrame, -1, -1)),
            avg(smart_5_raw#20L) windowspecdefinition(serial_number#1, date#0 ASC NULLS FIRST,
             specifiedwindowframe(RowFrame, -29, currentrow$()))]
```

There is no `Join` node and no scan of `/data/models`: because the aggregate
never references the joined `model` column, Catalyst's column-pruning rule
concluded the `left join` could not affect the result and dropped it.

## How to reproduce

```bash
# DuckDB
docker compose run --rm bench python -c '
import duckdb; con = duckdb.connect()
sql = open("/work/benchmarks/sql/windowed_join_full.sql").read()
sql = sql.replace("{RAW}", "/data/raw_data/*.parquet").replace("{MODELS}", "/data/models/*.parquet")
print(con.execute("EXPLAIN " + sql).fetchall()[0][1])'

# Spark
docker compose run --rm spark-client bash /work/docker/spark/submit.sh full --explain
```
