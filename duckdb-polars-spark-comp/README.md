# DuckDB vs Polars vs Spark — reproduced with generated data on Docker

This project re-implements the benchmark from
`duckdb-polars-spark-performance-comparision.pdf` ("DuckDB vs Polars vs Spark on
Databricks Serverless with Joins and Window Functions") with the two changes
requested:

1. **Databricks Serverless is replaced by Docker.** PySpark runs on a
   self-hosted Spark standalone cluster (one master, two workers) built from the
   official `apache/spark:3.5.3` image. DuckDB and Polars run in a sibling
   container on the same host, reading the same files.
2. **The Backblaze dataset is generated instead of downloaded**, at
   **5,000,000 rows** rather than the article's 285,339,435.

The workload itself is unchanged: three window expressions partitioned by
`serial_number` and ordered by `date`, a left join onto a small per-drive
dimension table, and four aggregates that double as a correctness fingerprint.

---

## What maps to what

The article's pipeline was tied to Databricks: Unity Catalog, Delta tables,
Volumes, `delta_scan()` on S3. Those pieces have direct local equivalents.

| Article | This project |
| --- | --- |
| Databricks Serverless compute | `apache/spark:3.5.3` standalone cluster in Docker Compose |
| Backblaze CSV download into a Unity Catalog Volume | `src/generate_data.py`, written to a Docker volume as parquet |
| `df.write.saveAsTable('raw_data')` (Delta) | `data/raw_data/*.parquet` |
| `df.write.saveAsTable('models')` (Delta) | `data/models/*.parquet` |
| DuckDB `unity_catalog` extension + `ATTACH` | `read_parquet()`, plus the article's eventual `delta_scan()` retry |
| `delta_scan('s3://...')` | `read_parquet()` on the local parquet files |
| `pl.Catalog(...).scan_table(...)` | `pl.scan_parquet(...)` |
| 285,339,435 rows, ~96 SMART ids | 5,000,000 rows, the same 91 SMART ids (193 columns) |

`smart_5_raw` is still the column the benchmark computes over, because that is
what the article used.

---

## Layout

```
.
├── docker-compose.yml            Spark master + 2 workers + one-shot client/bench
├── run.sh                        single entry point (see `./run.sh help`)
├── Makefile                      thin wrapper over run.sh
├── src/
│   ├── config.py                 schema, SMART ids, paths, workload constants
│   ├── generate_data.py          synthetic Backblaze-shaped data -> parquet
│   └── validate_data.py          structural checks before benchmarking
├── benchmarks/
│   ├── common.py                 result format, timing, SQL loading
│   ├── duckdb_benchmark.py
│   ├── polars_benchmark.py
│   ├── spark_benchmark.py
│   ├── build_report.py           results/*.json -> report/REPORT.md
│   └── sql/
│       ├── windowed_join_full.sql     the article's query, verbatim
│       └── windowed_join_pruned.sql   same semantics, 3-column scan
├── docker/
│   ├── bench/{Dockerfile,requirements.txt}
│   └── spark/submit.sh           spark-submit wrapper + cluster tuning
├── results/                      one JSON per engine run (gitignored)
└── report/
    ├── REPORT.md                 generated comparison
    └── PLANS.md                  captured physical plans (hand-written)
```

The generated parquet does **not** live in the repo: it is written to the Docker
named volume `dpsk-bench_bench-data`, mounted at `/data` in every container, so
all three engines read it at native volume speed rather than through a Windows
bind mount. `./run.sh clean` removes it.

---

## Quickstart

Requirements: Docker with Compose v2, and ~5 GB of free disk (a bit more if you
raise `BENCH_ROWS`). Nothing else — Python and Java are only needed *inside* the
containers.

`run.sh` and `docker/spark/submit.sh` are bash scripts. If you launch either with
a POSIX shell — `sh run.sh ...`, which is dash on Debian/Ubuntu/WSL — it detects
this and re-execs itself under `bash`, so the invocation style does not matter.

```bash
./run.sh all        # build, start the cluster, generate 5M rows, benchmark, report
```

That is the whole thing. After the first image build it takes a couple of
minutes: ~15 s to generate 5M rows, then a few seconds per engine per variant.

Individual stages, if you want to watch each one:

```bash
./run.sh up         # build images, start Spark master + 2 workers, wait for registration
./run.sh data       # generate 5,000,000 rows and validate them
./run.sh bench      # DuckDB, Polars and Spark for both scan variants
./run.sh report     # render report/REPORT.md
./run.sh down       # stop containers (keeps the generated data)
./run.sh clean      # stop containers and delete the data volume
./run.sh smoke      # 200,000-row end-to-end check -> report-smoke/ (main results untouched)
```

Useful extras:

```bash
./run.sh status          # containers + data volume size
./run.sh shell-bench     # bash inside the DuckDB/Polars container
./run.sh shell-spark     # bash inside the Spark driver container
```

### Changing the size of the run

Everything is environment-driven; no file edits needed.

```bash
BENCH_ROWS=20000000 ./run.sh data bench report
BENCH_SEED=7 ./run.sh data bench report          # different dataset, same shape
BENCH_VARIANTS=full ./run.sh bench               # skip the pruned variant
BENCH_RUNS=1 BENCH_WARMUP=0 ./run.sh bench       # the article's one-shot style
BENCH_RUNS=5 BENCH_WARMUP=2 ./run.sh bench       # steadier numbers
BENCH_WITH_POLARS_STREAMING=1 ./run.sh bench     # also try collect(engine="streaming")
BENCH_DATA_LIMIT=24GB BENCH_CPUS=16 ./run.sh bench
SPARK_EXECUTOR_MEMORY=3g SPARK_EXECUTOR_CORES=3 ./run.sh spark
```

---

## The workload

Every engine runs the same logical plan. For DuckDB it is literally the
article's SQL, kept in `benchmarks/sql/windowed_join_full.sql` so it can be read
next to the PDF:

```sql
WITH models_deduped AS (
    SELECT serial_number, ANY_VALUE(model) AS model
    FROM read_parquet('{MODELS}') GROUP BY serial_number
),
windowed AS (
    SELECT
        r.* EXCLUDE (model),
        ROW_NUMBER() OVER (PARTITION BY serial_number ORDER BY date) AS row_num,
        LAG(smart_5_raw, 1) OVER (PARTITION BY serial_number ORDER BY date)
            AS previous_smart_5_raw,
        AVG(smart_5_raw) OVER (PARTITION BY serial_number ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_avg_smart_5_raw
    FROM read_parquet('{RAW}') r
)
SELECT COUNT(*) AS row_count, SUM(row_num) AS sum_row_num,
       SUM(previous_smart_5_raw) AS sum_previous,
       SUM(rolling_avg_smart_5_raw) AS sum_rolling_avg
FROM windowed w LEFT JOIN models_deduped m USING (serial_number);
```

Polars and Spark express the same thing in their own APIs — `shift(1).over()`
and `rolling_mean(window_size=30, min_samples=1).over()` for Polars,
`Window.partitionBy(...).orderBy(...).rowsBetween(-29, 0)` for Spark.

### Two scan variants

| Variant | What it asks for | Why it exists |
| --- | --- | --- |
| `full` | `r.* EXCLUDE (model)` over all 193 columns | the article's actual query; end-to-end cost |
| `pruned` | only `serial_number`, `date`, `smart_5_raw` | the article's "give Polars its best chance" rewrite |

Both are kept because the difference between them is itself a finding: all
three engines prune the wide projection down to the same three columns, so the
two variants converge. `report/PLANS.md` has the captured physical plans.

---

## Generated data

The generator reproduces the *shape* of the Backblaze dataset without
reproducing a single real observation. Per drive it samples a model, capacity,
datacenter, cluster/vault/pod, and a lifetime of 90–1199 consecutive days; each
drive contributes one row per day. ~2% of drives eventually fail, and a failing
drive's `smart_5_raw` climbs toward the end of its life, which keeps the rolling
average non-trivial.

Schema is the article's: 11 base columns plus `smart_<id>_normalized` (int32)
and `smart_<id>_raw` (int64) for each of the 91 SMART ids in `src/config.py`.

`src/validate_data.py` refuses to let a broken dataset reach the benchmarks. It
asserts the exact row count, one `models` row per drive, no NULL keys, in-range
SMART counters, and that every drive is a contiguous daily series
(`date_diff(max, min) + 1 == count(*)`). Chunking bugs in a generator are
otherwise invisible — this project's own first two bugs were caught exactly
there.

---

## Fairness notes

These are the details that decide whether the numbers mean anything:

- **Data lives on a Docker volume, not a bind mount.** A Windows bind mount
  measurably caps throughput at roughly 270 MB/s write / 415 MB/s read on this
  host versus 2+ GB/s on a volume. All three engines read `/data`, so all three
  get the same fast path.
- **Only the query is timed.** Each engine reports `elapsed_seconds` for the
  scan→window→join→aggregate section, matching how the article timed its
  snippets. Spark additionally reports `session_start_seconds` and
  `wall_seconds` so startup is visible but not conflated with the query.
- **Spark gets a real cluster.** Two workers × 6 cores, one 6 GiB executor per
  worker, AQE enabled. Driver runs in client mode in its own container.
- **Spark's shuffle-partition setting is not doing the work.** It is a real knob
  (`SPARK_SHUFFLE_PARTITIONS`, handed to the benchmark script rather than passed
  as a `--conf`, because `SparkSession.builder.config()` silently overrides
  `--conf`). Measured at 24, 48 and 200 partitions the query is unchanged within
  run-to-run noise, so the default is not a conservative setting that quietly
  flatters the other engines.
- **DuckDB gets a memory ceiling, not a free hand.** `--memory-limit 16GB` and
  `--threads 12`, so a spill shows up as slowness rather than as the host
  swapping.
- **Correctness gates the comparison.** All four aggregates are compared across
  engines with a 1e-9 relative tolerance before any speedup is reported. If they
  disagree the report says `MISMATCH` rather than quietly ranking timings.
- **Optimisers are allowed to be optimisers.** The engines do not all execute
  the same physical plan, and that is reported rather than normalised away:
  DuckDB and Polars run the `left join`, while Spark's Catalyst prunes it out
  because the joined column is never used. See `report/PLANS.md`.

### Known limitations

- Five timed runs after two untimed warmups is the default
  (`BENCH_RUNS=5 BENCH_WARMUP=2`), and `elapsed_seconds` is the best of the
  timed runs. Set `BENCH_RUNS=1 BENCH_WARMUP=0` to reproduce the article's
  single-shot style — but at these sizes a single shot is mostly noise, and
  Spark's first timed run is still JIT-cold even after one warmup.
- Spark pays per-job overhead that DuckDB and Polars do not. On a small dataset
  that overhead dominates (at 200k rows Spark looks ~100× slower); it is only
  meaningful at a size where compute dominates startup.
- The generated fleet (~7.8k drives) is smaller than Backblaze's real ~450k,
  because 5M rows divided by ~640 rows-per-drive is a small fleet by
  construction. The join is therefore a genuine broadcast join in all three
  engines.

---

## Results

Committed run: 5,000,000 rows × 193 columns, seed `20251007`, 7,676 drives,
446 MiB of zstd parquet. Five timed runs after two warmups; best run reported.

| Engine | `full` (s) | `pruned` (s) | Configuration |
| --- | ---: | ---: | --- |
| DuckDB 1.5.5 | 0.15 | 0.15 | 12 threads, 16 GB limit |
| Polars 1.44.1 | 0.32 | 0.30 | in-memory engine |
| Spark 3.5.3 | 0.78 | 0.78 | 2 executors × 6 cores, 6 GiB each |

This table is a snapshot of the committed run; `report/REPORT.md` is the
authoritative version and lists every individual run plus its spread.

All four aggregates agree across all three engines, on both variants (within a
1e-9 relative tolerance — `sum_rolling_avg` is a float mean, so its trailing
digits differ between engines).

Polars' **streaming** engine — the article's final attempt, which OOM-killed for
them — completes here in ~0.32 s, indistinguishable from the in-memory engine.
Enable that extra comparison with `BENCH_WITH_POLARS_STREAMING=1`.

The headline reversal versus the article: on this dataset DuckDB and Polars beat
Spark on Docker by roughly 2.5–5×, where the article found PySpark on Databricks
Serverless fastest and DuckDB "way slower than expected". Three caveats matter
more than the ranking itself:

- **Spark pays ~3 s of session startup** on top of the query (JVM boot + cluster
  attach; ~13 s wall vs 0.78 s query). It is measured separately as
  `session_start_seconds` and deliberately left out of the table, because the
  article timed only its query snippets.
- **This is a compute-bound benchmark, not an I/O one.** The dataset is 446 MiB
  and stays resident in the page cache — a full six-column scan of all 5M rows
  completes in about 20 ms. The article's 285M rows across a networked lakehouse
  were I/O-bound; 5M local rows are not.
- **The engines did not run the same physical plan.** All three pruned the
  193-column projection down to three columns, and Spark additionally eliminated
  the `left join` entirely. See [`report/PLANS.md`](report/PLANS.md).

Full detail: [`report/REPORT.md`](report/REPORT.md), with a bar chart at
`report/timings.svg`. Regenerate any time with `./run.sh report`.

---

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `run.sh: 43: Syntax error: "(" unexpected` | Launched with a POSIX shell (`sh`, i.e. dash); `run.sh` uses bash arrays. It now re-execs under bash, so `./run.sh` / `bash run.sh` work — update if you still hit this. |
| `can't open file '/work/C:/...'` | Git Bash path rewriting. `run.sh` exports `MSYS_NO_PATHCONV=1`; if you invoke `docker compose` by hand, do the same. |
| Workers never register | `docker compose logs spark-master`. Give Docker Desktop enough memory (≥ 24 GB); the cluster alone reserves ~14 GB of worker capacity. |
| Spark tasks killed / `Container killed by YARN/OS` | Executor memory exceeds worker memory. Keep `SPARK_EXECUTOR_MEMORY` + `SPARK_EXECUTOR_OVERHEAD` under the worker's `SPARK_WORKER_MEMORY` (7g here). |
| Polars `MemoryError` / exit 137 | Expected on the `full` variant at large row counts — it is one of the article's own findings. Try `BENCH_VARIANTS=pruned`, or raise `BENCH_MEM_LIMIT`. |
| DuckDB spills to disk | Raise `BENCH_DATA_LIMIT`, or lower `BENCH_ROWS`. Spill files land in `/tmp/duckdb_spill`. |
| Results look stale | `results/` is append-only per engine; `./run.sh clean` removes the volume, but delete `results/` by hand if you want the report to start from a blank slate. |

