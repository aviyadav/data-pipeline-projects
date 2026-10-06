#!/usr/bin/env bash
#
# End-to-end driver for the DuckDB / Polars / Spark-on-Docker benchmark.
#
#   ./run.sh all        # build, start cluster, generate data, benchmark, report
#   ./run.sh smoke      # 200k-row end-to-end check of the whole pipeline
#   ./run.sh up|down|clean|data|bench|report|status|shell-bench|shell-spark
#
# Environment knobs (all optional):
#   BENCH_ROWS                  fact-table rows           (default 5000000)
#   BENCH_SEED                  generator seed            (default 20251007)
#   BENCH_VARIANTS              "full pruned" or subset   (default "full pruned")
#   BENCH_DATA_LIMIT            DuckDB memory limit       (default 16GB)
#   BENCH_CPUS                  threads for the bench box (default 12)
#   BENCH_RUNS                  timed runs per engine    (default 5)
#   BENCH_WARMUP                untimed runs first       (default 2)
#   BENCH_WITH_POLARS_STREAMING 1 to also run collect(engine="streaming")
#   SPARK_EXECUTOR_MEMORY/CORES/... passed through to docker/spark/submit.sh
#

# Bash-only features (arrays, [[ ]], (( ))) below mean this cannot run under a
# POSIX shell such as dash, where `sh run.sh` fails at the first array with
# `Syntax error: "(" unexpected`. Re-exec under bash so the invocation style
# does not matter. Placed before `set -euo pipefail` because dash's `set` may not
# support pipefail either.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# Git Bash on Windows rewrites container-absolute paths (/work/...) into
# Windows paths. Every docker invocation below depends on that not happening.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")"

ROWS="${BENCH_ROWS:-5000000}"
SEED="${BENCH_SEED:-20251007}"
VARIANTS="${BENCH_VARIANTS:-full pruned}"
DATA_LIMIT="${BENCH_DATA_LIMIT:-16GB}"
BENCH_CPUS="${BENCH_CPUS:-12}"
# Single-shot timings at sub-second scale are mostly noise, and Spark's first
# timed run is still JIT-cold even after one warmup -- that alone moved its
# reported best-of-3 by ~50%. Five timed runs after two untimed warmups is the
# default; BENCH_RUNS=1 BENCH_WARMUP=0 reproduces the article's one-shot style.
RUNS="${BENCH_RUNS:-5}"
WARMUP="${BENCH_WARMUP:-2}"
# Container paths for outputs. `smoke` points these at *-smoke so a quick check
# can never overwrite a real run's results or report.
RESULTS_PATH="${BENCH_RESULTS_PATH:-/work/results}"
REPORT_PATH="${BENCH_REPORT_PATH:-/work/report}"
SPARK_WORKERS=(spark-master spark-worker-1 spark-worker-2)

COMPOSE=(docker compose)

log()  { printf '\n\033[1m[run] %s\033[0m\n' "$*"; }
warn() { printf '\033[33m[run] %s\033[0m\n' "$*" >&2; }

usage() {
    sed -n '3,19p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

# ---------------------------------------------------------------- cluster ---

spark_alive_workers() {
    # Ask the master what it actually thinks. Counting "Registering worker"
    # lines in the log is unreliable: `docker compose logs` replays the whole
    # history, so a restarted worker can look registered because of a stale
    # line. The master UI's JSON is authoritative and unambiguous.
    local json
    if json="$(curl -fsS --max-time 3 "${SPARK_MASTER_UI:-http://localhost:8080}/json/" 2>/dev/null)"; then
        printf '%s' "${json}" | grep -c '"state" : "ALIVE"'
        return 0
    fi
    # Fallback when the master UI is unreachable (no curl, or the port is not
    # published to the host): the old log heuristic, best effort only.
    printf '%s' "$("${COMPOSE[@]}" logs spark-master 2>/dev/null | grep -c 'Registering worker' || true)"
}

wait_for_workers() {
    log "waiting for Spark workers to register"
    local deadline=$((SECONDS + 180)) alive=0
    while (( SECONDS < deadline )); do
        alive="$(spark_alive_workers | tr -dc '0-9')"
        if [[ "${alive:-0}" -ge 2 ]]; then
            log "${alive} workers ALIVE on the master"
            return 0
        fi
        sleep 3
    done
    warn "timed out waiting for workers; check 'docker compose logs spark-master'"
    return 1
}

cmd_up() {
    log "building images"
    "${COMPOSE[@]}" build bench
    log "starting Spark master + 2 workers"
    "${COMPOSE[@]}" up -d "${SPARK_WORKERS[@]}"
    wait_for_workers
}

cmd_down() {
    log "stopping containers (data volume is kept)"
    "${COMPOSE[@]}" down --remove-orphans
}

cmd_clean() {
    log "stopping containers and deleting the generated dataset"
    "${COMPOSE[@]}" down --remove-orphans --volumes
    rm -rf results report/timings.svg
}

cmd_status() {
    "${COMPOSE[@]}" ps
    log "data volume"
    "${COMPOSE[@]}" run --rm bench sh -c \
        'du -sh /data 2>/dev/null; find /data -name "*.parquet" | wc -l' 2>/dev/null || true
}

# ------------------------------------------------------------------- data ---

cmd_data() {
    log "generating ${ROWS} rows (seed ${SEED})"
    "${COMPOSE[@]}" run --rm -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" bench \
        python /work/src/generate_data.py \
        --rows "${ROWS}" --seed "${SEED}" --overwrite
    log "validating dataset"
    "${COMPOSE[@]}" run --rm -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" bench \
        python /work/src/validate_data.py --rows "${ROWS}"
}

# --------------------------------------------------------------- engines ----

run_duckdb() {
    local variant="$1"
    log "DuckDB ($variant)"
    "${COMPOSE[@]}" run --rm -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" bench \
        python /work/benchmarks/duckdb_benchmark.py \
        --variant "${variant}" --threads "${BENCH_CPUS}" --memory-limit "${DATA_LIMIT}" \
        --runs "${RUNS}" --warmup "${WARMUP}" \
        || warn "duckdb ($variant) exited non-zero; result file still records the outcome"
}

run_polars() {
    local variant="$1"
    log "Polars ($variant)"
    "${COMPOSE[@]}" run --rm -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" bench \
        python /work/benchmarks/polars_benchmark.py \
        --variant "${variant}" --threads "${BENCH_CPUS}" \
        --runs "${RUNS}" --warmup "${WARMUP}" \
        || warn "polars ($variant) exited non-zero; result file still records the outcome"

    if [[ "${BENCH_WITH_POLARS_STREAMING:-0}" == "1" ]]; then
        # The label must include the variant. Otherwise the pruned streaming run
        # overwrites the full streaming run's result file, silently.
        local stream_label="polars-streaming"
        [[ "${variant}" != "full" ]] && stream_label="polars-streaming-${variant}"
        log "Polars ($variant, streaming engine)"
        "${COMPOSE[@]}" run --rm -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" bench \
            python /work/benchmarks/polars_benchmark.py \
            --variant "${variant}" --threads "${BENCH_CPUS}" \
            --engine streaming --label "${stream_label}" \
            || warn "polars ($variant, streaming) exited non-zero"
    fi
}

run_spark() {
    local variant="$1"
    log "Spark ($variant)"
    "${COMPOSE[@]}" run --rm -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" spark-client \
        bash /work/docker/spark/submit.sh "${variant}" \
        --runs "${RUNS}" --warmup "${WARMUP}" \
        || warn "spark ($variant) exited non-zero; result file still records the outcome"
}

cmd_bench() {
    local variant
    for variant in ${VARIANTS}; do
        run_duckdb "${variant}"
        run_polars "${variant}"
        run_spark "${variant}"
    done
}

cmd_report() {
    log "building report"
    "${COMPOSE[@]}" run --rm \
        -e "BENCH_RESULTS_DIR=${RESULTS_PATH}" \
        -e "BENCH_REPORT_DIR=${REPORT_PATH}" \
        bench python /work/benchmarks/build_report.py
    log "report -> ${REPORT_PATH#/work/}/REPORT.md"
}

# ------------------------------------------------------------------ modes ---

cmd_all() {
    cmd_up
    cmd_data
    cmd_bench
    cmd_report
    log "done; see report/REPORT.md"
}

cmd_smoke() {
    # Small enough to finish in a couple of minutes, large enough that every
    # engine really reads parquet, sorts, windows and joins. Outputs go to
    # separate paths so a smoke check never clobbers a committed run.
    ROWS=200000
    RESULTS_PATH=/work/results-smoke
    REPORT_PATH=/work/report-smoke
    cmd_up
    cmd_data
    cmd_bench
    cmd_report
    log "smoke run complete -> report-smoke/REPORT.md (results/ and report/ untouched)"
}

cmd_shell_bench() { "${COMPOSE[@]}" run --rm bench bash; }
cmd_shell_spark() { "${COMPOSE[@]}" run --rm spark-client bash; }

# Only dispatch when executed. `source run.sh` merely defines the helpers,
# which makes them testable without starting a cluster.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    case "${1:-}" in
        all)         cmd_all ;;
        smoke)       cmd_smoke ;;
        up)          cmd_up ;;
        down)        cmd_down ;;
        clean)       cmd_clean ;;
        status)      cmd_status ;;
        data)        cmd_data ;;
        bench)       cmd_bench ;;
        duckdb)      for v in ${VARIANTS}; do run_duckdb "$v"; done ;;
        polars)      for v in ${VARIANTS}; do run_polars "$v"; done ;;
        spark)       for v in ${VARIANTS}; do run_spark "$v"; done ;;
        report)      cmd_report ;;
        shell-bench) cmd_shell_bench ;;
        shell-spark) cmd_shell_spark ;;
        ""|-h|--help|help) usage 0 ;;
        *)           warn "unknown command: $1"; usage 1 ;;
    esac
fi
