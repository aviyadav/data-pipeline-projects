#!/usr/bin/env bash
#
# Runs the Spark benchmark inside the `spark-client` container.
#
# Living in a file (rather than being passed as a one-liner through
# `docker compose run`) keeps the quoting sane and makes the cluster tuning
# visible and reviewable. All knobs come from the environment, so the same
# script works from run.sh, from CI, or interactively.
#
#   docker compose run --rm spark-client bash /work/docker/spark/submit.sh full
#

# `set -o pipefail` below is a bash feature; re-exec under bash so the script
# still works if a caller launches it with a POSIX shell such as dash.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

VARIANT="${1:-full}"
shift || true

: "${SPARK_MASTER:=spark://spark-master:7077}"
: "${SPARK_EXECUTOR_CORES:=6}"
: "${SPARK_EXECUTOR_MEMORY:=6g}"
: "${SPARK_EXECUTOR_OVERHEAD:=512m}"
: "${SPARK_SHUFFLE_PARTITIONS:=200}"

# In client deploy mode the driver runs here; workers must be able to dial back
# to this container, so advertise the container's own address rather than
# whatever DNS name the JVM would otherwise guess.
DRIVER_HOST="$(hostname -i | awk '{print $1}')"

echo "[submit] master=${SPARK_MASTER} driver=${DRIVER_HOST} variant=${VARIANT}"
echo "[submit] executor=${SPARK_EXECUTOR_MEMORY}/${SPARK_EXECUTOR_CORES}c overhead=${SPARK_EXECUTOR_OVERHEAD} shuffle_partitions=${SPARK_SHUFFLE_PARTITIONS}"

# spark.sql.shuffle.partitions is passed to the benchmark script, NOT as a
# --conf. The script sets it through SparkSession.builder.config(), which
# overrides --conf, so supplying it both ways silently ignored this knob. The
# script is the single source of truth; "$@" still lets a caller override it.
exec /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER}" \
    --conf "spark.driver.host=${DRIVER_HOST}" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
    --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
    --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_OVERHEAD}" \
    /work/benchmarks/spark_benchmark.py \
    --variant "${VARIANT}" \
    --shuffle-partitions "${SPARK_SHUFFLE_PARTITIONS}" \
    "$@"
