# Python image used for data generation, validation and the in-process engines
# (DuckDB, Polars). Pinned so that two runs of the benchmark are comparable.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# procps gives us `ps` for ad-hoc memory checks while babysitting a run.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY docker/bench/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /work
