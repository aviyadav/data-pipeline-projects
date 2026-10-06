# Data Quality Architecture & Benchmarking Platform

This project implements the three-layer data quality framework based on the article *"Data Quality in Modern Data Platforms: Building Quality Into Your Architecture"* by Nidhin, alongside local benchmark workloads comparing **Apache Spark**, **Polars**, and **DuckDB**.

---

## Architecture Overview

### 1. Three-Layer Data Quality Architecture (PySpark)

The core data pipeline enforces quality gates / circuit breakers at each layer of the medallion architecture:

- **Bronze Layer (Source Quality)**:
  - Validates schema (detects extra or missing columns).
  - Checks row count boundaries (`min_row_count`, `max_row_count`).
  - Verifies non-nullable critical columns.
  - Halts the pipeline if source quality criteria are not met.

- **Silver Layer (Transformation Quality)**:
  - Cleans and normalizes columns (e.g. valid regions vs. `UNKNOWN`).
  - Performs deduplication checks on entity identifiers (`customer_id`).
  - Flags and counts invalid transformation records.

- **Gold Layer (Business Logic Quality)**:
  - Computes business aggregations and marketing segments (e.g., `premium`, `standard`, `basic`).
  - Enforces data contracts and business distribution rules (e.g., minimum order thresholds for premium users, distribution checks requiring premium ratio < 20%).
  - Gated write: data is only committed to `gold/customer_segments` if all business logic quality checks pass.

---

### 2. Local Query Engine Benchmarks (Polars & DuckDB)

The project includes standalone analytical benchmark scripts evaluating complex windowing and analytical query performance:
- Deduplication and group-by aggregation (`ANY_VALUE(model)`).
- Time-series window functions: row numbering, lagged values (`LAG(smart_5_raw, 1)`), and 30-day rolling averages (`rolling_mean`).
- Relational joins and aggregate summaries.
- Configured to run locally inside Docker using shared Parquet datasets mounted at `/opt/spark/work-dir/data/` (with automatic synthetic data generation).

---

## Directory Structure

```text
├── config/
│   └── customer_segments_contract.json  # Data contract definition for Gold layer
├── docker/
│   └── Dockerfile                       # Spark 3.5.0 + Python 3.8 + PySpark + DuckDB + Polars
├── docker-compose.yml                   # Spark Master & Worker cluster configuration
├── bronze/                              # Raw data landing zone (Parquet)
├── silver/                              # Cleansed & transformed data (Parquet)
├── gold/                                # Business-ready data (Parquet)
├── data/                                # Shared benchmark dataset (raw_data & models)
├── scripts/
│   ├── bronze_validator.py              # Bronze validation rules & engine
│   ├── silver_transformation.py         # Silver cleansing & transformation checks
│   ├── gold_validation.py               # Gold business logic rules & segmentation
│   ├── main_pipeline.py                 # End-to-end pipeline orchestrator
│   ├── polars/
│   │   └── main_polars.py               # Local Polars benchmark script
│   └── duckdb/
│       └── main_duckdb.py               # Local DuckDB benchmark script
└── tests/
    └── test_bronze_validation.py        # Bronze validation unit tests
```

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- WSL2 (if running on Windows)

### 1. Start the Environment

Build and launch the Spark standalone cluster in the background:

```bash
docker compose up -d
```

Check container status:
```bash
docker compose ps
```

- **Spark Master UI**: [http://localhost:8081](http://localhost:8081)
- **Spark Master RPC Port**: `7077`

---

## Running the Pipelines & Benchmarks

### 1. Data Quality Pipeline (PySpark)

Execute the end-to-end medallion pipeline:

```bash
docker exec spark-master spark-submit /opt/spark/work-dir/scripts/main_pipeline.py
```

*Note: The built-in sample data includes intentionally defective records to verify that quality gates halt processing when rules fail.*

### 2. Polars Benchmark

Run the local Polars benchmark (rolling averages, lag, window functions, and joins):

```bash
docker exec spark-master python3 /opt/spark/work-dir/scripts/polars/main_polars.py
```

### 3. DuckDB Benchmark

Run the equivalent SQL benchmark on DuckDB using native Parquet scans and views:

```bash
docker exec spark-master python3 /opt/spark/work-dir/scripts/duckdb/main_duckdb.py
```

---

## Running Tests

To run the Bronze validation test suite:

```bash
docker exec spark-master python3 /opt/spark/work-dir/tests/test_bronze_validation.py
```

---

## Stopping the Services

To shut down the cluster and clean up networks:

```bash
docker compose down
```