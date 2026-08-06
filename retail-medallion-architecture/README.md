# Retail Medallion Architecture

A Python project for experimenting with a retail-data medallion architecture.
It currently includes a synthetic order generator that writes CSV files to the
incoming data layer.

## Requirements

- Python 3.13 or newer
- [uv](https://docs.astral.sh/uv/)

## Set up the project

Install the project and all locked dependencies:

```shell
uv sync
```

To update the lock file after changing dependencies in `pyproject.toml`:

```shell
uv lock
```

## Run the project entry point

Run the main package entry point:

```shell
uv run retail-medallion-architecture
```

## Generate incoming order data

Generate the default 2,000 order records:

```shell
uv run generate-orders
```

Generate a specific number of records, from 1 through 2,000:

```shell
uv run generate-orders --rows 500
```

Show all generator options:

```shell
uv run generate-orders --help
```

Each invocation creates a new timestamped `orders_*.csv` file under
`data/incoming`. Generated files follow this schema:

```text
order_id,ordered_at,customer_id,region,amount,currency,status
```

The generator tracks the next available order ID in
`data/incoming/.next_order_id`. It also scans existing order CSV files before
generation, ensuring that generated `order_id` values do not overlap between
files. Keep this state file if files are moved out of the incoming directory and
IDs must remain unique in subsequent runs.

## Run the medallion pipeline

Process an incoming order CSV through the Bronze, Silver, and Gold layers:

```shell
uv run pipeline data/incoming/orders_2026-08-06_201751_434618.csv
```

The command requires exactly one argument: the path to an existing order CSV.
Replace the example path with the file you want to process. If the path is
missing or does not identify a file, the command exits with a usage or
file-not-found message.

The pipeline:

- loads the original CSV values into `bronze.orders_raw`;
- skips files whose content has already been ingested;
- validates, converts, and deduplicates records into `silver.orders`;
- stores rejected records in `silver.orders_quarantine`;
- aggregates valid sales into `gold.daily_sales_by_region`; and
- prints the Gold output and quarantined records.

Pipeline data is persisted in `warehouse.duckdb` in the project root. Running
the pipeline again with the same file skips Bronze ingestion based on its SHA-256
content hash, then rebuilds and prints the Silver and Gold results.

To process a newly generated file, run the generator first and pass the path it
prints to the pipeline command:

```shell
uv run generate-orders --rows 500
uv run pipeline data/incoming/orders_<timestamp>.csv
```

## Inspect the medallion layers

After running the pipeline, inspect the data persisted in `warehouse.duckdb`:

```shell
uv run inspect_medallion_layers
```

The command prints PostgreSQL-style tables for:

- raw orders in `bronze.orders_raw`;
- validated orders in `silver.orders`;
- rejected orders and their reasons in `silver.orders_quarantine`; and
- aggregated sales in `gold.daily_sales_by_region`.

Run the pipeline first so that the database and medallion tables exist.

## Run modules directly

The generator, pipeline, and layer inspector can also be run as Python modules:

```shell
uv run python -m retail_medallion_architecture.generate_orders --rows 500
uv run python -m retail_medallion_architecture.pipeline data/incoming/orders_2026-08-06_201751_434618.csv
uv run python -m retail_medallion_architecture.inspect_medallion_layers
```

## Check the source code

Compile all package modules to catch Python syntax errors:

```shell
uv run python -m compileall src
```
