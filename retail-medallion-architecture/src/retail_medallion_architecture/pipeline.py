from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import duckdb

DATABASE = Path("warehouse.duckdb")

def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def initialise(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    connection.execute("CREATE SCHEMA IF NOT EXISTS silver")
    connection.execute("CREATE SCHEMA IF NOT EXISTS gold")

    connection.execute("""
        CREATE TABLE IF NOT EXISTS bronze.ingestion_batches (
            source_hash VARCHAR PRIMARY KEY,
            source_file VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS bronze.orders_raw (
            order_id VARCHAR,
            ordered_at VARCHAR,
            customer_id VARCHAR,
            region VARCHAR,
            amount VARCHAR,
            currency VARCHAR,
            status VARCHAR,
            source_file VARCHAR NOT NULL,
            source_hash VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        )
    """)

def ingest_bronze(connection: duckdb.DuckDBPyConnection, source: Path) -> bool:
    source = source.resolve()
    digest = file_hash(source)
    already_loaded = connection.execute(
        f"SELECT 1 FROM bronze.ingestion_batches WHERE source_hash = '{digest}'"
    ).fetchone()

    if already_loaded:
        print(f"Skipping {source.name}: this exact file has already been loaded")
        return False

    connection.begin()
    try:
        connection.execute(
            """
            INSERT INTO bronze.orders_raw
            SELECT
                order_id,
                ordered_at,
                customer_id,
                region,
                amount,
                currency,
                status,
                ?,
                ?,
                current_timestamp
            FROM read_csv(?, header=True, all_varchar=True)
            """,
            [source.name, digest, str(source)]
        )
        connection.execute(
            """
            INSERT INTO bronze.ingestion_batches (source_hash, source_file)
            VALUES (?, ?)
            """,
            [digest, source.name],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    print(f"Loaded {source.name} into bronze")
    return True


def build_silver(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TEMP VIEW typed_orders AS
        SELECT
            trim(order_id) AS order_id,
            try_cast(ordered_at AS TIMESTAMPTZ) AS ordered_at,
            trim(customer_id) AS customer_id,
            upper(trim(region)) AS region,
            try_cast(amount AS DECIMAL(18, 2)) AS amount,
            upper(trim(currency)) AS currency,
            lower(trim(status)) AS status,
            source_file,
            source_hash,
            ingested_at,
            row_number() OVER (
                PARTITION BY trim(order_id)
                ORDER BY ingested_at DESC, source_file DESC
            ) AS duplicate_rank
        FROM bronze.orders_raw
    """)
    valid = """
        order_id IS NOT NULL AND order_id <> ''
        AND ordered_at IS NOT NULL
        AND customer_id IS NOT NULL AND customer_id <> ''
        AND amount IS NOT NULL AND amount >= 0
        AND currency IN ('GBP', 'EUR', 'USD')
        AND status IN ('paid', 'refunded', 'cancelled')
        AND duplicate_rank = 1
    """
    connection.execute(f"""
        CREATE OR REPLACE TABLE silver.orders AS
        SELECT * EXCLUDE (duplicate_rank)
        FROM typed_orders
        WHERE {valid}
    """)
    connection.execute(f"""
        CREATE OR REPLACE TABLE silver.orders_quarantine AS
        SELECT
            * EXCLUDE (duplicate_rank),
            CASE
                WHEN duplicate_rank > 1 THEN 'duplicate order_id'
                WHEN ordered_at IS NULL THEN 'invalid ordered_at'
                WHEN amount IS NULL THEN 'invalid amount'
                WHEN amount < 0 THEN 'negative amount'
                WHEN currency NOT IN ('GBP', 'EUR', 'USD') THEN 'unsupported currency'
                WHEN status NOT IN ('paid', 'refunded', 'cancelled') THEN 'invalid status'
                ELSE 'missing required value'
            END AS rejection_reason
        FROM typed_orders
        WHERE NOT ({valid})
    """)

def build_gold(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TABLE gold.daily_sales_by_region AS
        SELECT
            cast(ordered_at AS DATE) AS order_date,
            region,
            currency,
            count(*) FILTER (WHERE status = 'paid') AS paid_orders,
            sum(amount) FILTER (WHERE status = 'paid') AS gross_sales,
            count(*) FILTER (WHERE status = 'refunded') AS refunded_orders,
            sum(amount) FILTER (WHERE status = 'refunded') AS refunded_value
        FROM silver.orders
        GROUP BY order_date, region, currency
        ORDER BY order_date, region, currency
    """)

def check_quality(connection: duckdb.DuckDBPyConnection) -> None:
    duplicate_count = connection.execute(
        "SELECT count(*) - count(DISTINCT order_id) FROM silver.orders"
    ).fetchone()[0]
    null_key_count = connection.execute(
        "SELECT count(*) FROM silver.orders WHERE order_id IS NULL"
    ).fetchone()[0]
    if duplicate_count or null_key_count:
        raise RuntimeError("Silver quality contract failed")

def print_query(connection: duckdb.DuckDBPyConnection, query: str) -> None:
    result = connection.execute(query)
    print(" | ".join(column[0] for column in result.description))
    for row in result.fetchall():
        print(" | ".join("NULL" if value is None else str(value) for value in row))

def run_pipeline(source: Path) -> None:
    with duckdb.connect(str(DATABASE)) as connection:
        initialise(connection)
        ingest_bronze(connection, source)
        build_silver(connection)
        check_quality(connection)
        build_gold(connection)
        print("\nGold output")
        print_query(connection, "SELECT * FROM gold.daily_sales_by_region")
        print("\nQuarantined records")
        print_query(
            connection,
            """SELECT order_id, ordered_at, amount, rejection_reason
            FROM silver.orders_quarantine""",
        )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: pipeline path/to/orders.csv")

    source = Path(sys.argv[1])
    if not source.is_file():
        raise SystemExit(f"Input file not found: {source}")

    run_pipeline(source)


if __name__ == "__main__":
    main()
