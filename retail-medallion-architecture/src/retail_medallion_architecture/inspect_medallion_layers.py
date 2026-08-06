import duckdb
from tabulate import tabulate

def show_table(
    connection: duckdb.DuckDBPyConnection,
    title: str,
    query: str,
) -> None:
    result = connection.execute(query)
    headers = [column[0] for column in result.description]

    print(f"\n{title}")
    print(tabulate(result.fetchall(), headers=headers, tablefmt="psql"))

with duckdb.connect("warehouse.duckdb") as connection:
    show_table(
        connection,
        "BRONZE - Raw orders",
        """
        SELECT
            order_id,
            ordered_at,
            customer_id,
            region,
            amount,
            currency,
            status,
            source_file
        FROM bronze.orders_raw
        ORDER BY order_id
        """,
    )

    show_table(
        connection,
        "SILVER - Validated orders",
        """
        SELECT
            order_id,
            ordered_at,
            customer_id,
            region,
            amount,
            currency,
            status
        FROM silver.orders
        ORDER BY order_id
        """,
    )

    show_table(
        connection,
        "SILVER - Quarantined orders",
        """
        SELECT
            order_id,
            ordered_at,
            amount,
            rejection_reason
        FROM silver.orders_quarantine
        ORDER BY order_id
        """,
    )

    show_table(
        connection,
        "GOLD - Daily sales by region",
        """
        SELECT *
        FROM gold.daily_sales_by_region
        ORDER BY order_date, region
        """,
    )
