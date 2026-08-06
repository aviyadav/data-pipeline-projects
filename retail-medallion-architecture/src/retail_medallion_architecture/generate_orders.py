"""Generate synthetic order CSV files for the incoming data directory."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

COLUMNS = (
    "order_id",
    "ordered_at",
    "customer_id",
    "region",
    "amount",
    "currency",
    "status",
)
MAX_ROWS = 2_000
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "incoming"
STATE_FILE = ".next_order_id"


def _next_order_id(output_dir: Path) -> int:
    """Return an ID greater than every generated or existing order ID."""
    highest_id = 0
    state_path = output_dir / STATE_FILE

    if state_path.exists():
        try:
            highest_id = int(state_path.read_text(encoding="utf-8").strip()) - 1
        except ValueError:
            pass

    for csv_path in output_dir.glob("orders_*.csv"):
        try:
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                for row in csv.DictReader(csv_file):
                    try:
                        highest_id = max(highest_id, int(row["order_id"]))
                    except (KeyError, TypeError, ValueError):
                        continue
        except OSError:
            continue

    return highest_id + 1


def _output_path(output_dir: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d_%H%M%S_%f")
    return output_dir / f"orders_{timestamp}.csv"


def generate_orders(rows: int, output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Generate one order CSV and return its path."""
    if not 1 <= rows <= MAX_ROWS:
        raise ValueError(f"rows must be between 1 and {MAX_ROWS}")

    output_dir.mkdir(parents=True, exist_ok=True)
    first_order_id = _next_order_id(output_dir)
    output_path = _output_path(output_dir)
    now = datetime.now(UTC)

    with output_path.open("x", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=COLUMNS)
        writer.writeheader()

        for offset in range(rows):
            order_id = first_order_id + offset
            ordered_at = now - timedelta(seconds=random.randint(0, 86_400))
            amount = Decimal(random.randint(100, 100_000)) / Decimal(100)
            writer.writerow(
                {
                    "order_id": order_id,
                    "ordered_at": ordered_at.isoformat(timespec="seconds").replace(
                        "+00:00", "Z"
                    ),
                    "customer_id": f"C{random.randint(1, 9999):04d}",
                    "region": random.choice(("North", "South", "East", "West")),
                    "amount": f"{amount:.2f}",
                    "currency": "GBP",
                    "status": random.choices(
                        ("paid", "refunded"), weights=(9, 1), k=1
                    )[0],
                }
            )

    (output_dir / STATE_FILE).write_text(
        str(first_order_id + rows), encoding="utf-8"
    )
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        default=MAX_ROWS,
        help=f"number of records to generate (1-{MAX_ROWS}; default: {MAX_ROWS})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    try:
        output_path = generate_orders(args.rows, args.output_dir)
    except ValueError as error:
        parser.error(str(error))

    print(f"Generated {args.rows} orders in {output_path}")


if __name__ == "__main__":
    main()
