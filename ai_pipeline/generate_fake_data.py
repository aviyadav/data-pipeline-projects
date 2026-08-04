import json
import multiprocessing
import os
import random
from datetime import datetime, timedelta

from pipeline.settings import settings

USERS = list(range(1, 501))
COUNTRIES = ["US", "IN", "GB", "DE", "BR", "JP"]
EVENTS = ["purchase", "view", "click", "add_to_cart"]
BASE_TIME = datetime(2025, 1, 1)


def _generate_chunk(args: tuple[int, int, int]) -> tuple[int, int]:
    """Generate a chunk of events and write to a worker-specific file."""
    worker_id, chunk_size, start_offset = args

    # Seed each worker uniquely for independent random streams
    random.seed(os.urandom(8))

    output_path = settings.raw_dir / f"events_{worker_id:04d}.json"
    count = 0

    with open(output_path, "w") as f:
        for i in range(chunk_size):
            event = {
                "user_id": random.choice(USERS),
                "country": random.choice(COUNTRIES),
                "event": random.choice(EVENTS),
                "amount": (
                    round(random.uniform(5.0, 500.0), 2)
                    if random.random() > 0.3
                    else 0.0
                ),
                "timestamp": (
                    BASE_TIME + timedelta(seconds=(start_offset + i) * 60)
                ).isoformat() + "Z",
            }
            f.write(json.dumps(event) + "\n")
            count += 1

    return worker_id, count


def generate_events(num_events: int = 100_000):
    settings.setup_dirs()

    num_workers = min(multiprocessing.cpu_count(), 8)
    chunk_size = num_events // num_workers
    remainder = num_events % num_workers

    # Build work items: (worker_id, chunk_size, start_offset)
    work_items: list[tuple[int, int, int]] = []
    offset = 0
    for worker_id in range(num_workers):
        size = chunk_size + (1 if worker_id < remainder else 0)
        work_items.append((worker_id, size, offset))
        offset += size

    print(f"Generating {num_events:,} events across {num_workers} workers …")

    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.map(_generate_chunk, work_items)

    total = sum(count for _, count in results)
    print(f"✅ Generated {total:,} events across {num_workers} files in {settings.raw_dir}")


if __name__ == "__main__":
    generate_events()
