#!/usr/bin/env python3
"""Generate a synthetic Backblaze-style SMART dataset.

Replaces the article's "download a few years of Backblaze data" step. The
generated data is statistically *shaped* like the real thing (drive models,
capacity, datacenters, rare failures, mostly-zero SMART counters that drift up
as a drive approaches failure) but is entirely synthetic and reproducible from
``--seed``.

Two tables are written, matching the article:

``raw_data``   one row per (drive, day) observation -- the large fact table.
``models``     distinct (serial_number, model) -- the small join dimension.

Usage::

    python src/generate_data.py --rows 5000000
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (  # noqa: E402
    DEFAULT_ROWS,
    DEFAULT_SEED,
    RAW_COLUMNS,
    RAW_DIR,
    RESULTS_DIR,
    SMART_IDS,
)

# --------------------------------------------------------------------------
# Domain constants -- modelled on the real Backblaze fleet, not real devices.
# --------------------------------------------------------------------------

MODEL_SPECS: tuple[tuple[str, int], ...] = (
    ("ST4000DM000", 4_000_787_030_016),
    ("ST8000DM002", 8_001_563_222_016),
    ("ST12000NM0007", 12_000_138_625_024),
    ("ST12000NM001G", 12_000_138_625_024),
    ("ST14000NM001G", 14_000_519_643_136),
    ("ST16000NM001G", 16_000_900_661_248),
    ("ST16000NM000J", 16_000_900_661_248),
    ("ST18000NM000J", 18_000_207_937_536),
    ("ST2000DM001", 2_000_398_934_016),
    ("HGST HMS5C4040ALE640", 4_000_787_030_016),
    ("HGST HMS5C4040BLE640", 4_000_787_030_016),
    ("HGST HUH721212ALN604", 12_000_138_625_024),
    ("HGST HUH728080ALE600", 8_001_563_222_016),
    ("TOSHIBA MG04ACA400N", 4_000_787_030_016),
    ("TOSHIBA MG07ACA14TA", 14_000_519_643_136),
    ("TOSHIBA MG08ACA16TA", 16_000_900_661_248),
    ("WDC WD40EFRX", 4_000_787_030_016),
    ("WDC WD60EFRX", 6_001_175_126_016),
    ("WDC WUH721414ALE6L4", 14_000_519_643_136),
    ("WDC WUH722222ALE6L4", 22_000_969_973_760),
    ("DELLBOSS VD", 240_057_409_536),
    ("Seagate M3 Portable", 4_000_787_030_016),
)

DATACENTERS: tuple[str, ...] = ("sjc", "sac", "phx", "ams", "sto", "sin")
SERIAL_PREFIXES: tuple[str, ...] = ("ZFL", "ZAE", "ZA1", "ZCH", "ZJV", "ZLW")
SERIAL_ALPHABET = np.frombuffer(b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", dtype=np.uint8)

EPOCH_DAYS_2022_01_01 = int(
    (np.datetime64("2022-01-01") - np.datetime64("1970-01-01")).astype("int64")
)
START_SPREAD_DAYS = 730          # drives come online across a 2-year window
MIN_DRIVE_DAYS = 90
MAX_DRIVE_DAYS = 1200
DRIVE_FAILURE_RATE = 0.02        # ~2% of drives eventually fail
LEGACY_FORMAT_RATE = 0.15

N_CLUSTERS, N_VAULTS, N_PODS = 9, 96, 24
CLUSTER_IDS: tuple[str, ...] = tuple(f"cluster_{i}" for i in range(1, N_CLUSTERS + 1))
VAULT_IDS: tuple[str, ...] = tuple(f"vault_{i}" for i in range(1, N_VAULTS + 1))
POD_IDS: tuple[str, ...] = tuple(f"pod_{i}" for i in range(1, N_PODS + 1))


def build_schema() -> pa.Schema:
    """Arrow schema for the fact table (mirrors the article's StructType)."""
    fields = [
        pa.field("date", pa.date32()),
        pa.field("serial_number", pa.string()),
        pa.field("model", pa.string()),
        pa.field("capacity_bytes", pa.int64()),
        pa.field("failure", pa.int32()),
        pa.field("datacenter", pa.string()),
        pa.field("cluster_id", pa.string()),
        pa.field("vault_id", pa.string()),
        pa.field("pod_id", pa.string()),
        pa.field("pod_slot_num", pa.int32()),
        pa.field("is_legacy_format", pa.int32()),
    ]
    for smart_id in SMART_IDS:
        fields.append(pa.field(f"smart_{smart_id}_normalized", pa.int32()))
        fields.append(pa.field(f"smart_{smart_id}_raw", pa.int64()))
    return pa.schema(fields)


# --------------------------------------------------------------------------
# Drive-level population
# --------------------------------------------------------------------------


def make_serials(n: int, rng: np.random.Generator) -> list[str]:
    """Unique Backblaze-looking serial numbers."""
    seen: set[str] = set()
    out: list[str] = []
    while len(out) < n:
        indices = rng.integers(0, len(SERIAL_ALPHABET), size=(n, 6))
        chars = SERIAL_ALPHABET[indices]  # map indices -> actual ASCII bytes
        for row in chars:
            serial = str(rng.choice(SERIAL_PREFIXES)) + row.tobytes().decode("ascii")
            if serial not in seen:
                seen.add(serial)
                out.append(serial)
                if len(out) == n:
                    break
    return out


def plan_drives(n_rows: int, rng: np.random.Generator) -> np.ndarray:
    """Per-drive observation counts that sum *exactly* to ``n_rows``."""
    guess = int(n_rows / 600) + 1024
    days = rng.integers(MIN_DRIVE_DAYS, MAX_DRIVE_DAYS, size=guess).astype(np.int64)
    cumulative = np.cumsum(days)
    idx = int(np.searchsorted(cumulative, n_rows, side="left"))
    days = days[: idx + 1]
    overshoot = int(cumulative[idx] - n_rows)
    days[-1] -= overshoot
    if days[-1] < 1:  # pathological small target; fall back to a single drive
        days = np.array([n_rows], dtype=np.int64)
    assert int(days.sum()) == n_rows, "drive plan must sum to the requested row count"
    return days


class Fleet:
    """Per-drive attributes, sampled once and reused across every row."""

    def __init__(self, days: np.ndarray, rng: np.random.Generator) -> None:
        self.days = days
        self.n_drives = len(days)
        self.row_offsets = np.zeros(self.n_drives, dtype=np.int64)
        np.cumsum(days[:-1], out=self.row_offsets[1:])

        model_idx = rng.integers(0, len(MODEL_SPECS), size=self.n_drives)
        self.model_idx = model_idx
        self.model_names = [MODEL_SPECS[i][0] for i in model_idx]
        self.capacity = np.array([MODEL_SPECS[i][1] for i in model_idx], dtype=np.int64)

        self.serials = make_serials(self.n_drives, rng)
        # Categorical attributes are stored as small integer codes so each row
        # group can be built with a single dictionary lookup.
        self.datacenter_code = rng.integers(0, len(DATACENTERS), self.n_drives).astype(np.int32)
        self.cluster_code = rng.integers(0, N_CLUSTERS, self.n_drives).astype(np.int32)
        self.vault_code = rng.integers(0, N_VAULTS, self.n_drives).astype(np.int32)
        self.pod_code = rng.integers(0, N_PODS, self.n_drives).astype(np.int32)
        self.pod_slot_num = rng.integers(0, 60, size=self.n_drives).astype(np.int32)
        self.is_legacy = (rng.random(self.n_drives) < LEGACY_FORMAT_RATE).astype(np.int32)
        self.start_day = EPOCH_DAYS_2022_01_01 + rng.integers(
            0, START_SPREAD_DAYS, size=self.n_drives
        )
        self.will_fail = rng.random(self.n_drives) < DRIVE_FAILURE_RATE
        # Zero-inflation probability for the "noisy" SMART counters.
        self.smart_p_nonzero = rng.uniform(0.02, 0.25, size=len(SMART_IDS))


# --------------------------------------------------------------------------
# Row-group construction
# --------------------------------------------------------------------------


def _dict_strings(indices: np.ndarray, values: list[str]) -> pa.Array:
    """Fast string column via dictionary encoding (avoids per-row Python objects)."""
    arr = pa.DictionaryArray.from_arrays(
        pa.array(indices.astype(np.int32)), pa.array(values, type=pa.string())
    )
    return arr.cast(pa.string())


def build_group(
    fleet: Fleet,
    g_start: int,
    g_end: int,
    rng: np.random.Generator,
    schema: pa.Schema,
) -> pa.Table:
    """Materialise one contiguous block of drives as an Arrow table."""
    counts = fleet.days[g_start:g_end]
    n = int(counts.sum())

    drive = np.repeat(np.arange(g_start, g_end, dtype=np.int32), counts)
    # Day index must be relative to the drive's own lifetime, and the drive's
    # lifetime starts at a global row offset -- not at the start of this chunk.
    group_base = int(fleet.row_offsets[g_start])
    day_index = (
        np.arange(n, dtype=np.int64)
        + group_base
        - np.repeat(fleet.row_offsets[g_start:g_end], counts)
    )

    days_per_row = fleet.days[drive]
    failing_row = fleet.will_fail[drive]

    columns: dict[str, pa.Array] = {
        "date": pa.array((fleet.start_day[drive] + day_index).astype(np.int32), type=pa.date32()),
        "serial_number": _dict_strings(drive, fleet.serials),
        "model": _dict_strings(fleet.model_idx[drive], [m[0] for m in MODEL_SPECS]),
        "capacity_bytes": pa.array(fleet.capacity[drive], type=pa.int64()),
        "failure": pa.array(
            (failing_row & (day_index >= days_per_row - 1)).astype(np.int32), type=pa.int32()
        ),
        "datacenter": _dict_strings(fleet.datacenter_code[drive], list(DATACENTERS)),
        "cluster_id": _dict_strings(fleet.cluster_code[drive], list(CLUSTER_IDS)),
        "vault_id": _dict_strings(fleet.vault_code[drive], list(VAULT_IDS)),
        "pod_id": _dict_strings(fleet.pod_code[drive], list(POD_IDS)),
        "pod_slot_num": pa.array(fleet.pod_slot_num[drive], type=pa.int32()),
        "is_legacy_format": pa.array(fleet.is_legacy[drive], type=pa.int32()),
    }

    # ---- the one counter the benchmark computes over -----------------------
    smart_5_raw = np.zeros(n, dtype=np.int64)
    noise = rng.random(n) < 0.05
    if noise.any():
        smart_5_raw[noise] = rng.integers(1, 32, size=int(noise.sum()))
    if failing_row.any():
        d = day_index[failing_row].astype(np.float64)
        total = np.maximum(days_per_row[failing_row].astype(np.float64), 1.0)
        progress = np.clip(d / total, 0.0, 1.0)
        growth = np.clip((progress ** 1.6) * 20_000.0, 0.0, 65_535.0)
        smart_5_raw[failing_row] = growth.astype(np.int64) + rng.integers(
            0, 40, size=int(failing_row.sum())
        )

    def smart_pair(smart_id: int, p_nonzero: float) -> tuple[pa.Array, pa.Array]:
        if smart_id == 5:
            raw = smart_5_raw
            norm = np.where(raw > 0, np.minimum(100 + raw, 253), 100).astype(np.int32)
            return pa.array(norm, type=pa.int32()), pa.array(raw, type=pa.int64())
        hit = rng.random(n) < p_nonzero
        raw = np.where(hit, rng.integers(1, 1 << 24, size=n), 0).astype(np.int64)
        norm = np.where(raw > 0, np.minimum(100 + raw, 253), 100).astype(np.int32)
        return pa.array(norm, type=pa.int32()), pa.array(raw, type=pa.int64())

    for pos, smart_id in enumerate(SMART_IDS):
        norm_arr, raw_arr = smart_pair(smart_id, float(fleet.smart_p_nonzero[pos]))
        columns[f"smart_{smart_id}_normalized"] = norm_arr
        columns[f"smart_{smart_id}_raw"] = raw_arr

    table = pa.table({name: columns[name] for name in RAW_COLUMNS}, schema=schema)
    return table


def group_bounds(days: np.ndarray, chunk_rows: int) -> list[tuple[int, int]]:
    cumulative = np.cumsum(days)
    total = int(cumulative[-1])
    if total <= chunk_rows:
        return [(0, len(days))]
    cuts = np.arange(chunk_rows, total, chunk_rows, dtype=np.int64)
    idx = np.unique(np.searchsorted(cumulative, cuts, side="left"))
    edges = [0] + [int(i) for i in idx if 0 < int(i) < len(days)] + [len(days)]
    return list(zip(edges[:-1], edges[1:]))


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def positive_int(text: str) -> int:
    """argparse type that rejects <= 0.

    A zero or negative row count would otherwise produce an empty table and a
    manifest claiming success -- a silent failure that validation would happily
    pass. --chunk-rows / --rows-per-file must also be positive or group_bounds
    divides by zero.
    """
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {value}")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=positive_int, default=DEFAULT_ROWS, help="fact-table row count (>= 1)")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--out", type=Path, default=None, help="data dir (default: config DATA_DIR)")
    p.add_argument("--chunk-rows", type=positive_int, default=250_000, help="rows materialised per Arrow table")
    p.add_argument("--rows-per-file", type=positive_int, default=500_000, help="rows per output parquet file")
    p.add_argument("--compression", default="zstd")
    p.add_argument("--overwrite", action="store_true", help="delete existing data dir first")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = Path(args.out) if args.out else RAW_DIR.parent
    raw_dir = data_dir / "raw_data"
    models_dir = data_dir / "models"

    if args.overwrite:
        for path in (raw_dir, models_dir):
            if path.exists():
                shutil.rmtree(path)
    raw_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    for stale in list(raw_dir.glob("*.parquet")) + list(models_dir.glob("*.parquet")):
        stale.unlink()

    rng = np.random.default_rng(args.seed)
    schema = build_schema()

    started = time.perf_counter()
    days = plan_drives(args.rows, rng)
    fleet = Fleet(days, rng)
    planned = time.perf_counter()

    print(
        f"[gen] rows={args.rows:,} drives={fleet.n_drives:,} "
        f"columns={len(RAW_COLUMNS)} seed={args.seed}",
        flush=True,
    )

    bounds = group_bounds(fleet.days, args.chunk_rows)
    file_index = 0
    rows_in_file = 0
    writer: pq.ParquetWriter | None = None
    written_rows = 0
    files: list[Path] = []

    def open_writer() -> pq.ParquetWriter:
        path = raw_dir / f"part-{file_index:05d}.parquet"
        files.append(path)
        return pq.ParquetWriter(
            path, schema, compression=args.compression, use_dictionary=True
        )

    table_started = time.perf_counter()
    for g_start, g_end in bounds:
        table = build_group(fleet, g_start, g_end, rng, schema)
        if writer is None or rows_in_file + table.num_rows > args.rows_per_file:
            if writer is not None:
                writer.close()
                file_index += 1
            writer = open_writer()
            rows_in_file = 0
        writer.write_table(table)
        rows_in_file += table.num_rows
        written_rows += table.num_rows
        del table
        print(f"[gen] {written_rows:,}/{args.rows:,} rows", flush=True)

    if writer is not None:
        writer.close()

    # ---- the small join dimension -----------------------------------------
    models_table = pa.table(
        {
            "serial_number": pa.array(fleet.serials, type=pa.string()),
            "model": pa.array(fleet.model_names, type=pa.string()),
        }
    )
    models_path = models_dir / "part-00000.parquet"
    pq.write_table(models_table, models_path, compression=args.compression)

    elapsed = time.perf_counter() - started
    raw_bytes = sum(f.stat().st_size for f in raw_dir.glob("*.parquet"))
    models_bytes = models_path.stat().st_size

    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "rows": written_rows,
        "columns": len(RAW_COLUMNS),
        "drives": fleet.n_drives,
        "models_rows": models_table.num_rows,
        "seed": args.seed,
        "raw_files": len(files),
        "raw_bytes": raw_bytes,
        "models_bytes": models_bytes,
        "compression": args.compression,
        "plan_seconds": round(planned - started, 3),
        "write_seconds": round(time.perf_counter() - table_started, 3),
        "elapsed_seconds": round(elapsed, 3),
    }
    for target in (data_dir / "dataset.json", RESULTS_DIR / "dataset.json"):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        f"[gen] done in {elapsed:.1f}s -> {written_rows:,} rows in {len(files)} files "
        f"({raw_bytes / 2**20:.0f} MiB raw, {models_bytes / 2**10:.0f} KiB models)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
