"""Helpers shared by the DuckDB, Polars and Spark benchmarks.

Keeping result serialisation and timing in one place means all three engines
emit byte-compatible JSON, which is what makes the comparison defensible: the
report only ever compares fields that every engine produced the same way.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

BENCH_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BENCH_DIR.parent

sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config import (  # noqa: E402
    BENCH_SMART_RAW,
    DATA_DIR,
    MODELS_DIR,
    RAW_DIR,
    RESULTS_DIR,
    ROLLING_WINDOW,
    models_glob,
    raw_glob,
)

SQL_DIR = BENCH_DIR / "sql"

AGGREGATES = ("row_count", "sum_row_num", "sum_previous", "sum_rolling_avg")


def load_sql(variant: str, raw: Optional[str] = None, models: Optional[str] = None) -> str:
    """Return the benchmark SQL for ``variant`` with parquet globs substituted."""
    if variant not in ("full", "pruned"):
        raise ValueError(f"unknown variant {variant!r}")
    text = (SQL_DIR / f"windowed_join_{variant}.sql").read_text(encoding="utf-8")
    return text.replace("{RAW}", raw or raw_glob()).replace("{MODELS}", models or models_glob())


def sql_fingerprint(sql: str) -> str:
    """Stable hash of the query text (ignoring layout), recorded with results."""
    normalized = re.sub(r"\s+", " ", sql).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def dataset_manifest() -> dict:
    path = DATA_DIR / "dataset.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def dataset_rows() -> Optional[int]:
    return dataset_manifest().get("rows")


def peak_rss_mb() -> Optional[float]:
    """Best-effort peak RSS. ``resource`` is unavailable on Windows hosts."""
    try:
        import resource  # type: ignore

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KiB, macOS reports bytes.
        divisor = 1024.0 if sys.platform != "darwin" else 1024.0 * 1024.0
        return round(usage / divisor, 1)
    except Exception:
        pass
    try:  # Windows
        import ctypes
        from ctypes import wintypes  # type: ignore

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(counters)
        handle = ctypes.windll.kernel32.GetCurrentProcess()  # type: ignore[attr-defined]
        if ctypes.windll.psapi.GetProcessMemoryInfo(  # type: ignore[attr-defined]
            handle, ctypes.byref(counters), counters.cb
        ):
            return round(counters.PeakWorkingSetSize / (1024.0 * 1024.0), 1)
    except Exception:
        pass
    return None


def normalize_aggregates(raw: Any) -> dict:
    """Coerce one result row (tuple / dict / list) into the four named fields."""
    if isinstance(raw, dict):
        values = [raw[name] for name in AGGREGATES]
    elif hasattr(raw, "asDict"):  # pyspark Row
        as_dict = raw.asDict()
        values = [as_dict[name] for name in AGGREGATES]
    else:
        values = list(raw)

    out: dict = {}
    for name, value in zip(AGGREGATES, values):
        if value is None:
            out[name] = None
        elif name in ("row_count", "sum_row_num"):
            out[name] = int(value)
        else:
            out[name] = float(value)
    return out


class Stopwatch:
    """Wall-clock stopwatch with an explicit ``start``/``stop`` pair."""

    def __init__(self) -> None:
        self.elapsed: Optional[float] = None
        self._t0: Optional[float] = None

    def start(self) -> "Stopwatch":
        self._t0 = time.perf_counter()
        return self

    def stop(self) -> float:
        if self._t0 is None:
            raise RuntimeError("stopwatch was never started")
        self.elapsed = time.perf_counter() - self._t0
        return self.elapsed


def write_result(engine: str, payload: dict) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"engine": engine, "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%S"), **payload}
    path = RESULTS_DIR / f"{engine}.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def base_payload(
    engine: str,
    version: str,
    variant: str,
    sql: str,
    settings: Optional[dict] = None,
) -> dict:
    return {
        "engine_version": version,
        "variant": variant,
        "pruned": variant == "pruned",
        "dataset_rows": dataset_rows(),
        "dataset_manifest": dataset_manifest(),
        "query_fingerprint": sql_fingerprint(sql),
        "benchmark_smart_column": BENCH_SMART_RAW,
        "rolling_window": ROLLING_WINDOW,
        "settings": settings or {},
        "host": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
    }
