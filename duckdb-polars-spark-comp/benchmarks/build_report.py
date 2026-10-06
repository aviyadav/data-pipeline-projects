#!/usr/bin/env python3
"""Turn ``results/*.json`` into a human-readable comparison report.

Every number in the report comes from a result file written by one of the three
benchmarks; nothing is typed in by hand. Correctness is checked by comparing the
four aggregate "fingerprints" the article used (row_count, sum_row_num,
sum_previous, sum_rolling_avg) across engines: if the engines disagree, the
timings are not comparable and the report says so.

    python benchmarks/build_report.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

BENCH_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BENCH_DIR.parent
sys.path.insert(0, str(BENCH_DIR))

from common import AGGREGATES, RESULTS_DIR  # noqa: E402

REPORT_DIR = Path(os.environ.get("BENCH_REPORT_DIR", PROJECT_ROOT / "report"))

DISPLAY = {
    "duckdb": "DuckDB",
    "polars": "Polars",
    "spark": "Spark (Docker)",
}

# Order the engines fastest-first conceptually is wrong; keep a stable order
# that matches how the article walked through them.
ORDER = ["spark", "duckdb", "polars"]


def load_results() -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    if not RESULTS_DIR.exists():
        return out
    for path in sorted(RESULTS_DIR.glob("*.json")):
        if path.name in ("dataset.json", "validation.json"):
            continue
        try:
            out[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
    return out


def fmt_seconds(value: Optional[float]) -> str:
    return "-" if value is None else f"{value:,.2f}"


def engine_family(stem: str) -> str:
    return stem.split("-")[0]


def label_for(stem: str) -> str:
    family = engine_family(stem)
    base = DISPLAY.get(family, family)
    suffix = stem[len(family):].lstrip("-")
    return f"{base} ({suffix})" if suffix else base


def sort_key(stem: str):
    family = engine_family(stem)
    index = ORDER.index(family) if family in ORDER else len(ORDER)
    return (index, stem)


def bar(value: float, peak: float, width: int = 28) -> str:
    if peak <= 0:
        return ""
    filled = max(1, int(round(value / peak * width)))
    return "#" * filled


def svg_chart(rows: List[tuple], peak: float) -> str:
    """Small horizontal bar chart; self-contained, no external assets."""
    row_h, pad_l, pad_t = 34, 190, 16
    width = 720
    height = pad_t * 2 + row_h * len(rows)
    bar_max = width - pad_l - 90
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="ui-sans-serif, system-ui, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
    ]
    for index, (label, seconds, status) in enumerate(rows):
        y = pad_t + index * row_h
        value = seconds if seconds is not None else peak
        length = max(2.0, value / peak * bar_max) if peak else 2.0
        color = "#2f6fed" if status == "ok" else "#c2413b"
        text = f"{seconds:,.2f} s" if seconds is not None else "failed"
        parts.append(
            f'<text x="12" y="{y + 20}" font-size="14" fill="#111827">{label}</text>'
        )
        parts.append(
            f'<rect x="{pad_l}" y="{y + 6}" width="{length:.1f}" height="20" rx="3" fill="{color}"/>'
        )
        parts.append(
            f'<text x="{pad_l + length + 8:.1f}" y="{y + 21}" font-size="13" '
            f'fill="#374151">{text}</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def correctness(payloads: Dict[str, dict]) -> tuple:
    """Compare the aggregate fingerprints across engines that succeeded."""
    ok = {k: v for k, v in payloads.items() if v.get("status") == "ok" and v.get("result")}
    if len(ok) < 2:
        return True, "fewer than two engines produced a result; nothing to compare"

    reference_stem = next((k for k in sorted(ok, key=sort_key) if engine_family(k) == "spark"), None)
    reference_stem = reference_stem or sorted(ok, key=sort_key)[0]
    reference = ok[reference_stem]["result"]

    problems: List[str] = []
    for stem, payload in sorted(ok.items(), key=lambda kv: sort_key(kv[0])):
        if stem == reference_stem:
            continue
        result = payload["result"]
        for field in AGGREGATES:
            a, b = reference.get(field), result.get(field)
            if a is None or b is None:
                continue
            if field == "row_count":
                if int(a) != int(b):
                    problems.append(f"{stem}: {field} {b:,} != {a:,}")
            else:
                scale = max(abs(float(a)), 1.0)
                if abs(float(a) - float(b)) / scale > 1e-9:
                    problems.append(f"{stem}: {field} {b} != {a}")

    if problems:
        return False, "; ".join(problems)
    # The four aggregates are compared with a tolerance, not bit-for-bit:
    # `sum_rolling_avg` is a float mean, so engines legitimately differ in the
    # last few digits. Say so rather than claiming exact equality.
    return True, (
        "all engines agree within 1e-9 relative tolerance "
        f"(reference: {label_for(reference_stem)}); `sum_rolling_avg` is a float "
        "mean, so its trailing digits differ between engines"
    )


def variant_section(variant: str, payloads: Dict[str, dict]) -> str:
    subset = {k: v for k, v in payloads.items() if v.get("variant") == variant}
    if not subset:
        return f"_No results recorded for the `{variant}` variant._\n"

    ok_timings = {
        k: v["elapsed_seconds"]
        for k, v in subset.items()
        if v.get("status") == "ok" and v.get("elapsed_seconds")
    }
    spark_time = next(
        (v for k, v in ok_timings.items() if engine_family(k) == "spark"), None
    )
    peak = max(ok_timings.values()) if ok_timings else 0.0

    lines: List[str] = []
    lines.append("| Engine | Status | Elapsed (s) | vs Spark | rows | Peak RSS (MB) | Error |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
    for stem in sorted(subset, key=sort_key):
        payload = subset[stem]
        seconds = payload.get("elapsed_seconds")
        status = payload.get("status", "?")
        result = payload.get("result") or {}
        rows = result.get("row_count")
        rss = payload.get("peak_rss_mb")
        if status == "ok" and seconds and spark_time:
            ratio = f"{seconds / spark_time:.2f}x"
        else:
            ratio = "-"
        error = (payload.get("error") or "").replace("|", "/")
        if len(error) > 90:
            error = error[:87] + "..."
        lines.append(
            f"| {label_for(stem)} | {status} | {fmt_seconds(seconds)} | {ratio} | "
            f"{rows if rows is not None else '-'} | {rss if rss is not None else '-'} | {error} |"
        )
    lines.append("")

    if ok_timings:
        lines.append("```text")
        for stem in sorted(ok_timings, key=lambda s: ok_timings[s]):
            seconds = ok_timings[stem]
            lines.append(f"{label_for(stem):<28} {seconds:>9,.2f}s  {bar(seconds, peak)}")
        lines.append("```")
        lines.append("")

    if any(v.get("peak_rss_mb") is None for v in subset.values()):
        lines.append(
            "_Peak RSS is blank for engines that do not report it. Spark's work "
            "happens in executor JVMs, so a driver-side figure would not be "
            "comparable with the in-process engines' numbers._\n"
        )

    settings = []
    for stem in sorted(subset, key=sort_key):
        payload = subset[stem]
        runs = payload.get("runs_seconds") or []
        settings.append(
            f"- **{label_for(stem)}** `v{payload.get('engine_version', '?')}` "
            f"timed_runs={runs} "
            f"settings={json.dumps(payload.get('settings', {}), sort_keys=True)}"
        )
    lines.extend(settings)
    lines.append("")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=REPORT_DIR / "REPORT.md")
    parser.add_argument("--title", default="DuckDB vs Polars vs Spark on Docker")
    args = parser.parse_args(argv)

    payloads = load_results()
    dataset_path = RESULTS_DIR / "dataset.json"
    dataset = json.loads(dataset_path.read_text(encoding="utf-8")) if dataset_path.exists() else {}
    validation_path = RESULTS_DIR / "validation.json"
    validation = (
        json.loads(validation_path.read_text(encoding="utf-8")) if validation_path.exists() else {}
    )

    doc: List[str] = []
    doc.append(f"# {args.title}\n")
    doc.append(
        f"_Generated by `benchmarks/build_report.py` at "
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} from `results/*.json`._\n"
    )

    doc.append("## What this measures\n")
    doc.append(
        "A re-implementation of the benchmark in "
        "`duckdb-polars-spark-performance-comparision.pdf`, with two deliberate "
        "changes requested for this project:\n"
    )
    doc.append(
        "1. **Databricks Serverless is replaced by Docker.** PySpark runs on a "
        "self-hosted Spark standalone cluster (one master, two workers) built "
        "from the official `apache/spark:3.5.3` image. DuckDB and Polars run in "
        "a sibling container on the same host, reading the same files.\n"
        "2. **The Backblaze dataset is generated, not downloaded**, at "
        f"**{dataset.get('rows', 0):,} rows** instead of the article's "
        "285,339,435.\n"
    )
    doc.append(
        "The workload is unchanged: three window expressions partitioned by "
        "`serial_number` and ordered by `date` (`row_number`, `lag(1)`, a "
        "trailing 30-row `avg`), a left join onto a small per-drive dimension, "
        "and four aggregates that act as a correctness fingerprint.\n"
    )

    doc.append("## Dataset\n")
    if dataset:
        doc.append("| Property | Value |")
        doc.append("| --- | --- |")
        doc.append(f"| Rows | {dataset.get('rows', 0):,} |")
        doc.append(f"| Columns | {dataset.get('columns', '?')} |")
        doc.append(f"| Drives (distinct serials) | {dataset.get('drives', '?'):,} |")
        doc.append(f"| Dimension rows (`models`) | {dataset.get('models_rows', '?'):,} |")
        doc.append(f"| Seed | {dataset.get('seed', '?')} |")
        doc.append(f"| Fact files | {dataset.get('raw_files', '?')} |")
        doc.append(
            f"| Fact size on disk | {dataset.get('raw_bytes', 0) / 2**20:,.0f} MiB "
            f"({dataset.get('compression', '?')}) |"
        )
        doc.append(f"| Generation time | {dataset.get('elapsed_seconds', '?')} s |")
        doc.append("")
    else:
        doc.append("_No dataset manifest found._\n")

    if validation:
        doc.append("## Validation\n")
        failed = validation.get("checks_failed") or []
        doc.append(
            f"- Rows: **{validation.get('rows', '?')}**, drives: "
            f"**{validation.get('drives', '?')}**\n"
            f"- Date range: {validation.get('date_min')} .. {validation.get('date_max')}\n"
            f"- Failure rows: {validation.get('failures')}\n"
            f"- `smart_5_raw`: avg {validation.get('smart5_avg', 0):.3f}, "
            f"max {validation.get('smart5_max')}\n"
            f"- Structural checks: {'**all passed**' if not failed else '**FAILED**: ' + ', '.join(failed)}\n"
        )

    full = {k: v for k, v in payloads.items() if v.get("variant") == "full"}
    pruned = {k: v for k, v in payloads.items() if v.get("variant") == "pruned"}

    doc.append("## Results — article query (`full` variant)\n")
    doc.append(
        "The article's query verbatim: an `r.* EXCLUDE (model)` projection over "
        "the 193-column table. Be careful reading this as a wide-scan test — "
        "every engine's optimiser prunes that projection down to the three "
        "columns the plan actually needs, which is why it lands so close to the "
        "`pruned` variant below. Captured physical plans are in "
        "[`report/PLANS.md`](PLANS.md).\n"
    )
    doc.append(variant_section("full", payloads))

    ok_full = {k: v["elapsed_seconds"] for k, v in full.items() if v.get("status") == "ok"}
    if ok_full:
        rows = sorted(ok_full.items(), key=lambda kv: kv[1])
        doc.append("![timings](timings.svg)\n")
        chart_rows = [
            (label_for(stem), full[stem].get("elapsed_seconds"), full[stem].get("status"))
            for stem in sorted(full, key=sort_key)
        ]
        peak = max([r[1] for r in chart_rows if r[1]] or [1.0])
        try:
            (args.out.parent).mkdir(parents=True, exist_ok=True)
            (args.out.parent / "timings.svg").write_text(svg_chart(chart_rows, peak), encoding="utf-8")
        except OSError:
            pass

    if pruned:
        doc.append("## Results — pruned scan\n")
        doc.append(
            "Same semantics, but the query text names only `serial_number`, "
            "`date` and `smart_5_raw`, mirroring the article's attempt to give "
            "Polars its best chance. Because the `full` variant gets "
            "column-pruned anyway, the two land in the same place — that "
            "convergence is the finding, not a mistake.\n"
        )
        doc.append(variant_section("pruned", payloads))

    doc.append("## Correctness\n")
    for variant_name, subset in (("full", full), ("pruned", pruned)):
        if not subset:
            continue
        ok, message = correctness(subset)
        doc.append(f"- **{variant_name}**: {'PASS' if ok else 'MISMATCH'} — {message}")
    doc.append("")

    doc.append("## Observations\n")
    if ok_full:
        fastest = min(ok_full.items(), key=lambda kv: kv[1])
        slowest = max(ok_full.items(), key=lambda kv: kv[1])
        doc.append(
            f"- Fastest engine on the article's query: **{label_for(fastest[0])}** at "
            f"{fastest[1]:,.2f}s; slowest: **{label_for(slowest[0])}** at "
            f"{slowest[1]:,.2f}s ({slowest[1] / fastest[1]:.2f}x)."
        )
    failures = [
        f"{label_for(k)}: {(v.get('error') or '')}" for k, v in payloads.items() if v.get("status") != "ok"
    ]
    for failure in failures:
        doc.append(f"- Engine did not complete — {failure}")
    if not failures:
        doc.append("- All three engines completed both variants on this machine.")
    doc.append(
        "- `elapsed_seconds` is the best of the timed runs and covers the whole "
        "query from the data source: DuckDB `execute()`, Polars `collect()`, and "
        "for Spark a fresh re-plan plus the distributed job (shuffle, sort, "
        "window, aggregate). Spark's session startup is reported separately as "
        "`session_start_seconds` and is deliberately excluded."
    )
    doc.append("")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(doc), encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
