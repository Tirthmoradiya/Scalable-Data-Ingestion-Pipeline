#!/usr/bin/env python3
"""
Scalable Data Ingestion Pipeline — Benchmark Suite
====================================================
Generates synthetic datasets and measures:
  - Streaming read throughput (no DB write)
  - Multi-format throughput: CSV, NDJSON, Parquet
  - Full pipeline throughput at increasing dataset sizes
  - Thread worker scaling (1 → 2 → 4 → 8 workers)
  - Cross-platform DB backend comparison

Usage
-----
# SQLite (default — no setup required)
python scripts/benchmark.py

# MySQL
python scripts/benchmark.py --db-url "mysql+pymysql://user:pass@localhost/bench_db"

# PostgreSQL
python scripts/benchmark.py --db-url "postgresql+psycopg2://user:pass@localhost/bench_db"

# Custom scale & workers
python scripts/benchmark.py --max-rows 50000 --workers 1 2 4 8

# Save results to JSON
python scripts/benchmark.py --output results.json
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent

CITIES = ["London", "New York", "Tokyo", "Berlin", "Sydney", "Paris", "Toronto", "Mumbai"]
COUNTRIES = ["UK", "US", "JP", "DE", "AU", "FR", "CA", "IN"]


# ---------------------------------------------------------------------------
# Dataset generators
# ---------------------------------------------------------------------------
def generate_csv(rows: int, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "email", "phone", "city", "country"])
        writer.writeheader()
        for i in range(rows):
            writer.writerow(
                {
                    "name": f"User {i}",
                    "email": f"user{i}@benchmark.com",
                    "phone": f"+1-555-{i:07d}",
                    "city": CITIES[i % len(CITIES)],
                    "country": COUNTRIES[i % len(COUNTRIES)],
                }
            )
    return path


def generate_ndjson(rows: int, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for i in range(rows):
            f.write(
                json.dumps(
                    {
                        "name": f"User {i}",
                        "email": f"ndjson{i}@benchmark.com",
                        "phone": f"+1-555-{i:07d}",
                        "city": CITIES[i % len(CITIES)],
                        "country": COUNTRIES[i % len(COUNTRIES)],
                    }
                )
                + "\n"
            )
    return path


def generate_parquet(rows: int, path: Path) -> Path:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("  ⚠️  pyarrow not installed — skipping Parquet benchmark")
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "name": [f"User {i}" for i in range(rows)],
            "email": [f"parquet{i}@benchmark.com" for i in range(rows)],
            "phone": [f"+1-555-{i:07d}" for i in range(rows)],
            "city": [CITIES[i % len(CITIES)] for i in range(rows)],
            "country": [COUNTRIES[i % len(COUNTRIES)] for i in range(rows)],
        }
    )
    pq.write_table(table, path, row_group_size=500)
    return path


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def make_db_url(db_url_arg: str | None, tmp: Path, label: str = "bench") -> str:
    """Return a usable DB URL, defaulting to a fresh SQLite file."""
    if db_url_arg:
        return db_url_arg
    db_path = tmp / f"{label}.db"
    db_path.unlink(missing_ok=True)
    return f"sqlite:///{db_path}"


def detect_db_backend(db_url: str) -> str:
    if "sqlite" in db_url:
        return "SQLite"
    if "mysql" in db_url:
        return "MySQL"
    if "postgresql" in db_url or "postgres" in db_url:
        return "PostgreSQL"
    return "Unknown"


# ---------------------------------------------------------------------------
# Core benchmark functions
# ---------------------------------------------------------------------------
def bench_streaming(
    file_path: Path, chunk_sizes: list[int], ingester_cls: Any
) -> list[dict[str, Any]]:
    """Pure read throughput — no DB involved."""
    results = []
    for chunk_size in chunk_sizes:
        ingester = ingester_cls(file_path)
        chunks: list[int] = []
        start = time.perf_counter()
        for chunk in ingester.ingest_chunks(chunk_size=chunk_size):
            chunks.append(len(chunk))
        elapsed = time.perf_counter() - start
        total = sum(chunks)
        results.append(
            {
                "chunk_size": chunk_size,
                "total_rows": total,
                "num_chunks": len(chunks),
                "elapsed_s": round(elapsed, 4),
                "throughput_rps": round(total / elapsed) if elapsed > 0 else 0,
            }
        )
    return results


def bench_format(
    label: str,
    rows: int,
    file_path: Path,
    ingester_cls: Any,
    db_url: str,
    workers: int = 1,
    chunk_size: int = 500,
) -> dict[str, Any]:
    """Full pipeline: ingest → clean → validate → load."""
    from pipeline.runner import PipelineRunner

    runner = PipelineRunner(db_url=db_url)
    ingester = ingester_cls(file_path)
    start = time.perf_counter()
    result = runner.run(
        ingester=ingester,
        entity_type="customers",
        max_workers=workers,
        chunk_size=chunk_size,
    )
    elapsed = time.perf_counter() - start
    return {
        "format": label,
        "rows": rows,
        "workers": workers,
        "chunk_size": chunk_size,
        "elapsed_s": round(elapsed, 3),
        "throughput_rps": round(rows / elapsed) if elapsed > 0 else 0,
        "rows_ingested": result.rows_ingested,
        "rows_failed": result.rows_failed,
        "chunks": result.chunks_processed,
    }


def bench_scale(
    row_counts: list[int],
    tmp: Path,
    db_url_arg: str | None,
    workers: int = 1,
    chunk_size: int = 500,
) -> list[dict]:
    """Throughput at increasing dataset sizes."""
    from pipeline.ingestion.csv_ingester import CSVIngester

    results = []
    for n in row_counts:
        db_url = make_db_url(db_url_arg, tmp, f"scale_{n}")
        csv_path = generate_csv(n, tmp / f"scale_{n}.csv")
        r = bench_format("CSV", n, csv_path, CSVIngester, db_url, workers, chunk_size)
        results.append(r)
        # clean up SQLite DB files between runs to avoid UNIQUE violations
        if "sqlite" in db_url:
            db_file = Path(db_url.replace("sqlite:///", ""))
            db_file.unlink(missing_ok=True)
    return results


def bench_workers(
    rows: int,
    worker_counts: list[int],
    tmp: Path,
    db_url_arg: str | None,
    chunk_size: int = 500,
) -> list[dict]:
    """Same dataset, varying worker counts."""
    from pipeline.ingestion.csv_ingester import CSVIngester

    csv_path = generate_csv(rows, tmp / "workers_bench.csv")
    results = []
    for w in worker_counts:
        db_url = make_db_url(db_url_arg, tmp, f"workers_{w}")
        r = bench_format("CSV", rows, csv_path, CSVIngester, db_url, w, chunk_size)
        results.append(r)
        if "sqlite" in db_url:
            db_file = Path(db_url.replace("sqlite:///", ""))
            db_file.unlink(missing_ok=True)
    return results


def bench_formats(rows: int, tmp: Path, db_url_arg: str | None) -> list[dict]:
    """Compare CSV vs NDJSON vs Parquet at the same row count."""
    from pipeline.ingestion.csv_ingester import CSVIngester
    from pipeline.ingestion.json_ingester import JSONIngester

    results = []

    for label, gen_fn, ingester_cls, ext in [
        ("CSV", generate_csv, CSVIngester, "csv"),
        ("NDJSON", generate_ndjson, JSONIngester, "ndjson"),
    ]:
        path = gen_fn(rows, tmp / f"fmt_{ext}.{ext}")
        db_url = make_db_url(db_url_arg, tmp, f"fmt_{ext}")
        r = bench_format(label, rows, path, ingester_cls, db_url)
        results.append(r)
        if "sqlite" in db_url:
            db_file = Path(db_url.replace("sqlite:///", ""))
            db_file.unlink(missing_ok=True)

    # Parquet (optional)
    try:
        import pyarrow  # noqa: F401

        from pipeline.ingestion.parquet_ingester import ParquetIngester

        path = generate_parquet(rows, tmp / "fmt_parquet.parquet")
        db_url = make_db_url(db_url_arg, tmp, "fmt_parquet")
        r = bench_format("Parquet", rows, path, ParquetIngester, db_url)
        results.append(r)
        if "sqlite" in db_url:
            db_file = Path(db_url.replace("sqlite:///", ""))
            db_file.unlink(missing_ok=True)
    except ImportError:
        print("  ⚠️  pyarrow not installed — skipping Parquet format benchmark")

    return results


# ---------------------------------------------------------------------------
# Pretty printing
# ---------------------------------------------------------------------------
def print_table(title: str, rows: list[dict], highlight_col: str | None = None) -> None:
    if not rows:
        return
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}")
    headers = list(rows[0].keys())
    col_w = {h: max(len(h), max(len(str(r[h])) for r in rows)) + 2 for h in headers}
    header_line = "  ".join(h.ljust(col_w[h]) for h in headers)
    print(header_line)
    print("─" * len(header_line))
    for row in rows:
        line = "  ".join(str(row[h]).ljust(col_w[h]) for h in headers)
        print(line)


def print_summary(stream: list[dict], scale: list[dict], workers: list[dict], backend: str) -> None:
    peak_stream = max(stream, key=lambda r: r["throughput_rps"]) if stream else None
    peak_scale = max(scale, key=lambda r: r["throughput_rps"]) if scale else None
    best_w = max(workers, key=lambda r: r["throughput_rps"]) if workers else None
    print(f"\n{'═' * 70}")
    print(f"  SUMMARY  |  Backend: {backend}")
    print(f"{'═' * 70}")
    if peak_stream:
        print(
            f"  ✅ Peak streaming read       : "
            f"{peak_stream['throughput_rps']:>10,} rows/sec  "
            f"(chunk={peak_stream['chunk_size']})"
        )
    if peak_scale:
        print(
            f"  ✅ Peak full-pipeline        : "
            f"{peak_scale['throughput_rps']:>10,} rows/sec  "
            f"({peak_scale['rows']:,} rows)"
        )
    if best_w:
        print(
            f"  ✅ Best worker count         : {best_w['workers']} workers  "
            f"→ {best_w['throughput_rps']:>10,} rows/sec"
        )
    print(f"{'═' * 70}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the pipeline benchmark suite against any DB backend."
    )
    p.add_argument(
        "--db-url",
        default=None,
        help=(
            "SQLAlchemy DB URL. Defaults to SQLite (no setup needed).\n"
            "Examples:\n"
            "  mysql+pymysql://user:pass@localhost/bench_db\n"
            "  postgresql+psycopg2://user:pass@localhost/bench_db"
        ),
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=10_000,
        help="Maximum row count for scale benchmark (default: 10,000)",
    )
    p.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4],
        help="Worker counts to test in the scaling benchmark (default: 1 2 4)",
    )
    p.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Chunk size for all pipeline runs (default: 500)",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Save all results to a JSON file",
    )
    p.add_argument(
        "--skip-streaming",
        action="store_true",
        help="Skip the streaming-only benchmark",
    )
    p.add_argument(
        "--skip-formats",
        action="store_true",
        help="Skip the multi-format comparison benchmark",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    tmp = ROOT / "data" / "_bench"
    tmp.mkdir(parents=True, exist_ok=True)

    backend = detect_db_backend(args.db_url or "sqlite")
    if args.max_rows > 5_000:
        row_counts = [500, 1_000, 5_000, args.max_rows]
    else:
        row_counts = [500, 1_000, args.max_rows]

    print("\n🚀  Scalable Data Ingestion Pipeline — Benchmark Suite")
    print(f"{'═' * 70}")
    print(f"  Backend  : {backend}")
    print(f"  Max rows : {args.max_rows:,}")
    print(f"  Workers  : {args.workers}")
    print(f"  Chunk    : {args.chunk_size}")
    print(f"{'═' * 70}")

    all_results: dict = {"backend": backend, "config": vars(args)}

    # ── 1. Streaming throughput ──────────────────────────────────────────
    stream_results: list[dict] = []
    if not args.skip_streaming:
        print("\n[1/4] Streaming read throughput (no DB, CSV) ...")
        from pipeline.ingestion.csv_ingester import CSVIngester

        csv_path = generate_csv(10_000, tmp / "stream_10k.csv")
        stream_results = bench_streaming(csv_path, [100, 500, 1_000, 2_500, 5_000], CSVIngester)
        print_table("Streaming Throughput — 10,000 rows CSV (no DB write)", stream_results)
        all_results["streaming"] = stream_results

    # ── 2. Multi-format comparison ───────────────────────────────────────
    fmt_results: list[dict] = []
    if not args.skip_formats:
        print("\n[2/4] Multi-format comparison (CSV vs NDJSON vs Parquet, 5,000 rows) ...")
        fmt_results = bench_formats(5_000, tmp, args.db_url)
        print_table(f"Format Comparison — 5,000 rows → {backend}", fmt_results)
        all_results["formats"] = fmt_results

    # ── 3. Scale benchmark ───────────────────────────────────────────────
    print(f"\n[3/4] Pipeline throughput at increasing scale → {backend} ...")
    scale_results = bench_scale(row_counts, tmp, args.db_url, args.workers[0], args.chunk_size)
    print_table(f"Scale Benchmark — CSV → {backend} (workers={args.workers[0]})", scale_results)
    all_results["scale"] = scale_results

    # ── 4. Worker scaling ────────────────────────────────────────────────
    print(f"\n[4/4] Worker thread scaling — 5,000 rows → {backend} ...")
    worker_results = bench_workers(5_000, args.workers, tmp, args.db_url, args.chunk_size)
    print_table(f"Worker Scaling — 5,000 rows → {backend}", worker_results)
    all_results["workers"] = worker_results

    # ── Summary ──────────────────────────────────────────────────────────
    print_summary(stream_results, scale_results, worker_results, backend)

    # ── Optional JSON output ─────────────────────────────────────────────
    if args.output:
        Path(args.output).write_text(json.dumps(all_results, indent=2))
        print(f"  📄 Results saved to: {args.output}\n")

    # Cleanup temp data
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
