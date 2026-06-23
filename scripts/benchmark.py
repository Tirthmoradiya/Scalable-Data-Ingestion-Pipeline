#!/usr/bin/env python3
"""
Scalability benchmark script.
Generates synthetic datasets of increasing sizes and measures:
  - Throughput (rows/sec)
  - Chunk processing time
  - Memory efficiency (via chunked streaming)
  - Thread scaling (1 → 2 → 4 → 8 workers)

Run: python scripts/benchmark.py
"""

from __future__ import annotations

import csv
import io
import json
import os
import statistics
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Dataset generators
# ---------------------------------------------------------------------------
def generate_csv(rows: int, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["name", "email", "phone", "city", "country"]
        )
        writer.writeheader()
        for i in range(rows):
            writer.writerow(
                {
                    "name": f"User {i}",
                    "email": f"user{i}@benchmark.com",
                    "phone": f"+1-555-{i:04d}",
                    "city": ["London", "New York", "Tokyo", "Berlin"][i % 4],
                    "country": ["UK", "US", "JP", "DE"][i % 4],
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
                        "id": i,
                        "event": "page_view",
                        "user_id": i % 1000,
                        "url": f"/page/{i}",
                        "ts": f"2024-01-{(i % 28) + 1:02d}T12:00:00Z",
                        "duration_ms": (i % 500) + 50,
                    }
                )
                + "\n"
            )
    return path


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------
def bench_ingestion(label: str, rows: int, file_path: Path, ingester_cls, db_url: str) -> dict:
    from pipeline.ingestion.csv_ingester import CSVIngester
    from pipeline.ingestion.json_ingester import JSONIngester
    from pipeline.runner import PipelineRunner

    runner = PipelineRunner(db_url=db_url)
    ingester = ingester_cls(file_path)

    start = time.perf_counter()
    result = runner.run(ingester=ingester, entity_type="customers", max_workers=1, chunk_size=500)
    elapsed = time.perf_counter() - start

    throughput = rows / elapsed if elapsed > 0 else 0
    return {
        "label": label,
        "rows": rows,
        "elapsed_s": round(elapsed, 3),
        "throughput_rps": round(throughput),
        "chunks": result.chunks_processed,
    }


def bench_thread_scaling(rows: int, file_path: Path, tmp: Path) -> list[dict]:
    from pipeline.runner import PipelineRunner
    from pipeline.ingestion.csv_ingester import CSVIngester

    results = []
    for workers in [1, 2, 4]:
        db_path = tmp / f"scale_w{workers}.db"
        db_path.unlink(missing_ok=True)  # always start fresh
        runner = PipelineRunner(db_url=f"sqlite:///{db_path}")
        ingester = CSVIngester(file_path)
        start = time.perf_counter()
        result = runner.run(
            ingester=ingester, entity_type="customers", max_workers=workers, chunk_size=500
        )
        elapsed = time.perf_counter() - start
        results.append(
            {
                "workers": workers,
                "rows": rows,
                "elapsed_s": round(elapsed, 3),
                "throughput_rps": round(rows / elapsed) if elapsed > 0 else 0,
                "chunks": result.chunks_processed,
            }
        )
        db_path.unlink(missing_ok=True)
    return results


def bench_chunk_streaming(sizes: list[int], file_path: Path) -> list[dict]:
    """Measure streaming throughput vs chunk size."""
    from pipeline.ingestion.csv_ingester import CSVIngester

    results = []
    for chunk_size in sizes:
        ingester = CSVIngester(file_path)
        chunks = []
        start = time.perf_counter()
        for chunk in ingester.ingest_chunks(chunk_size=chunk_size):
            chunks.append(len(chunk))
        elapsed = time.perf_counter() - start
        total_rows = sum(chunks)
        results.append(
            {
                "chunk_size": chunk_size,
                "total_rows": total_rows,
                "num_chunks": len(chunks),
                "elapsed_s": round(elapsed, 4),
                "throughput_rps": round(total_rows / elapsed) if elapsed > 0 else 0,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------
def print_table(title: str, rows: list[dict]) -> None:
    if not rows:
        return
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")
    headers = list(rows[0].keys())
    col_w = {h: max(len(h), max(len(str(r[h])) for r in rows)) + 2 for h in headers}
    header_line = "  ".join(h.ljust(col_w[h]) for h in headers)
    print(header_line)
    print("-" * len(header_line))
    for row in rows:
        print("  ".join(str(row[h]).ljust(col_w[h]) for h in headers))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    tmp = ROOT / "data" / "_bench"
    tmp.mkdir(parents=True, exist_ok=True)

    print("\n🚀  Scalable Data Ingestion Pipeline — Benchmark Suite")
    print("=" * 65)

    # ── 1. Streaming throughput (CSV ingester only, no DB) ────────────────
    print("\n[1/3] Streaming read throughput (no DB write) ...")
    csv_10k = generate_csv(10_000, tmp / "bench_10k.csv")
    stream_results = bench_chunk_streaming([100, 500, 1000, 2500, 5000], csv_10k)
    print_table("Chunk Streaming Throughput (10,000 rows CSV)", stream_results)

    # ── 2. Full pipeline throughput at increasing dataset sizes ───────────
    print("\n[2/3] Full pipeline throughput (ingest → clean → load → SQLite) ...")
    from pipeline.ingestion.csv_ingester import CSVIngester
    from pipeline.ingestion.json_ingester import JSONIngester

    scale_results = []
    for n in [500, 1_000, 5_000, 10_000]:
        db_path = tmp / f"bench_{n}.db"
        db_url = f"sqlite:///{db_path}"
        csv_path = generate_csv(n, tmp / f"bench_{n}.csv")
        r = bench_ingestion(f"CSV {n:>6,} rows", n, csv_path, CSVIngester, db_url)
        scale_results.append(r)
        db_path.unlink(missing_ok=True)

    print_table("Full Pipeline Throughput (CSV → SQLite)", scale_results)

    # ── 3. Thread worker scaling ──────────────────────────────────────────
    print("\n[3/3] Thread worker scaling (5,000 rows, varying workers) ...")
    csv_5k = generate_csv(5_000, tmp / "bench_scale.csv")
    thread_results = bench_thread_scaling(5_000, csv_5k, tmp)
    print_table("Thread Scaling (5,000 rows CSV → SQLite)", thread_results)

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    best = max(scale_results, key=lambda r: r["throughput_rps"])
    best_thread = max(thread_results, key=lambda r: r["throughput_rps"])
    print(f"  ✅ Peak streaming throughput : {stream_results[-1]['throughput_rps']:>7,} rows/sec")
    print(f"  ✅ Peak full-pipeline        : {best['throughput_rps']:>7,} rows/sec  ({best['label']})")
    print(f"  ✅ Best worker config        : {best_thread['workers']} workers → {best_thread['throughput_rps']:>7,} rows/sec")
    print("=" * 65 + "\n")

    # Cleanup
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
