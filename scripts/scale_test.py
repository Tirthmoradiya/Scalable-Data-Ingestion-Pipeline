"""
Scale test runner — finds the maximum throughput the pipeline can sustain
within a 4 GB RAM budget, across multiple row counts and chunk sizes.

What it does
------------
1. Generates datasets: 10K → 100K → 500K → 1M → 2M → 5M rows (stops if OOM risk)
2. For each dataset × chunk_size configuration, runs the pipeline
3. Monitors peak RAM via psutil background thread (samples every 50ms)
4. Records: throughput (rows/s), peak RAM (MB), duration, rows loaded, failed
5. Prints a rich summary table and saves JSON results to data/scale/results.json

Usage
-----
    .venv/bin/python scripts/scale_test.py
    .venv/bin/python scripts/scale_test.py --max-rows 5000000 --ram-limit-gb 3.5
    .venv/bin/python scripts/scale_test.py --quick   # 10K / 100K / 500K only
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psutil
from sqlalchemy import create_engine, text

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.cleaning.cleaner import DataCleaner
from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.loader.db_loader import DBLoader
from pipeline.models import Base
from pipeline.transformations.transformer import DataTransformer
from pipeline.utils.metrics import PipelineMetrics

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
SCALE_DATA_DIR = PROJECT_DIR / "data" / "scale"
RESULTS_FILE = SCALE_DATA_DIR / "results.json"

CHUNK_SIZES_TO_TEST = [500, 1000, 5000]

ROW_TIERS = [
    ("10K",  10_000),
    ("100K", 100_000),
    ("500K", 500_000),
    ("1M",   1_000_000),
    ("2M",   2_000_000),
    ("5M",   5_000_000),
]


# ---------------------------------------------------------------------------
# Memory monitor (background thread)
# ---------------------------------------------------------------------------
class MemoryMonitor:
    """Samples RSS memory of the current process at 50ms intervals."""

    def __init__(self, sample_interval: float = 0.05) -> None:
        self._proc = psutil.Process(os.getpid())
        self._interval = sample_interval
        self._running = False
        self._thread: threading.Thread | None = None
        self.peak_mb: float = 0.0
        self.baseline_mb: float = 0.0
        self.samples: list[float] = []

    def start(self) -> None:
        self.baseline_mb = self._proc.memory_info().rss / 1e6
        self.peak_mb = self.baseline_mb
        self.samples = [self.baseline_mb]
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _sample_loop(self) -> None:
        while self._running:
            try:
                rss = self._proc.memory_info().rss / 1e6
                self.samples.append(rss)
                if rss > self.peak_mb:
                    self.peak_mb = rss
            except psutil.NoSuchProcess:
                break
            time.sleep(self._interval)

    @property
    def delta_mb(self) -> float:
        return self.peak_mb - self.baseline_mb


# ---------------------------------------------------------------------------
# Result data class
# ---------------------------------------------------------------------------
@dataclass
class BenchResult:
    label: str
    row_count: int
    chunk_size: int
    file_size_mb: float
    duration_sec: float
    rows_loaded: int
    rows_failed: int
    throughput_rps: float       # rows per second through pipeline
    peak_ram_mb: float
    delta_ram_mb: float         # RAM added by this run
    db_size_mb: float
    error: Optional[str] = None

    def csv_row(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Single pipeline run
# ---------------------------------------------------------------------------
def run_pipeline_benchmark(
    csv_path: Path,
    db_path: Path,
    chunk_size: int,
    batch_size: int = 500,
    label: str = "",
    row_count: int = 0,
) -> BenchResult:
    """Run the pipeline against csv_path and return benchmark metrics."""
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)

    monitor = MemoryMonitor()
    file_size_mb = csv_path.stat().st_size / 1e6

    t_start = time.perf_counter()
    monitor.start()

    total_ingested = 0
    total_failed = 0
    error_msg = None

    try:
        from sqlalchemy.orm import Session

        ingester = CSVIngester(csv_path)
        metrics = PipelineMetrics(source=str(csv_path))

        with Session(engine) as session:
            transformer = DataTransformer(session, metrics)
            loader = DBLoader(session, batch_size=batch_size)

            for chunk in ingester.ingest_chunks(chunk_size=chunk_size):
                cleaned = DataCleaner.clean_records(chunk)
                # Route: orders pipeline (full transform)
                customer_map = loader.get_customer_map()
                orders = transformer.transform_orders(cleaned, customer_map)
                loader.load_orders(orders)
                session.commit()

                total_ingested += metrics.rows_ingested
                total_failed += metrics.rows_failed
                # Reset per-chunk metrics
                metrics.rows_ingested = 0
                metrics.rows_failed = 0

    except MemoryError as exc:
        error_msg = f"OOM: {exc}"
    except Exception as exc:
        error_msg = str(exc)[:200]
    finally:
        monitor.stop()

    duration = time.perf_counter() - t_start
    engine.dispose()
    db_size_mb = db_path.stat().st_size / 1e6 if db_path.exists() else 0.0
    throughput = total_ingested / duration if duration > 0 else 0

    return BenchResult(
        label=label,
        row_count=row_count,
        chunk_size=chunk_size,
        file_size_mb=round(file_size_mb, 2),
        duration_sec=round(duration, 2),
        rows_loaded=total_ingested,
        rows_failed=total_failed,
        throughput_rps=round(throughput, 0),
        peak_ram_mb=round(monitor.peak_mb, 1),
        delta_ram_mb=round(monitor.delta_ram_mb, 1),
        db_size_mb=round(db_size_mb, 2),
        error=error_msg,
    )


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------
def print_header() -> None:
    bar = "═" * 110
    print(f"\n╔{bar}╗")
    print(f"║{'PIPELINE SCALE TEST — 4GB RAM BUDGET':^110}║")
    print(f"╚{bar}╝\n")


def print_result(r: BenchResult) -> None:
    status = "✓" if not r.error else "✗"
    print(
        f"  {status} {r.label:<8} chunk={r.chunk_size:<6} "
        f"file={r.file_size_mb:>7.1f}MB  "
        f"dur={r.duration_sec:>7.1f}s  "
        f"rows={r.rows_loaded:>9,}  "
        f"RPS={r.throughput_rps:>9,.0f}  "
        f"RAM_peak={r.peak_ram_mb:>7.1f}MB  "
        f"ΔRAM={r.delta_ram_mb:>6.1f}MB  "
        f"DB={r.db_size_mb:>6.1f}MB"
        + (f"  [ERR: {r.error[:40]}]" if r.error else "")
    )


def print_summary(results: list[BenchResult]) -> None:
    good = [r for r in results if not r.error]
    if not good:
        print("\n[!] All runs failed or errored.")
        return

    best_rps = max(good, key=lambda r: r.throughput_rps)
    best_mem = min(good, key=lambda r: r.delta_ram_mb)
    biggest = max(good, key=lambda r: r.row_count)

    print("\n" + "─" * 110)
    print(f"  SUMMARY")
    print("─" * 110)
    print(f"  Total runs:           {len(results)}")
    print(f"  Successful runs:      {len(good)}")
    print(f"  Failed runs:          {len(results) - len(good)}")
    print()
    print(f"  Best throughput:      {best_rps.throughput_rps:>12,.0f} rows/sec  "
          f"({best_rps.label}, chunk={best_rps.chunk_size})")
    print(f"  Most memory-eff.:     ΔRAM={best_mem.delta_ram_mb:>6.1f}MB  "
          f"({best_mem.label}, chunk={best_mem.chunk_size})")
    print(f"  Largest dataset run:  {biggest.row_count:>12,} rows  ({biggest.label})")
    print(f"  Peak RAM observed:    {max(r.peak_ram_mb for r in good):>7.1f} MB")
    print(f"  Max DB size:          {max(r.db_size_mb for r in good):>7.1f} MB")
    print()

    # Throughput table
    print("  Throughput by dataset size and chunk size:")
    header = f"  {'Dataset':<10}" + "".join(f"  chunk={cs:<6}" for cs in CHUNK_SIZES_TO_TEST)
    print(header)
    print("  " + "-" * (len(header) - 2))
    by_label = {}
    for r in good:
        by_label.setdefault(r.label, {})[r.chunk_size] = r.throughput_rps
    for label, _ in ROW_TIERS:
        if label not in by_label:
            continue
        row = f"  {label:<10}"
        for cs in CHUNK_SIZES_TO_TEST:
            val = by_label[label].get(cs)
            row += f"  {val:>12,.0f}" if val else f"  {'—':>12}"
        print(row)
    print("─" * 110)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline scale test")
    parser.add_argument("--max-rows", type=int, default=2_000_000)
    parser.add_argument("--ram-limit-gb", type=float, default=3.5,
                        help="Stop generating new tiers if available RAM < this")
    parser.add_argument("--quick", action="store_true",
                        help="Only run 10K / 100K / 500K")
    parser.add_argument("--chunk-sizes", default="500,1000,5000",
                        help="Comma-separated chunk sizes to test")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    chunk_sizes = [int(x) for x in args.chunk_sizes.split(",")]
    ram_limit_bytes = args.ram_limit_gb * 1e9

    SCALE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print_header()
    avail_gb = psutil.virtual_memory().available / 1e9
    total_gb = psutil.virtual_memory().total / 1e9
    print(f"  System RAM:  {total_gb:.1f} GB total  |  {avail_gb:.1f} GB available")
    print(f"  Safety stop: when available RAM < {args.ram_limit_gb:.1f} GB")
    print(f"  Chunk sizes: {chunk_sizes}")
    print(f"  Max rows:    {args.max_rows:,}")
    print()

    results: list[BenchResult] = []
    generator_script = SCRIPT_DIR / "generate_dataset.py"
    python = str(PROJECT_DIR / ".venv" / "bin" / "python")

    tiers = [(lbl, n) for lbl, n in ROW_TIERS
             if n <= args.max_rows and (not args.quick or n <= 500_000)]

    for tier_label, row_count in tiers:
        # Safety check
        avail_now = psutil.virtual_memory().available / 1e9
        if avail_now < args.ram_limit_gb:
            print(f"\n  ⚠ Available RAM dropped to {avail_now:.2f} GB < {args.ram_limit_gb} GB — stopping.")
            break

        csv_path = SCALE_DATA_DIR / f"orders_{tier_label}.csv"

        # Generate dataset if not cached
        if not csv_path.exists():
            print(f"\n▶ Generating {tier_label} rows ({row_count:,})…")
            t0 = time.perf_counter()
            result = subprocess.run(
                [python, str(generator_script),
                 "--rows", str(row_count),
                 "--out", str(csv_path),
                 "--type", "orders"],
                capture_output=True, text=True
            )
            elapsed = time.perf_counter() - t0
            size_mb = csv_path.stat().st_size / 1e6 if csv_path.exists() else 0
            if result.returncode != 0:
                print(f"  [ERROR] Generator failed: {result.stderr[:100]}")
                continue
            print(f"  Generated in {elapsed:.1f}s  |  {size_mb:.0f} MB")
        else:
            size_mb = csv_path.stat().st_size / 1e6
            print(f"\n▶ Using cached {tier_label} dataset ({size_mb:.0f} MB)")

        # Run benchmark for each chunk size
        print(f"  {'Label':<10} {'Chunk':>8} {'File MB':>9} {'Duration':>10} "
              f"{'Rows':>12} {'RPS':>11} {'Peak MB':>10} {'ΔRAM MB':>9} {'DB MB':>8}")
        print("  " + "-" * 100)

        for chunk_size in chunk_sizes:
            gc.collect()

            # Fresh SQLite DB per run to avoid data skew
            db_path = SCALE_DATA_DIR / f"bench_{tier_label}_chunk{chunk_size}.db"
            db_path.unlink(missing_ok=True)

            r = run_pipeline_benchmark(
                csv_path=csv_path,
                db_path=db_path,
                chunk_size=chunk_size,
                batch_size=args.batch_size,
                label=tier_label,
                row_count=row_count,
            )
            results.append(r)
            print_result(r)

            # Check RAM after run
            avail_after = psutil.virtual_memory().available / 1e9
            if avail_after < args.ram_limit_gb:
                print(f"\n  ⚠ RAM exhaustion risk — {avail_after:.2f} GB available. Stopping tier.")
                break

    # Save results as JSON
    results_data = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "system": {
            "total_ram_gb": round(psutil.virtual_memory().total / 1e9, 1),
            "cpu_count": psutil.cpu_count(logical=False),
            "cpu_count_logical": psutil.cpu_count(logical=True),
        },
        "config": {
            "chunk_sizes": chunk_sizes,
            "batch_size": args.batch_size,
            "ram_limit_gb": args.ram_limit_gb,
        },
        "results": [asdict(r) for r in results],
    }
    RESULTS_FILE.write_text(json.dumps(results_data, indent=2))

    print_summary(results)
    print(f"\n  Results saved → {RESULTS_FILE}")


if __name__ == "__main__":
    main()
