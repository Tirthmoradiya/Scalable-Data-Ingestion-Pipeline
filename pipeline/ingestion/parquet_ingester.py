"""
Parquet file ingester — streaming row-group support.

Requires ``pyarrow`` (installed via ``pip install pyarrow``).

Features
--------
- Full-load via ``ingest()``
- Streaming via ``ingest_chunks(chunk_size=N)`` — reads one pandas chunk at a time
- Column projection: only load the columns you need
- Predicate information exposed via Arrow row-group metadata

Usage
-----
    ingester = ParquetIngester("data/orders.parquet")
    for chunk in ingester.ingest_chunks(chunk_size=5000):
        process(chunk)
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

from pipeline.ingestion.base_ingester import BaseIngester
from pipeline.utils.logger import get_logger

log = get_logger(__name__)


class ParquetIngester(BaseIngester):
    """
    Reads a Parquet file and yields rows as plain dicts.

    Parameters
    ----------
    file_path:
        Path to the ``.parquet`` file (or directory of partitioned Parquet files).
    columns:
        List of column names to load. ``None`` loads all columns.
    """

    def __init__(
        self,
        file_path: str | Path,
        columns: list[str] | None = None,
    ) -> None:
        self.file_path = Path(file_path)
        self.columns = columns

    def ingest(self) -> list[dict[str, Any]]:
        """Load all rows into memory and return as a flat list."""
        records: list[dict[str, Any]] = []
        for chunk in self.ingest_chunks(chunk_size=0):
            records.extend(chunk)
        log.info("parquet_ingested", path=str(self.file_path), rows=len(records))
        return records

    def ingest_chunks(
        self, chunk_size: int = 10_000
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Yield successive chunks of rows.

        If ``chunk_size <= 0``, the entire file is yielded as one chunk.

        Requires ``pyarrow``.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError("ParquetIngester requires pyarrow: pip install pyarrow") from exc

        if not self.file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.file_path}")

        pq_any: Any = pq
        pf: Any = pq_any.ParquetFile(self.file_path)
        total_rows = pf.metadata.num_rows
        log.info(
            "parquet_open",
            path=str(self.file_path),
            rows=total_rows,
            row_groups=pf.metadata.num_row_groups,
        )

        if chunk_size <= 0:
            table = pf.read(columns=self.columns)
            # Use pandas/arrow for consistent dict-of-lists -> list-of-dicts conversion
            yield _table_to_records(table)
            return

        batch_size = max(chunk_size, 1)
        for batch in pf.iter_batches(batch_size=batch_size, columns=self.columns):
            import pyarrow as pa

            pa_any: Any = pa
            records = pa_any.Table.from_batches([batch]).to_pylist()
            if records:
                log.debug("parquet_chunk", rows=len(records))
                yield records


def _table_to_records(table: Any) -> list[dict[str, Any]]:
    """Convert a PyArrow Table to a list of dicts."""
    return cast(list[dict[str, Any]], table.to_pylist())
