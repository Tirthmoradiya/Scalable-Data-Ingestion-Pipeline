"""CSV file ingester — streaming chunk support."""
from __future__ import annotations

import csv
from collections.abc import Generator
from pathlib import Path

from pipeline.ingestion.base_ingester import BaseIngester
from pipeline.utils.logger import get_logger

log = get_logger(__name__)


class CSVIngester(BaseIngester):
    """
    Reads a CSV file and returns rows as dicts.

    Supports both full-load (``ingest()``) and streaming chunks
    (``ingest_chunks()``) for memory-efficient processing of large files.

    Parameters
    ----------
    file_path:
        Absolute or relative path to the CSV file.
    encoding:
        File encoding (default: utf-8-sig to handle BOM).
    delimiter:
        CSV delimiter character (default: comma).
    """

    def __init__(
        self,
        file_path: str | Path,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
    ) -> None:
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.delimiter = delimiter

    def ingest(self) -> list[dict]:
        """Load all rows into memory and return as a flat list."""
        records: list[dict] = []
        for chunk in self.ingest_chunks(chunk_size=0):
            records.extend(chunk)
        log.info("csv_ingested", path=str(self.file_path), rows=len(records))
        return records

    def ingest_chunks(self, chunk_size: int = 1000) -> Generator[list[dict], None, None]:
        """
        Yield successive chunks of rows.

        If ``chunk_size <= 0``, yields all rows in a single chunk.
        Never loads the entire file into memory at once.
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        with self.file_path.open(encoding=self.encoding, newline="") as fh:
            reader = csv.DictReader(fh, delimiter=self.delimiter)

            if chunk_size <= 0:
                # Return everything at once
                rows = [dict(row) for row in reader]
                log.debug("csv_chunk", path=str(self.file_path), rows=len(rows))
                yield rows
                return

            chunk: list[dict] = []
            total = 0
            for row in reader:
                chunk.append(dict(row))
                if len(chunk) >= chunk_size:
                    total += len(chunk)
                    log.debug(
                        "csv_chunk",
                        path=str(self.file_path),
                        chunk_rows=len(chunk),
                        total_so_far=total,
                    )
                    yield chunk
                    chunk = []

            if chunk:
                total += len(chunk)
                log.debug("csv_chunk_final", path=str(self.file_path), rows=len(chunk))
                yield chunk

        log.info("csv_stream_complete", path=str(self.file_path), total_rows=total if chunk_size > 0 else None)
