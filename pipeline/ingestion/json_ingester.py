"""JSON / NDJSON file ingester — streaming chunk support."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

from pipeline.ingestion.base_ingester import BaseIngester
from pipeline.utils.logger import get_logger

log = get_logger(__name__)


class JSONIngester(BaseIngester):
    """
    Reads a JSON or NDJSON (newline-delimited JSON) file.

    - JSON array / object → ingest() loads fully (arrays can also stream chunks).
    - NDJSON → ingest_chunks() streams line-by-line, never loading the full file.

    Parameters
    ----------
    file_path:
        Path to the JSON / NDJSON file.
    ndjson:
        Force NDJSON mode even if the file extension is not ``.ndjson``.
    encoding:
        File encoding (default: utf-8).
    """

    def __init__(
        self,
        file_path: str | Path,
        ndjson: bool = False,
        encoding: str = "utf-8",
    ) -> None:
        self.file_path = Path(file_path)
        self.encoding = encoding
        self._ndjson = ndjson or self.file_path.suffix.lower() == ".ndjson"

    # ------------------------------------------------------------------
    # Full load
    # ------------------------------------------------------------------
    def ingest(self) -> list[dict]:
        """Return all records as a list."""
        records: list[dict] = []
        for chunk in self.ingest_chunks():
            records.extend(chunk)
        log.info("json_ingested", path=str(self.file_path), records=len(records))
        return records

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------
    def ingest_chunks(self, chunk_size: int = 1000) -> Generator[list[dict], None, None]:
        """
        Yield chunks of records.

        For NDJSON, streams line by line — O(chunk_size) memory.
        For regular JSON arrays, loads the array fully then chunks it.
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.file_path}")

        if self._ndjson:
            yield from self._stream_ndjson(chunk_size)
        else:
            yield from self._stream_json_array(chunk_size)

    def _stream_ndjson(self, chunk_size: int) -> Generator[list[dict], None, None]:
        chunk: list[dict] = []
        with self.file_path.open(encoding=self.encoding) as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    log.warning("ndjson_skip_malformed", lineno=lineno, error=str(exc))
                    continue
                if not isinstance(obj, dict):
                    log.warning("ndjson_skip_non_dict", lineno=lineno, got=type(obj).__name__)
                    continue
                chunk.append(obj)
                if chunk_size > 0 and len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
        if chunk:
            yield chunk

    def _stream_json_array(self, chunk_size: int) -> Generator[list[dict], None, None]:
        with self.file_path.open(encoding=self.encoding) as fh:
            data = json.load(fh)

        if isinstance(data, list):
            records = [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            records = [data]
        else:
            raise ValueError(f"Expected JSON array or object, got {type(data).__name__}")

        if chunk_size <= 0:
            yield records
            return

        for i in range(0, len(records), chunk_size):
            yield records[i : i + chunk_size]
