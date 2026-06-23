"""Tests for ParquetIngester — requires pyarrow (installed via dev extras)."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.ingestion.parquet_ingester import ParquetIngester, _table_to_records


def _make_parquet(tmp_path: Path, rows: list[dict], filename: str = "data.parquet") -> Path:
    """Write a minimal Parquet file using pyarrow and return its path."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({k: [r[k] for r in rows] for k in rows[0]})
    path = tmp_path / filename
    pq.write_table(table, path)
    return path


class TestParquetIngesterIngest:
    def test_ingest_returns_all_rows(self, tmp_path: Path) -> None:
        path = _make_parquet(tmp_path, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        records = ParquetIngester(path).ingest()
        assert len(records) == 2

    def test_ingest_returns_dicts(self, tmp_path: Path) -> None:
        path = _make_parquet(tmp_path, [{"x": 10, "y": 20}])
        records = ParquetIngester(path).ingest()
        assert records[0] == {"x": 10, "y": 20}

    def test_ingest_single_row(self, tmp_path: Path) -> None:
        path = _make_parquet(tmp_path, [{"val": 42}])
        records = ParquetIngester(path).ingest()
        assert len(records) == 1
        assert records[0]["val"] == 42

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        ingester = ParquetIngester(tmp_path / "missing.parquet")
        with pytest.raises(FileNotFoundError, match="Parquet file not found"):
            ingester.ingest()

    def test_file_not_found_in_chunks_raises(self, tmp_path: Path) -> None:
        ingester = ParquetIngester(tmp_path / "missing.parquet")
        with pytest.raises(FileNotFoundError):
            list(ingester.ingest_chunks())


class TestParquetIngesterChunks:
    def test_chunks_sum_to_total(self, tmp_path: Path) -> None:
        rows = [{"id": i, "val": i * 2} for i in range(20)]
        path = _make_parquet(tmp_path, rows)
        chunks = list(ParquetIngester(path).ingest_chunks(chunk_size=7))
        total = sum(len(c) for c in chunks)
        assert total == 20

    def test_chunk_size_respected(self, tmp_path: Path) -> None:
        rows = [{"id": i} for i in range(15)]
        path = _make_parquet(tmp_path, rows)
        chunks = list(ParquetIngester(path).ingest_chunks(chunk_size=5))
        assert all(len(c) <= 5 for c in chunks)

    def test_chunk_size_zero_returns_one_chunk(self, tmp_path: Path) -> None:
        rows = [{"id": i} for i in range(10)]
        path = _make_parquet(tmp_path, rows)
        chunks = list(ParquetIngester(path).ingest_chunks(chunk_size=0))
        # chunk_size=0 → whole file as one chunk
        assert sum(len(c) for c in chunks) == 10

    def test_chunks_are_list_of_dicts(self, tmp_path: Path) -> None:
        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        path = _make_parquet(tmp_path, rows)
        for chunk in ParquetIngester(path).ingest_chunks(chunk_size=1):
            assert isinstance(chunk, list)
            assert all(isinstance(r, dict) for r in chunk)


class TestParquetIngesterColumnProjection:
    def test_columns_filters_to_subset(self, tmp_path: Path) -> None:
        rows = [{"id": 1, "name": "Alice", "score": 99}]
        path = _make_parquet(tmp_path, rows)
        records = ParquetIngester(path, columns=["id", "name"]).ingest()
        assert "score" not in records[0]
        assert "id" in records[0]
        assert "name" in records[0]

    def test_single_column_projection(self, tmp_path: Path) -> None:
        rows = [{"x": 10, "y": 20, "z": 30}]
        path = _make_parquet(tmp_path, rows)
        records = ParquetIngester(path, columns=["z"]).ingest()
        assert records[0] == {"z": 30}


class TestTableToRecords:
    def test_converts_arrow_table_to_list_of_dicts(self) -> None:
        import pyarrow as pa

        table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        result = _table_to_records(table)
        assert result == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    def test_empty_table_returns_empty_list(self) -> None:
        import pyarrow as pa

        table = pa.table({"a": pa.array([], type=pa.int64())})
        result = _table_to_records(table)
        assert result == []
