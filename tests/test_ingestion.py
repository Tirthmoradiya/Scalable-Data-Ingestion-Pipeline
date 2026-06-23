"""Unit tests for CSV, JSON, and API ingesters."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.ingestion.json_ingester import JSONIngester
from pipeline.ingestion.api_ingester import APIIngester


# ---------------------------------------------------------------------------
# CSVIngester
# ---------------------------------------------------------------------------
class TestCSVIngester:
    def test_reads_all_rows(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,email\nAlice,alice@x.com\nBob,bob@x.com\n")
        records = CSVIngester(csv_file).ingest()
        assert len(records) == 2

    def test_returns_dicts_with_correct_keys(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,value\n1,foo\n")
        records = CSVIngester(csv_file).ingest()
        assert records[0] == {"id": "1", "value": "foo"}

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            CSVIngester(tmp_path / "missing.csv").ingest()

    def test_empty_file_returns_empty_list(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("name,email\n")  # headers only
        records = CSVIngester(csv_file).ingest()
        assert records == []

    def test_custom_delimiter(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "pipe.csv"
        csv_file.write_text("name|email\nAlice|alice@x.com\n")
        records = CSVIngester(csv_file, delimiter="|").ingest()
        assert records[0]["name"] == "Alice"

    def test_utf8_encoding(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "utf8.csv"
        csv_file.write_bytes("name,city\nClàudia,Zürich\n".encode("utf-8"))
        records = CSVIngester(csv_file, encoding="utf-8").ingest()
        assert records[0]["name"] == "Clàudia"


# ---------------------------------------------------------------------------
# JSONIngester
# ---------------------------------------------------------------------------
class TestJSONIngester:
    def test_reads_json_array(self, tmp_path: Path) -> None:
        f = tmp_path / "data.json"
        f.write_text(json.dumps([{"id": 1}, {"id": 2}]))
        records = JSONIngester(f).ingest()
        assert len(records) == 2

    def test_wraps_single_object(self, tmp_path: Path) -> None:
        f = tmp_path / "single.json"
        f.write_text(json.dumps({"id": 1, "name": "test"}))
        records = JSONIngester(f).ingest()
        assert len(records) == 1
        assert records[0]["id"] == 1

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            JSONIngester(tmp_path / "missing.json").ingest()

    def test_ndjson_reads_valid_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "data.ndjson"
        f.write_text('{"a": 1}\n{"b": 2}\n')
        records = JSONIngester(f).ingest()
        assert len(records) == 2

    def test_ndjson_skips_malformed_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.ndjson"
        f.write_text('{"a": 1}\nTHIS IS INVALID\n{"b": 2}\n')
        records = JSONIngester(f).ingest()
        assert len(records) == 2  # malformed line skipped

    def test_ndjson_skips_empty_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "gaps.ndjson"
        f.write_text('{"a": 1}\n\n{"b": 2}\n')
        records = JSONIngester(f).ingest()
        assert len(records) == 2

    def test_invalid_top_level_type_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "scalar.json"
        f.write_text('"just a string"')
        with pytest.raises(ValueError, match="Expected JSON array or object"):
            JSONIngester(f).ingest()

    def test_auto_detect_ndjson_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "events.ndjson"
        f.write_text('{"x": 1}\n{"x": 2}\n')
        records = JSONIngester(f).ingest()
        assert len(records) == 2


# ---------------------------------------------------------------------------
# APIIngester
# ---------------------------------------------------------------------------
class TestAPIIngester:
    def test_single_page(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": [{"id": 1}], "next": None}
        mock_resp.raise_for_status.return_value = None

        with patch("httpx.Client.get", return_value=mock_resp):
            records = APIIngester("https://api.example.com/data").ingest()

        assert len(records) == 1

    def test_pagination_follows_next(self) -> None:
        page1 = MagicMock()
        page1.json.return_value = {
            "results": [{"id": 1}, {"id": 2}],
            "next": "https://api.example.com/data?page=2",
        }
        page1.raise_for_status.return_value = None

        page2 = MagicMock()
        page2.json.return_value = {"results": [{"id": 3}], "next": None}
        page2.raise_for_status.return_value = None

        with patch(
            "httpx.Client.get",
            side_effect=[page1, page2],
        ):
            records = APIIngester("https://api.example.com/data").ingest()

        assert len(records) == 3

    def test_list_response(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"id": 1}, {"id": 2}]
        mock_resp.raise_for_status.return_value = None

        with patch("httpx.Client.get", return_value=mock_resp):
            records = APIIngester(
                "https://api.example.com/data", data_key=None
            ).ingest()

        assert len(records) == 2


# ---------------------------------------------------------------------------
# Streaming / chunking tests
# ---------------------------------------------------------------------------
class TestCSVIngesterChunks:
    def test_ingest_chunks_yields_correct_sizes(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        rows = ["name,email"] + [f"User{i},u{i}@x.com" for i in range(10)]
        csv_file.write_text("\n".join(rows))
        chunks = list(CSVIngester(csv_file).ingest_chunks(chunk_size=3))
        assert len(chunks) == 4
        assert len(chunks[0]) == 3
        assert len(chunks[-1]) == 1

    def test_ingest_chunks_total_rows_match(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        rows = ["name,email"] + [f"User{i},u{i}@x.com" for i in range(25)]
        csv_file.write_text("\n".join(rows))
        total = sum(len(c) for c in CSVIngester(csv_file).ingest_chunks(chunk_size=7))
        assert total == 25

    def test_ingest_chunks_missing_file_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            list(CSVIngester(tmp_path / "nope.csv").ingest_chunks())

    def test_ingest_chunks_zero_chunk_returns_all(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        chunks = list(CSVIngester(csv_file).ingest_chunks(chunk_size=0))
        assert len(chunks) == 1
        assert len(chunks[0]) == 2

    def test_bom_stripped_from_utf8_sig(self, tmp_path) -> None:
        csv_file = tmp_path / "bom.csv"
        csv_file.write_bytes("name,email\nAlice,alice@x.com\n".encode("utf-8-sig"))
        records = CSVIngester(csv_file, encoding="utf-8-sig").ingest()
        assert "name" in records[0]
        assert records[0]["name"] == "Alice"


class TestJSONIngesterChunks:
    def test_ndjson_chunks_correct_sizes(self, tmp_path) -> None:
        f = tmp_path / "data.ndjson"
        f.write_text("\n".join(f'{{"id": {i}}}' for i in range(10)))
        chunks = list(JSONIngester(f).ingest_chunks(chunk_size=3))
        assert sum(len(c) for c in chunks) == 10

    def test_json_array_chunks(self, tmp_path) -> None:
        import json as _json
        f = tmp_path / "data.json"
        f.write_text(_json.dumps([{"id": i} for i in range(7)]))
        chunks = list(JSONIngester(f).ingest_chunks(chunk_size=3))
        assert sum(len(c) for c in chunks) == 7

    def test_ndjson_chunks_skip_malformed(self, tmp_path) -> None:
        f = tmp_path / "mixed.ndjson"
        f.write_text('{"a": 1}\nBAD LINE\n{"b": 2}\n{"c": 3}\n')
        chunks = list(JSONIngester(f).ingest_chunks(chunk_size=10))
        total = sum(len(c) for c in chunks)
        assert total == 3
