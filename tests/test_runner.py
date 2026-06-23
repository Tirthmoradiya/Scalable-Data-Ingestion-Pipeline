"""
Tests for PipelineRunner — integration tests against file-based SQLite.
SQLite :memory: cannot be shared across threads, so we use tmp_path.
"""

from __future__ import annotations

from pathlib import Path

from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.ingestion.json_ingester import JSONIngester
from pipeline.runner import PipelineRunner

SAMPLE_DIR = Path(__file__).parent.parent / "data"


def sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path}/test_pipeline.db"


class TestPipelineRunnerCustomers:
    def test_run_returns_result(self, tmp_path: Path) -> None:
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        ingester = CSVIngester(SAMPLE_DIR / "sample_orders.csv")
        result = runner.run(ingester=ingester, entity_type="customers", max_workers=1)
        assert result.run_id is not None
        assert result.finished_at is not None

    def test_run_records_ingested_rows(self, tmp_path: Path) -> None:
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        ingester = CSVIngester(SAMPLE_DIR / "sample_orders.csv")
        result = runner.run(ingester=ingester, entity_type="customers", max_workers=1)
        assert result.rows_ingested >= 0
        assert result.rows_failed >= 0

    def test_run_creates_pipeline_run_record(self, tmp_path: Path) -> None:
        from sqlalchemy import create_engine, select
        from sqlalchemy.orm import Session

        from pipeline.models import PipelineRun

        url = sqlite_url(tmp_path)
        runner = PipelineRunner(db_url=url)
        ingester = CSVIngester(SAMPLE_DIR / "sample_orders.csv")
        runner.run(ingester=ingester, entity_type="customers", max_workers=1)

        eng = create_engine(url)
        with Session(eng) as sess:
            runs = sess.scalars(select(PipelineRun)).all()
            assert len(runs) >= 1
            assert runs[-1].source is not None


class TestPipelineRunnerProducts:
    def test_run_json_products(self, tmp_path: Path) -> None:
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        ingester = JSONIngester(SAMPLE_DIR / "sample_products.json")
        result = runner.run(ingester=ingester, entity_type="products", max_workers=1)
        assert result.finished_at is not None

    def test_run_ndjson_generic(self, tmp_path: Path) -> None:
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        ingester = JSONIngester(SAMPLE_DIR / "sample_events.ndjson")
        result = runner.run(ingester=ingester, entity_type="generic", max_workers=1)
        assert result.run_id is not None


class TestPipelineRunnerChunking:
    def test_small_chunk_size_produces_multiple_chunks(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "big.csv"
        lines = ["name,email"] + [f"User{i},u{i}@x.com" for i in range(10)]
        csv_file.write_text("\n".join(lines))

        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        ingester = CSVIngester(csv_file)
        result = runner.run(
            ingester=ingester,
            entity_type="customers",
            chunk_size=3,
            max_workers=1,  # SQLite doesn't support concurrent writes
        )
        assert result.chunks_processed >= 3

    def test_result_summary_contains_key_info(self, tmp_path: Path) -> None:
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        ingester = CSVIngester(SAMPLE_DIR / "sample_orders.csv")
        result = runner.run(ingester=ingester, entity_type="generic", max_workers=1)
        summary = result.summary()
        assert "RunResult" in summary
        assert result.run_id[:8] in summary
