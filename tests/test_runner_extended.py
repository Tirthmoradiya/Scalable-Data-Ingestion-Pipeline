"""Additional runner tests targeting uncovered branches."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.runner import PipelineRunner, RunResult


def sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path}/runner_ext.db"


# ---------------------------------------------------------------------------
# RunResult helpers
# ---------------------------------------------------------------------------
class TestRunResultSummary:
    def test_summary_in_progress_when_no_finished_at(self) -> None:
        result = RunResult(run_id="abcd1234-xxxx", source="test")
        result.finished_at = None
        summary = result.summary()
        assert "[in progress]" in summary
        assert "abcd1234" in summary

    def test_summary_with_finished_at(self) -> None:
        from datetime import timedelta

        result = RunResult(run_id="abcd1234-xxxx", source="test")
        result.finished_at = result.started_at + timedelta(seconds=3)
        summary = result.summary()
        assert "3.00s" in summary
        assert "abcd1234" in summary


# ---------------------------------------------------------------------------
# Unknown entity type (the else branch in _process_chunk)
# ---------------------------------------------------------------------------
class TestRunnerUnknownEntityType:
    def test_unknown_entity_type_logs_warning_and_completes(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,email\nAlice,alice@x.com\n")

        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        result = runner.run(
            ingester=CSVIngester(csv_file),
            entity_type="unknown_entity",
            max_workers=1,
        )
        assert result.finished_at is not None

    def test_orders_entity_type_completes(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "customer_email,status,total_amount,ordered_at\n"
            "alice@x.com,completed,99.99,2024-01-01\n"
        )
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        result = runner.run(
            ingester=CSVIngester(csv_file),
            entity_type="orders",
            max_workers=1,
        )
        assert result.finished_at is not None

    def test_categories_entity_type_completes(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "cats.csv"
        csv_file.write_text("name\nelectronics\nbooks\n")
        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        result = runner.run(
            ingester=CSVIngester(csv_file),
            entity_type="categories",
            max_workers=1,
        )
        assert result.finished_at is not None


# ---------------------------------------------------------------------------
# _process_all_chunks fallback (ingester without ingest_chunks)
# ---------------------------------------------------------------------------
class TestRunnerFallbackIngest:
    def test_fallback_to_ingest_when_no_ingest_chunks(self, tmp_path: Path) -> None:
        """Ingester that only has ingest() (not ingest_chunks) should still work."""

        class MinimalIngester:
            def ingest(self):
                return [{"name": "Fallback", "email": "f@x.com"}]

        runner = PipelineRunner(db_url=sqlite_url(tmp_path))
        result = runner.run(
            ingester=MinimalIngester(),  # type: ignore[arg-type]
            entity_type="customers",
            max_workers=1,
        )
        assert result.finished_at is not None


# ---------------------------------------------------------------------------
# _flush_with_retry — failure path
# ---------------------------------------------------------------------------
class TestFlushWithRetry:
    def test_flush_retry_raises_after_all_attempts(self, tmp_path: Path) -> None:
        """If commit always fails, _flush_with_retry should eventually raise."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        from pipeline.models import Base

        engine = create_engine(f"sqlite:///{tmp_path}/retry.db")
        Base.metadata.create_all(engine)

        runner = PipelineRunner(db_url=f"sqlite:///{tmp_path}/retry.db")
        # Override settings to only 1 attempt so the test is fast
        runner._settings.pipeline.retry_max_attempts = 1
        runner._settings.pipeline.retry_backoff_factor = 0.0

        with (
            Session(engine) as session,
            patch.object(session, "commit", side_effect=RuntimeError("db error")),
            pytest.raises(RuntimeError, match="db error"),
        ):
            runner._flush_with_retry(session)

    def test_flush_retries_then_succeeds(self, tmp_path: Path) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        from pipeline.models import Base

        engine = create_engine(f"sqlite:///{tmp_path}/retry2.db")
        Base.metadata.create_all(engine)

        runner = PipelineRunner(db_url=f"sqlite:///{tmp_path}/retry2.db")
        runner._settings.pipeline.retry_max_attempts = 3
        runner._settings.pipeline.retry_backoff_factor = 0.0

        call_count = 0

        def flaky_commit():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("transient")

        with (
            Session(engine) as session,
            patch.object(session, "commit", side_effect=flaky_commit),
            patch("time.sleep"),
        ):
            runner._flush_with_retry(session)  # should not raise

        assert call_count == 2


# ---------------------------------------------------------------------------
# _save_run failure is swallowed (line 296-297)
# ---------------------------------------------------------------------------
class TestSaveRunFailure:
    def test_save_run_error_does_not_propagate(self, tmp_path: Path) -> None:
        from pipeline.utils.metrics import PipelineMetrics

        runner = PipelineRunner(db_url=sqlite_url(tmp_path))

        # Simulate DBLoader.save_pipeline_run raising
        with patch("pipeline.runner.DBLoader") as mock_loader:
            mock_loader.return_value.save_pipeline_run.side_effect = RuntimeError("db fail")
            metrics = PipelineMetrics(source="test")
            # Should not raise — error is logged and swallowed
            runner._save_run(metrics)
