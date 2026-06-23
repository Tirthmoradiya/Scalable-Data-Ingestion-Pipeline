"""
Tests for the retry decorator and DeadLetterWriter.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.utils.retry import DeadLetterWriter, with_retry


# ---------------------------------------------------------------------------
# with_retry decorator
# ---------------------------------------------------------------------------
class TestWithRetry:
    def test_succeeds_on_first_attempt(self) -> None:
        call_count = 0

        @with_retry(max_attempts=3)
        def fn() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        assert fn() == "ok"
        assert call_count == 1

    def test_retries_on_exception(self) -> None:
        call_count = 0

        @with_retry(max_attempts=3, backoff_factor=0.01)
        def fn() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "recovered"

        assert fn() == "recovered"
        assert call_count == 3

    def test_reraises_after_max_attempts(self) -> None:
        @with_retry(max_attempts=2, backoff_factor=0.01, reraise=True)
        def always_fails() -> None:
            raise RuntimeError("permanent error")

        with pytest.raises(RuntimeError, match="permanent error"):
            always_fails()

    def test_only_retries_specified_exceptions(self) -> None:
        call_count = 0

        @with_retry(max_attempts=3, backoff_factor=0.01, exceptions=(ValueError,))
        def fn() -> None:
            nonlocal call_count
            call_count += 1
            raise TypeError("wrong type — should not retry")

        with pytest.raises(TypeError):
            fn()
        assert call_count == 1  # no retries for TypeError


# ---------------------------------------------------------------------------
# DeadLetterWriter
# ---------------------------------------------------------------------------
class TestDeadLetterWriter:
    def test_creates_file_on_write(self, tmp_path: Path) -> None:
        dlq = DeadLetterWriter(run_id="test-run", dead_letter_dir=str(tmp_path))
        dlq.write(record={"email": "bad@x.com"}, reason="invalid email")
        dlq.close()
        assert dlq.path.exists()

    def test_each_entry_is_valid_json(self, tmp_path: Path) -> None:
        dlq = DeadLetterWriter(run_id="test-run", dead_letter_dir=str(tmp_path))
        dlq.write(record={"a": 1}, reason="reason A")
        dlq.write(record={"b": 2}, reason="reason B")
        dlq.close()

        lines = dlq.path.read_text().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            entry = json.loads(line)
            assert "ts" in entry
            assert "reason" in entry
            assert "record" in entry
            assert "run_id" in entry

    def test_count_tracks_writes(self, tmp_path: Path) -> None:
        dlq = DeadLetterWriter(run_id="r1", dead_letter_dir=str(tmp_path))
        for i in range(5):
            dlq.write(record={"i": i}, reason=f"err {i}")
        dlq.close()
        assert dlq.count == 5

    def test_context_manager(self, tmp_path: Path) -> None:
        with DeadLetterWriter(run_id="ctx-run", dead_letter_dir=str(tmp_path)) as dlq:
            dlq.write(record={}, reason="test")
        assert dlq.path.exists()

    def test_appends_to_existing_file(self, tmp_path: Path) -> None:
        """Two writers with the same run_id append to the same file."""
        dlq1 = DeadLetterWriter(run_id="same-run", dead_letter_dir=str(tmp_path))
        dlq1.write(record={}, reason="first")
        dlq1.close()

        dlq2 = DeadLetterWriter(run_id="same-run", dead_letter_dir=str(tmp_path))
        dlq2.write(record={}, reason="second")
        dlq2.close()

        lines = dlq2.path.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_creates_dead_letter_dir_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b" / "c"
        dlq = DeadLetterWriter(run_id="r", dead_letter_dir=str(nested))
        dlq.close()
        assert nested.exists()
