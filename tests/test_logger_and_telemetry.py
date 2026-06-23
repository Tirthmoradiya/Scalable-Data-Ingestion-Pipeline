"""Tests for logger.py and telemetry.py utilities."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from pipeline.utils.logger import (
    bind_run_context,
    clear_run_context,
    configure_logging,
    get_logger,
)


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------
class TestConfigureLogging:
    def test_configure_logging_json_format(self) -> None:
        """Should complete without error for json format."""
        configure_logging(log_level="DEBUG", log_format="json")

    def test_configure_logging_console_format(self) -> None:
        """Should complete without error for console format."""
        configure_logging(log_level="INFO", log_format="console")

    def test_configure_logging_sets_root_level(self) -> None:
        configure_logging(log_level="WARNING", log_format="console")
        root = logging.getLogger()
        assert root.level == logging.WARNING

    def test_configure_logging_info_level(self) -> None:
        configure_logging(log_level="INFO", log_format="console")
        root = logging.getLogger()
        assert root.level == logging.INFO

    def test_configure_logging_unknown_level_defaults_to_info(self) -> None:
        """Unknown level string should fall back to INFO (via getattr default)."""
        configure_logging(log_level="NOTAREALLEVEL", log_format="console")
        root = logging.getLogger()
        assert root.level == logging.INFO

    def test_root_logger_has_handler_after_configure(self) -> None:
        configure_logging(log_level="INFO", log_format="console")
        root = logging.getLogger()
        assert len(root.handlers) >= 1


# ---------------------------------------------------------------------------
# get_logger
# ---------------------------------------------------------------------------
class TestGetLogger:
    def test_get_logger_returns_logger(self) -> None:
        logger = get_logger("test.module")
        assert logger is not None
        # After configure_logging() the proxy is fully bound;
        # before it, structlog returns a BoundLoggerLazyProxy — both are valid.
        assert hasattr(logger, "info")

    def test_get_logger_different_names(self) -> None:
        log_a = get_logger("module.a")
        log_b = get_logger("module.b")
        # Both are valid loggers (structlog returns bound loggers regardless of name)
        assert log_a is not None
        assert log_b is not None


# ---------------------------------------------------------------------------
# bind / clear context
# ---------------------------------------------------------------------------
class TestRunContext:
    def test_bind_and_clear_run_context(self) -> None:
        """Should not raise."""
        bind_run_context(run_id="test-run-id", source="test_source")
        clear_run_context()

    def test_clear_context_is_idempotent(self) -> None:
        clear_run_context()
        clear_run_context()  # calling twice should not raise


# ---------------------------------------------------------------------------
# telemetry
# ---------------------------------------------------------------------------
from pipeline.utils.telemetry import (  # noqa: E402
    ACTIVE_RUNS,
    BATCH_DURATION,
    ROWS_FAILED,
    ROWS_INGESTED,
    RunTimer,
    _get_tracer,
    configure_tracing,
    start_metrics_server,
    trace_span,
)


class TestPrometheusCounters:
    def test_rows_ingested_counter_increments(self) -> None:
        before = ROWS_INGESTED.labels(source="test_logger", entity="orders")._value.get()
        ROWS_INGESTED.labels(source="test_logger", entity="orders").inc(5)
        after = ROWS_INGESTED.labels(source="test_logger", entity="orders")._value.get()
        assert after - before == 5

    def test_rows_failed_counter_increments(self) -> None:
        before = ROWS_FAILED.labels(source="test_logger", reason_category="validation")._value.get()
        ROWS_FAILED.labels(source="test_logger", reason_category="validation").inc(3)
        after = ROWS_FAILED.labels(source="test_logger", reason_category="validation")._value.get()
        assert after - before == 3

    def test_batch_duration_histogram_observe(self) -> None:
        # Should not raise
        BATCH_DURATION.labels(source="test_logger").observe(0.5)


class TestConfigureTracing:
    def setup_method(self) -> None:
        # Reset the global tracer so configure_tracing can run fresh each test
        import pipeline.utils.telemetry as tel

        tel._tracer = None

    def test_configure_tracing_disabled_does_not_raise(self) -> None:
        configure_tracing(enabled=False)

    def test_configure_tracing_disabled_sets_tracer(self) -> None:
        import pipeline.utils.telemetry as tel

        configure_tracing(enabled=False)
        assert tel._tracer is not None

    def test_configure_tracing_is_idempotent(self) -> None:
        """Calling twice should be a no-op (guard on _tracer is not None)."""
        configure_tracing(enabled=False)
        import pipeline.utils.telemetry as tel

        first_tracer = tel._tracer
        configure_tracing(enabled=False)
        assert tel._tracer is first_tracer  # same object, not replaced

    def test_configure_tracing_enabled_path_graceful(self) -> None:
        """Enabled path may fail to connect but should not raise — falls back to no-op."""
        configure_tracing(endpoint="grpc://localhost:9999", enabled=True)

    def teardown_method(self) -> None:
        import pipeline.utils.telemetry as tel

        tel._tracer = None


class TestGetTracer:
    def setup_method(self) -> None:
        import pipeline.utils.telemetry as tel

        tel._tracer = None

    def test_get_tracer_lazy_initialises(self) -> None:
        import pipeline.utils.telemetry as tel

        assert tel._tracer is None
        tracer = _get_tracer()
        assert tracer is not None
        assert tel._tracer is not None

    def teardown_method(self) -> None:
        import pipeline.utils.telemetry as tel

        tel._tracer = None


class TestTraceSpan:
    def setup_method(self) -> None:
        import pipeline.utils.telemetry as tel

        tel._tracer = None

    def test_trace_span_yields_span(self) -> None:
        with trace_span("test_operation", key="value") as span:
            assert span is not None

    def test_trace_span_with_no_attributes(self) -> None:
        with trace_span("empty_span") as span:
            assert span is not None

    def test_trace_span_records_exception(self) -> None:
        with pytest.raises(ValueError, match="test error"), trace_span("failing_span"):
            raise ValueError("test error")

    def test_trace_span_multiple_attributes(self) -> None:
        with trace_span("multi_attr", a=1, b="hello", c=True) as span:
            assert span is not None

    def teardown_method(self) -> None:
        import pipeline.utils.telemetry as tel

        tel._tracer = None


class TestRunTimer:
    def test_run_timer_context_manager_success(self) -> None:
        before = ACTIVE_RUNS._value.get()
        with RunTimer(source="test_timer"):
            during = ACTIVE_RUNS._value.get()
            assert during == before + 1
        after = ACTIVE_RUNS._value.get()
        assert after == before

    def test_run_timer_context_manager_on_exception(self) -> None:
        before = ACTIVE_RUNS._value.get()
        with pytest.raises(RuntimeError), RunTimer(source="test_timer_err"):
            raise RuntimeError("fail")
        after = ACTIVE_RUNS._value.get()
        assert after == before  # active count still decremented

    def test_run_timer_returns_self(self) -> None:
        timer = RunTimer(source="test_timer_self")
        result = timer.__enter__()
        assert result is timer
        timer.__exit__(None, None, None)


class TestStartMetricsServer:
    def test_start_metrics_server_calls_start_http_server(self) -> None:
        with patch("pipeline.utils.telemetry.start_http_server") as mock_start:
            start_metrics_server(port=19090)
            mock_start.assert_called_once_with(19090)
