"""
Prometheus metrics + OpenTelemetry tracing for the pipeline.

Prometheus metrics
------------------
  pipeline_rows_ingested_total    — counter, labels: source, entity
  pipeline_rows_failed_total      — counter, labels: source, reason_category
  pipeline_batch_duration_seconds — histogram, labels: source
  pipeline_run_duration_seconds   — histogram, labels: source, status
  pipeline_active_runs            — gauge

OpenTelemetry tracing
---------------------
  Tracer name: ``pipeline``
  Call ``configure_tracing(endpoint, service_name)`` once at startup.
  Use ``trace_span(name, **attributes)`` as a context manager.

Start the metrics HTTP server:
    from pipeline.utils.telemetry import start_metrics_server
    start_metrics_server(port=9090)
"""
from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------------------
# Prometheus metrics definitions
# ---------------------------------------------------------------------------
ROWS_INGESTED = Counter(
    "pipeline_rows_ingested_total",
    "Total rows successfully ingested",
    ["source", "entity"],
)

ROWS_FAILED = Counter(
    "pipeline_rows_failed_total",
    "Total rows that failed validation or loading",
    ["source", "reason_category"],
)

BATCH_DURATION = Histogram(
    "pipeline_batch_duration_seconds",
    "Time to process a single batch",
    ["source"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
)

RUN_DURATION = Histogram(
    "pipeline_run_duration_seconds",
    "Total pipeline run duration",
    ["source", "status"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

ACTIVE_RUNS = Gauge(
    "pipeline_active_runs",
    "Number of pipeline runs currently in progress",
)

DB_POOL_SIZE = Gauge(
    "pipeline_db_pool_size",
    "Current SQLAlchemy connection pool size",
)

CIRCUIT_BREAKER_OPENS = Counter(
    "pipeline_circuit_breaker_opens_total",
    "Total number of times a circuit breaker has opened",
    ["breaker_name"],
)


# ---------------------------------------------------------------------------
# Prometheus server
# ---------------------------------------------------------------------------
def start_metrics_server(port: int = 9090) -> None:
    """Start a Prometheus HTTP server on the given port."""
    start_http_server(port)


# ---------------------------------------------------------------------------
# OpenTelemetry tracing
# ---------------------------------------------------------------------------
_tracer: Any = None  # opentelemetry.trace.Tracer | None


def configure_tracing(
    endpoint: str = "http://localhost:4317",
    service_name: str = "data-pipeline",
    enabled: bool = True,
) -> None:
    """
    Initialise the OTel SDK and wire a gRPC OTLP exporter.

    Call once at application startup (before any trace_span() calls).
    Safe to call multiple times — subsequent calls are no-ops.

    Parameters
    ----------
    endpoint:
        gRPC OTLP collector endpoint.
    service_name:
        Reported ``service.name`` resource attribute.
    enabled:
        If False, installs a no-op tracer (useful in tests / dev).
    """
    global _tracer  # noqa: PLW0603

    if _tracer is not None:
        return  # already initialised

    if not enabled:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider

        _tracer = TracerProvider().get_tracer(service_name)
        trace.set_tracer_provider(TracerProvider())
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer(service_name)
    except Exception:
        # Gracefully degrade if OTel SDK is unavailable
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider

        _tracer = TracerProvider().get_tracer(service_name)


def _get_tracer() -> Any:
    global _tracer  # noqa: PLW0603
    if _tracer is None:
        # Lazy no-op initialisation so callers never need to check for None
        from opentelemetry.sdk.trace import TracerProvider

        _tracer = TracerProvider().get_tracer("pipeline")
    return _tracer


@contextlib.contextmanager
def trace_span(name: str, **attributes: Any) -> Generator[Any, None, None]:
    """
    Context manager that opens an OTel span.

    Usage::

        with trace_span("process_chunk", chunk_index=3, entity="orders") as span:
            ...
            span.set_attribute("rows_loaded", 500)
    """
    tracer = _get_tracer()
    with tracer.start_as_current_span(name) as span:
        for key, value in attributes.items():
            span.set_attribute(key, str(value))
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(
                __import__("opentelemetry.trace", fromlist=["StatusCode"]).StatusCode.ERROR,
                description=str(exc),
            )
            raise


# ---------------------------------------------------------------------------
# Prometheus context manager helpers
# ---------------------------------------------------------------------------
class RunTimer:
    """Context manager that records a pipeline run's duration to Prometheus."""

    def __init__(self, source: str) -> None:
        self._source = source
        self._timer = RUN_DURATION.labels(source=source, status="success").time()
        ACTIVE_RUNS.inc()

    def __enter__(self) -> "RunTimer":
        self._timer.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ACTIVE_RUNS.dec()
        if exc_type is not None:
            RUN_DURATION.labels(source=self._source, status="error").observe(0)
        self._timer.__exit__(exc_type, exc_val, exc_tb)
