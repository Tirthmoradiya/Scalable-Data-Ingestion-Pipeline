"""
Structured logger using structlog.

In production → JSON output (machine-parseable, ELK/Grafana Loki compatible).
In development → human-friendly coloured console output.

Usage
-----
    from pipeline.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("rows_loaded", count=500, source="orders.csv")
"""

from __future__ import annotations

import logging
import sys
from typing import Any, cast

import structlog


def configure_logging(log_level: str = "INFO", log_format: str = "console") -> None:
    """
    Configure structlog globally.  Call once at application startup.

    Parameters
    ----------
    log_level:
        Standard Python log level string.
    log_format:
        ``"json"`` for production (ELK-friendly), ``"console"`` for development.
    """
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
    ]

    renderer: Any
    if log_format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog bound logger for the given module name."""
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))


def bind_run_context(run_id: str, source: str) -> None:
    """Bind pipeline run context to all subsequent log calls in this thread."""
    structlog.contextvars.bind_contextvars(run_id=run_id, source=source)


def clear_run_context() -> None:
    """Clear per-run context (call after each run completes)."""
    structlog.contextvars.clear_contextvars()
