"""
Retry decorator with exponential back-off + dead-letter queue.

Features
--------
- Wraps any callable with configurable max attempts and back-off
- Uses ``tenacity`` under the hood for production-grade retry semantics
- Exhausted records are written to a dead-letter JSONL file for later reprocessing

Usage
-----
    from pipeline.utils.retry import with_retry, DeadLetterWriter

    @with_retry(max_attempts=3, backoff_factor=0.5)
    def flush_batch(session, batch):
        session.flush()

    # Dead-letter writer
    dlq = DeadLetterWriter(run_id="abc123", dead_letter_dir="dead_letter")
    dlq.write(record={"email": "bad@x.com"}, reason="ValidationError: ...")
    dlq.close()
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar

import tenacity

from pipeline.utils.logger import get_logger

log = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def with_retry(
    max_attempts: int = 3,
    backoff_factor: float = 0.5,
    reraise: bool = True,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """
    Decorator — retries the wrapped function with exponential back-off.

    Parameters
    ----------
    max_attempts:
        Maximum number of total attempts (1 = no retry).
    backoff_factor:
        Multiplier for sleep time between retries (wait = backoff_factor * 2^attempt).
    reraise:
        If True, re-raise the last exception after all attempts are exhausted.
    exceptions:
        Tuple of exception types to catch and retry on.
    """

    def decorator(fn: F) -> F:
        retry_strategy = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=tenacity.wait_exponential(multiplier=backoff_factor, min=0.1, max=30),
            retry=tenacity.retry_if_exception_type(exceptions),
            reraise=reraise,
        )

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return retry_strategy(fn)(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------
# Dead-Letter Queue
# ---------------------------------------------------------------------------
class DeadLetterWriter:
    """
    Writes failed records to a JSONL file under ``dead_letter_dir/``.

    File name: ``{dead_letter_dir}/{run_id}.jsonl``
    Each line: ``{"ts": "...", "reason": "...", "record": {...}}``
    """

    def __init__(self, run_id: str, dead_letter_dir: str = "dead_letter") -> None:
        self._run_id = run_id
        self._dir = Path(dead_letter_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / f"{run_id}.jsonl"
        self._fh = self._path.open("a", encoding="utf-8")
        self._count = 0

    def write(self, record: dict[str, Any], reason: str) -> None:
        """Append a failed record with its failure reason."""
        entry = {
            "ts": datetime.now(tz=UTC).isoformat(),
            "run_id": self._run_id,
            "reason": reason,
            "record": record,
        }
        self._fh.write(json.dumps(entry, default=str) + "\n")
        self._fh.flush()
        self._count += 1
        log.warning("dead_letter_write", run_id=self._run_id, reason=reason, count=self._count)

    def close(self) -> None:
        self._fh.close()
        if self._count:
            log.info(
                "dead_letter_closed",
                path=str(self._path),
                total_records=self._count,
            )

    def __enter__(self) -> DeadLetterWriter:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def count(self) -> int:
        return self._count
