"""
Thread-safe circuit breaker for external I/O calls.

States
------
CLOSED   — normal operation; failures are counted
OPEN     — all calls fail-fast immediately (no I/O attempted)
HALF_OPEN — a single probe call is allowed; if it succeeds the breaker
             resets to CLOSED; if it fails, the breaker returns to OPEN

Usage
-----
    from pipeline.utils.circuit_breaker import CircuitBreaker

    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0)

    @cb
    def call_api(url: str) -> dict:
        return httpx.get(url).json()

    # or as a context manager:
    with cb.protected():
        response = httpx.get(url)
"""
from __future__ import annotations

import functools
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, TypeVar

from pipeline.utils.logger import get_logger

log = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitOpenError(Exception):
    """Raised when a call is attempted against an OPEN circuit breaker."""


@dataclass
class CircuitBreaker:
    """
    Thread-safe circuit breaker.

    Parameters
    ----------
    failure_threshold:
        Number of consecutive failures before the circuit opens.
    recovery_timeout:
        Seconds to wait in OPEN state before allowing a probe (HALF_OPEN).
    success_threshold:
        Consecutive successes in HALF_OPEN required to return to CLOSED.
    name:
        Label used in log messages.
    """

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    success_threshold: int = 1
    name: str = "default"

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False, repr=False)
    _failure_count: int = field(default=0, init=False, repr=False)
    _success_count: int = field(default=0, init=False, repr=False)
    _opened_at: float | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------
    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._get_state_unlocked()

    def _get_state_unlocked(self) -> CircuitState:
        if self._state is CircuitState.OPEN:
            assert self._opened_at is not None
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                log.info("circuit_half_open", name=self.name)
        return self._state

    def _on_success(self) -> None:
        with self._lock:
            state = self._get_state_unlocked()
            self._failure_count = 0
            if state is CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._opened_at = None
                    log.info("circuit_closed", name=self.name)

    def _on_failure(self, exc: Exception) -> None:
        with self._lock:
            state = self._get_state_unlocked()
            if state is CircuitState.HALF_OPEN:
                # Probe failed — re-open immediately
                self._open()
                return
            self._failure_count += 1
            log.warning(
                "circuit_failure",
                name=self.name,
                count=self._failure_count,
                threshold=self.failure_threshold,
                error=str(exc),
            )
            if self._failure_count >= self.failure_threshold:
                self._open()

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._failure_count = 0
        log.error(
            "circuit_opened",
            name=self.name,
            recovery_timeout=self.recovery_timeout,
        )

    # ------------------------------------------------------------------
    # Call wrapper
    # ------------------------------------------------------------------
    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute *fn* through the circuit breaker."""
        with self._lock:
            state = self._get_state_unlocked()

        if state is CircuitState.OPEN:
            raise CircuitOpenError(
                f"Circuit '{self.name}' is OPEN — refusing call to protect downstream"
            )

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except CircuitOpenError:
            raise
        except Exception as exc:
            self._on_failure(exc)
            raise

    def __call__(self, fn: F) -> F:
        """Use as a decorator."""

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return self.call(fn, *args, **kwargs)

        return wrapper  # type: ignore[return-value]

    def reset(self) -> None:
        """Manually reset to CLOSED state (useful in tests)."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._opened_at = None
