"""Tests for CircuitBreaker — covers all state transitions and decorators."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from pipeline.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _failing(exc: Exception = RuntimeError("boom")):
    def fn():
        raise exc

    return fn


def _succeeding(value=42):
    def fn():
        return value

    return fn


# ---------------------------------------------------------------------------
# CLOSED state
# ---------------------------------------------------------------------------
class TestCircuitBreakerClosed:
    def test_initial_state_is_closed(self) -> None:
        cb = CircuitBreaker()
        assert cb.state is CircuitState.CLOSED

    def test_successful_call_returns_value(self) -> None:
        cb = CircuitBreaker()
        result = cb.call(_succeeding(99))
        assert result == 99

    def test_failure_increments_count_but_stays_closed_below_threshold(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(_failing())
        assert cb.state is CircuitState.CLOSED

    def test_reaching_threshold_opens_circuit(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            with pytest.raises(RuntimeError):
                cb.call(_failing())
        assert cb.state is CircuitState.OPEN

    def test_success_resets_failure_count(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        # 2 failures then 1 success — should stay closed
        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(_failing())
        cb.call(_succeeding())
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        # Still 1 failure after reset, not 3 — so still CLOSED
        assert cb.state is CircuitState.CLOSED


# ---------------------------------------------------------------------------
# OPEN state
# ---------------------------------------------------------------------------
class TestCircuitBreakerOpen:
    def test_open_circuit_raises_circuit_open_error(self) -> None:
        cb = CircuitBreaker(failure_threshold=1)
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        assert cb.state is CircuitState.OPEN
        with pytest.raises(CircuitOpenError):
            cb.call(_succeeding())

    def test_circuit_open_error_is_reraised_immediately(self) -> None:
        cb = CircuitBreaker(failure_threshold=1)
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        # Second call should raise CircuitOpenError, not call the function
        mock_fn = MagicMock()
        with pytest.raises(CircuitOpenError):
            cb.call(mock_fn)
        mock_fn.assert_not_called()

    def test_open_transitions_to_half_open_after_timeout(self) -> None:
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        assert cb.state is CircuitState.OPEN
        time.sleep(0.1)
        # Checking state should trigger transition to HALF_OPEN
        assert cb.state is CircuitState.HALF_OPEN


# ---------------------------------------------------------------------------
# HALF_OPEN state
# ---------------------------------------------------------------------------
class TestCircuitBreakerHalfOpen:
    def _open_and_wait(self, cb: CircuitBreaker) -> None:
        """Helper: open the circuit then wait for recovery."""
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        time.sleep(cb.recovery_timeout + 0.02)

    def test_successful_probe_closes_circuit(self) -> None:
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        self._open_and_wait(cb)
        assert cb.state is CircuitState.HALF_OPEN
        cb.call(_succeeding())
        assert cb.state is CircuitState.CLOSED

    def test_failed_probe_reopens_circuit(self) -> None:
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        self._open_and_wait(cb)
        assert cb.state is CircuitState.HALF_OPEN
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        assert cb.state is CircuitState.OPEN

    def test_multiple_successes_required_to_close(self) -> None:
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05, success_threshold=2)
        self._open_and_wait(cb)
        # First probe success — still HALF_OPEN (need 2)
        cb.call(_succeeding())
        assert cb.state is CircuitState.HALF_OPEN
        # Second probe success — now CLOSED
        cb.call(_succeeding())
        assert cb.state is CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Decorator usage
# ---------------------------------------------------------------------------
class TestCircuitBreakerDecorator:
    def test_as_decorator_passes_args(self) -> None:
        cb = CircuitBreaker()

        @cb
        def add(a: int, b: int) -> int:
            return a + b

        assert add(3, 4) == 7

    def test_as_decorator_propagates_exceptions(self) -> None:
        cb = CircuitBreaker(failure_threshold=10)

        @cb
        def boom() -> None:
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            boom()

    def test_decorator_preserves_function_name(self) -> None:
        cb = CircuitBreaker()

        @cb
        def my_func() -> None:
            pass

        assert my_func.__name__ == "my_func"

    def test_decorator_opens_after_threshold(self) -> None:
        cb = CircuitBreaker(failure_threshold=2)

        @cb
        def bad() -> None:
            raise RuntimeError("err")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                bad()

        assert cb.state is CircuitState.OPEN
        with pytest.raises(CircuitOpenError):
            bad()


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------
class TestCircuitBreakerReset:
    def test_reset_returns_to_closed(self) -> None:
        cb = CircuitBreaker(failure_threshold=1)
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        assert cb.state is CircuitState.OPEN
        cb.reset()
        assert cb.state is CircuitState.CLOSED

    def test_reset_clears_failure_count(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(_failing())
        cb.reset()
        # After reset, need 3 new failures to open
        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(_failing())
        assert cb.state is CircuitState.CLOSED

    def test_calls_succeed_after_reset(self) -> None:
        cb = CircuitBreaker(failure_threshold=1)
        with pytest.raises(RuntimeError):
            cb.call(_failing())
        cb.reset()
        result = cb.call(_succeeding(7))
        assert result == 7


# ---------------------------------------------------------------------------
# circuit_open_error is not swallowed
# ---------------------------------------------------------------------------
class TestCircuitOpenErrorNotSwallowed:
    def test_circuit_open_error_propagates_through_call(self) -> None:
        """If the wrapped fn itself raises CircuitOpenError, it must be re-raised."""
        cb = CircuitBreaker()

        def raises_open():
            raise CircuitOpenError("manual")

        with pytest.raises(CircuitOpenError, match="manual"):
            cb.call(raises_open)
