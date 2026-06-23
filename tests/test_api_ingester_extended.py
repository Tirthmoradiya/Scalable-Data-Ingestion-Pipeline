"""Extended tests for APIIngester — covers retry, rate-limiting, network errors, etc."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from pipeline.ingestion.api_ingester import APIIngester
from pipeline.utils.circuit_breaker import CircuitBreaker, CircuitOpenError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_resp(status: int, body, headers: dict | None = None) -> MagicMock:
    """Build a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.json.return_value = body
    resp.headers = headers or {}
    if status >= 400:
        exc = httpx.HTTPStatusError(
            f"HTTP {status}", request=MagicMock(), response=resp
        )
        resp.raise_for_status.side_effect = exc
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Unexpected response shapes
# ---------------------------------------------------------------------------
class TestAPIIngesterUnexpectedShape:
    def test_unexpected_shape_stops_pagination(self) -> None:
        """Non-dict, non-list response (e.g. a scalar) triggers warning and stops."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = "unexpected string"
        mock_resp.raise_for_status.return_value = None

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", return_value=mock_resp):
            records = APIIngester("https://api.example.com", circuit_breaker=cb).ingest()

        assert records == []

    def test_dict_with_data_key_none_returns_empty(self) -> None:
        """data_key present but key missing from dict yields empty."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"other_key": [], "next": None}
        mock_resp.raise_for_status.return_value = None

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", return_value=mock_resp):
            records = APIIngester("https://api.example.com", circuit_breaker=cb).ingest()

        assert records == []

    def test_records_filtered_to_dicts_only(self) -> None:
        """Non-dict items in the page list should be silently dropped."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "results": [{"id": 1}, "not a dict", 42, {"id": 2}],
            "next": None,
        }
        mock_resp.raise_for_status.return_value = None

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", return_value=mock_resp):
            records = APIIngester("https://api.example.com", circuit_breaker=cb).ingest()

        assert len(records) == 2
        assert all(isinstance(r, dict) for r in records)


# ---------------------------------------------------------------------------
# Rate limiting (429 / 503)
# ---------------------------------------------------------------------------
class TestAPIIngesterRateLimiting:
    def test_rate_limited_then_success(self) -> None:
        """429 response triggers wait-and-retry, then success on second attempt."""
        rate_limited = _make_resp(429, {}, headers={"Retry-After": "0.001"})
        success = _make_resp(200, {"results": [{"id": 1}], "next": None})

        cb = CircuitBreaker(failure_threshold=10)
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            resp = rate_limited if call_count == 0 else success
            call_count += 1
            return resp

        with patch("httpx.Client.get", side_effect=side_effect), patch("time.sleep"):
            records = APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=3,
            ).ingest()

        assert len(records) == 1

    def test_503_triggers_retry(self) -> None:
        """503 also treated as rate-limited."""
        rate_limited = _make_resp(503, {}, headers={"Retry-After": "0.001"})
        success = _make_resp(200, {"results": [{"id": 99}], "next": None})

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            resp = rate_limited if call_count == 0 else success
            call_count += 1
            return resp

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", side_effect=side_effect), patch("time.sleep"):
            records = APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=3,
            ).ingest()

        assert len(records) == 1


# ---------------------------------------------------------------------------
# 5xx server errors with retry
# ---------------------------------------------------------------------------
class TestAPIIngesterServerErrors:
    def test_5xx_retries_then_succeeds(self) -> None:
        server_error = _make_resp(500, {})
        success = _make_resp(200, {"results": [{"id": 7}], "next": None})

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            resp = server_error if call_count < 2 else success
            call_count += 1
            return resp

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", side_effect=side_effect), patch("time.sleep"):
            records = APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=3,
                backoff_factor=0.0,
            ).ingest()

        assert len(records) == 1

    def test_5xx_all_retries_exhausted_raises(self) -> None:
        server_error = _make_resp(500, {})

        cb = CircuitBreaker(failure_threshold=10)
        with (
            patch("httpx.Client.get", return_value=server_error),
            patch("time.sleep"),
            pytest.raises(httpx.HTTPStatusError),
        ):
            APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=2,
                backoff_factor=0.0,
            ).ingest()

    def test_4xx_not_retried_raises_immediately(self) -> None:
        not_found = _make_resp(404, {})

        cb = CircuitBreaker(failure_threshold=10)
        with (
            patch("httpx.Client.get", return_value=not_found),
            pytest.raises(httpx.HTTPStatusError),
        ):
            APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=3,
            ).ingest()


# ---------------------------------------------------------------------------
# Network / timeout errors
# ---------------------------------------------------------------------------
class TestAPIIngesterNetworkErrors:
    def test_network_error_retries_then_succeeds(self) -> None:
        success = _make_resp(200, {"results": [{"id": 5}], "next": None})

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.NetworkError("connection refused")
            return success

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", side_effect=side_effect), patch("time.sleep"):
            records = APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=3,
                backoff_factor=0.0,
            ).ingest()

        assert len(records) == 1

    def test_timeout_retries_then_raises(self) -> None:
        cb = CircuitBreaker(failure_threshold=10)
        with (
            patch("httpx.Client.get", side_effect=httpx.TimeoutException("timed out")),
            patch("time.sleep"),
            pytest.raises(httpx.TimeoutException),
        ):
            APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=2,
                backoff_factor=0.0,
            ).ingest()

    def test_network_error_all_retries_raises(self) -> None:
        cb = CircuitBreaker(failure_threshold=10)
        with (
            patch("httpx.Client.get", side_effect=httpx.NetworkError("no route")),
            patch("time.sleep"),
            pytest.raises(httpx.NetworkError),
        ):
            APIIngester(
                "https://api.example.com",
                circuit_breaker=cb,
                max_retries=2,
                backoff_factor=0.0,
            ).ingest()


# ---------------------------------------------------------------------------
# Circuit breaker integration
# ---------------------------------------------------------------------------
class TestAPIIngesterCircuitBreaker:
    def test_circuit_open_error_propagates(self) -> None:
        cb = CircuitBreaker(failure_threshold=1)
        # Open the circuit
        cb._on_failure(RuntimeError("boom"))

        with pytest.raises(CircuitOpenError):
            APIIngester("https://api.example.com", circuit_breaker=cb).ingest()


# ---------------------------------------------------------------------------
# _parse_retry_after
# ---------------------------------------------------------------------------
class TestParseRetryAfter:
    def test_numeric_retry_after_header(self) -> None:
        resp = MagicMock(spec=httpx.Response)
        resp.headers = {"Retry-After": "5.0"}
        result = APIIngester._parse_retry_after(resp)
        assert result == 5.0

    def test_invalid_retry_after_falls_through_to_default(self) -> None:
        resp = MagicMock(spec=httpx.Response)
        resp.headers = {"Retry-After": "not-a-number"}
        result = APIIngester._parse_retry_after(resp)
        assert result == 1.0  # default

    def test_x_ratelimit_reset_header_used_as_fallback(self) -> None:
        future_ts = str(time.time() + 2.0)
        resp = MagicMock(spec=httpx.Response)
        resp.headers = {"X-RateLimit-Reset": future_ts}
        result = APIIngester._parse_retry_after(resp)
        assert 0.0 <= result <= 3.0

    def test_invalid_x_ratelimit_reset_falls_through_to_default(self) -> None:
        resp = MagicMock(spec=httpx.Response)
        resp.headers = {"X-RateLimit-Reset": "bad-value"}
        result = APIIngester._parse_retry_after(resp)
        assert result == 1.0

    def test_no_headers_returns_default(self) -> None:
        resp = MagicMock(spec=httpx.Response)
        resp.headers = {}
        result = APIIngester._parse_retry_after(resp)
        assert result == 1.0

    def test_x_ratelimit_reset_in_past_returns_zero(self) -> None:
        past_ts = str(time.time() - 10.0)
        resp = MagicMock(spec=httpx.Response)
        resp.headers = {"X-RateLimit-Reset": past_ts}
        result = APIIngester._parse_retry_after(resp)
        assert result == 0.0


# ---------------------------------------------------------------------------
# ingest_chunks with custom params / headers
# ---------------------------------------------------------------------------
class TestAPIIngesterCustomConfig:
    def test_custom_headers_passed(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": [{"id": 1}], "next": None}
        mock_resp.raise_for_status.return_value = None

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", return_value=mock_resp) as mock_get:
            APIIngester(
                "https://api.example.com",
                headers={"Authorization": "Bearer token"},
                circuit_breaker=cb,
            ).ingest()
            _, kwargs = mock_get.call_args
            assert kwargs["headers"]["Authorization"] == "Bearer token"

    def test_custom_params_passed_on_first_page(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": [], "next": None}
        mock_resp.raise_for_status.return_value = None

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", return_value=mock_resp) as mock_get:
            APIIngester(
                "https://api.example.com",
                params={"format": "json"},
                circuit_breaker=cb,
            ).ingest()
            _, kwargs = mock_get.call_args
            assert kwargs["params"]["format"] == "json"

    def test_chunk_size_param_ignored(self) -> None:
        """chunk_size is accepted for interface compat but ignored."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": [{"id": i} for i in range(5)], "next": None}
        mock_resp.raise_for_status.return_value = None

        cb = CircuitBreaker(failure_threshold=10)
        with patch("httpx.Client.get", return_value=mock_resp):
            chunks = list(
                APIIngester("https://api.example.com", circuit_breaker=cb).ingest_chunks(
                    chunk_size=2
                )
            )

        total = sum(len(c) for c in chunks)
        assert total == 5
