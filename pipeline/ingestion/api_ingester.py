"""
REST API ingester with pagination, circuit-breaker, and streaming chunk support.

Improvements over the original requests-based ingester
-------------------------------------------------------
- Uses ``httpx`` (sync client with connection pooling & HTTP/2 ready)
- ``ingest_chunks()`` yields one page at a time — no accumulation in memory
- Integrated ``CircuitBreaker``: opens after N consecutive failures and
  refuses further calls until the recovery timeout expires
- Respects ``Retry-After`` and ``X-RateLimit-Reset`` headers
- Structured logging on every page fetch
- Full type annotations

Usage
-----
    ingester = APIIngester(
        url="https://api.example.com/orders",
        circuit_breaker=CircuitBreaker(failure_threshold=5, recovery_timeout=30),
    )
    for chunk in ingester.ingest_chunks():
        process(chunk)
"""
from __future__ import annotations

import time
from collections.abc import Generator
from typing import Any

import httpx

from pipeline.ingestion.base_ingester import BaseIngester
from pipeline.utils.circuit_breaker import CircuitBreaker, CircuitOpenError
from pipeline.utils.logger import get_logger

log = get_logger(__name__)

# Default circuit-breaker shared across all APIIngester instances unless overridden
_DEFAULT_CB = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0, name="api_ingester")


class APIIngester(BaseIngester):
    """
    Ingests records from a paginated REST API endpoint.

    Parameters
    ----------
    url:
        The API endpoint URL (first page).
    headers:
        Optional HTTP headers (e.g. ``Authorization: Bearer …``).
    params:
        Query parameters appended to every request.
    data_key:
        The JSON key that holds the list of records (e.g. ``"results"``).
        If ``None``, the top-level response is treated as the record list.
    next_key:
        The JSON key that holds the next-page URL (e.g. ``"next"``).
        Pagination stops when this key is absent or ``None``.
    timeout:
        Request timeout in seconds.
    circuit_breaker:
        ``CircuitBreaker`` instance. Defaults to a module-level shared breaker.
    max_retries:
        Retry each page up to this many times on transient errors (5xx / network).
    backoff_factor:
        Exponential backoff multiplier between retries.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        data_key: str | None = "results",
        next_key: str | None = "next",
        timeout: int = 30,
        circuit_breaker: CircuitBreaker | None = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.data_key = data_key
        self.next_key = next_key
        self.timeout = timeout
        self._cb = circuit_breaker or _DEFAULT_CB
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    # ------------------------------------------------------------------
    # Full load (backward-compatible)
    # ------------------------------------------------------------------
    def ingest(self) -> list[dict[str, Any]]:
        """Fetch all pages and return as a flat list."""
        records: list[dict[str, Any]] = []
        for chunk in self.ingest_chunks():
            records.extend(chunk)
        log.info("api_ingested_total", url=self.url, total=len(records))
        return records

    # ------------------------------------------------------------------
    # Streaming chunks — one page per chunk
    # ------------------------------------------------------------------
    def ingest_chunks(self, chunk_size: int = 0) -> Generator[list[dict[str, Any]], None, None]:  # noqa: ARG002
        """
        Yield one page of records at a time.

        ``chunk_size`` is accepted for interface compatibility but ignored —
        the API page size is determined by the server.
        """
        url: str | None = self.url
        first_page = True

        with httpx.Client(timeout=self.timeout) as client:
            while url:
                payload = self._fetch_page(client, url, self.params if first_page else {})
                first_page = False

                if self.data_key and isinstance(payload, dict):
                    page_records: list[Any] = payload.get(self.data_key, [])
                    next_url: str | None = (
                        payload.get(self.next_key) if self.next_key else None
                    )
                elif isinstance(payload, list):
                    page_records = payload
                    next_url = None
                else:
                    log.warning("api_unexpected_shape", url=url, type=type(payload).__name__)
                    break

                records = [r for r in page_records if isinstance(r, dict)]
                log.info("api_page_fetched", url=url, records=len(records))

                if records:
                    yield records

                url = next_url if next_url else None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_page(
        self,
        client: httpx.Client,
        url: str,
        params: dict[str, Any],
    ) -> Any:
        """Fetch a single page through the circuit breaker with retry."""
        for attempt in range(self.max_retries):
            try:
                response: httpx.Response = self._cb.call(
                    client.get, url, headers=self.headers, params=params
                )
                response.raise_for_status()
                return response.json()
            except CircuitOpenError:
                log.error("api_circuit_open", url=url)
                raise
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status in (429, 503):
                    wait = self._parse_retry_after(exc.response)
                    log.warning(
                        "api_rate_limited",
                        url=url,
                        status=status,
                        wait=wait,
                        attempt=attempt + 1,
                    )
                    time.sleep(wait)
                elif status >= 500 and attempt < self.max_retries - 1:
                    sleep_time = self.backoff_factor * (2**attempt)
                    log.warning(
                        "api_server_error_retry",
                        url=url,
                        status=status,
                        sleep=sleep_time,
                        attempt=attempt + 1,
                    )
                    time.sleep(sleep_time)
                else:
                    raise
            except (httpx.NetworkError, httpx.TimeoutException) as exc:
                if attempt < self.max_retries - 1:
                    sleep_time = self.backoff_factor * (2**attempt)
                    log.warning(
                        "api_network_error_retry",
                        url=url,
                        error=str(exc),
                        sleep=sleep_time,
                        attempt=attempt + 1,
                    )
                    time.sleep(sleep_time)
                else:
                    raise

        raise RuntimeError(f"All {self.max_retries} retries exhausted for {url}")

    @staticmethod
    def _parse_retry_after(response: httpx.Response) -> float:
        """Extract wait time from Retry-After or X-RateLimit-Reset headers."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        reset = response.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                return max(0.0, float(reset) - time.time())
            except ValueError:
                pass
        return 1.0  # default: wait 1s
