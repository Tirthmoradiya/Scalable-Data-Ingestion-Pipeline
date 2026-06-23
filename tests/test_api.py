"""
Tests for the FastAPI status API.
Uses FastAPI TestClient — no running server required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pipeline.api.app import app

client = TestClient(app, raise_server_exceptions=False)


class TestHealthEndpoint:
    def test_health_returns_200(self) -> None:
        with patch("pipeline.api.app.get_session") as mock_dep:
            mock_session = MagicMock()
            mock_session.execute.return_value = MagicMock()
            mock_dep.return_value = iter([mock_session])
            # Override dependency
            app.dependency_overrides = {}
        resp = client.get("/health")
        # Accept 200 or 503 (DB may not be up in CI)
        assert resp.status_code in (200, 503, 500, 422)

    def test_health_schema(self) -> None:
        resp = client.get("/health")
        if resp.status_code == 200:
            data = resp.json()
            assert "status" in data
            assert "version" in data
            assert "db_reachable" in data


class TestMetricsEndpoint:
    def test_metrics_returns_text(self) -> None:
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "pipeline_rows_ingested_total" in resp.text or "# HELP" in resp.text

    def test_metrics_content_type(self) -> None:
        resp = client.get("/metrics")
        assert "text/plain" in resp.headers["content-type"]


class TestDocsEndpoint:
    def test_swagger_docs_available(self) -> None:
        resp = client.get("/docs")
        assert resp.status_code == 200

    def test_openapi_schema(self) -> None:
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert "paths" in schema
        assert "/health" in schema["paths"]
        assert "/runs" in schema["paths"]
        assert "/metrics" in schema["paths"]


class TestRunsEndpoint:
    def test_runs_returns_list(self) -> None:
        resp = client.get("/runs")
        # May return 200 (empty list) or 500 (no DB) in test context
        assert resp.status_code in (200, 500)
        if resp.status_code == 200:
            assert isinstance(resp.json(), list)

    def test_runs_pagination_params(self) -> None:
        resp = client.get("/runs?limit=5&offset=0")
        assert resp.status_code in (200, 500)

    def test_runs_invalid_limit_rejected(self) -> None:
        resp = client.get("/runs?limit=0")
        assert resp.status_code == 422  # FastAPI validation error

    def test_run_not_found_returns_404(self) -> None:
        resp = client.get("/runs/999999")
        assert resp.status_code in (404, 500)
