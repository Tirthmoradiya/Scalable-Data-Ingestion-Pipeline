"""
Settings validation tests.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeline.settings import DatabaseSettings, ObservabilitySettings, PipelineSettings, Settings


class TestDatabaseSettings:
    def test_defaults_load(self) -> None:
        s = DatabaseSettings()
        assert s.host == "localhost"
        assert s.port == 3306
        assert s.pool_size == 10

    def test_url_property_masks_password(self) -> None:
        s = DatabaseSettings()
        assert s.url_safe.startswith("mysql+pymysql://")
        assert "***" in s.url_safe

    def test_url_property_includes_charset(self) -> None:
        s = DatabaseSettings()
        assert "utf8mb4" in s.url

    def test_invalid_port_raises(self) -> None:
        with pytest.raises(ValidationError):
            DatabaseSettings(port=99999)

    def test_pool_size_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            DatabaseSettings(pool_size=0)


class TestPipelineSettings:
    def test_defaults(self) -> None:
        s = PipelineSettings()
        assert s.batch_size == 500
        assert s.max_workers == 4
        assert s.chunk_size == 1000

    def test_batch_size_must_be_at_least_1(self) -> None:
        with pytest.raises(ValidationError):
            PipelineSettings(batch_size=0)

    def test_max_workers_bounded(self) -> None:
        with pytest.raises(ValidationError):
            PipelineSettings(max_workers=33)


class TestObservabilitySettings:
    def test_valid_log_levels(self) -> None:
        for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            s = ObservabilitySettings(log_level=level)
            assert s.log_level == level

    def test_invalid_log_level_raises(self) -> None:
        with pytest.raises(ValidationError):
            ObservabilitySettings(log_level="VERBOSE")

    def test_log_format_options(self) -> None:
        assert ObservabilitySettings(log_format="json").log_format == "json"
        assert ObservabilitySettings(log_format="console").log_format == "console"


class TestSettings:
    def test_root_settings_load(self) -> None:
        s = Settings()
        assert s.app_name == "data-pipeline"
        assert s.version == "1.0.0"

    def test_is_development_default(self) -> None:
        s = Settings()
        assert s.is_development is True
        assert s.is_production is False

    def test_sub_settings_accessible(self) -> None:
        s = Settings()
        assert s.db is not None
        assert s.pipeline is not None
        assert s.obs is not None
        assert s.api is not None
