"""
Pydantic-Settings based configuration.

Supports layered env files:
  .env                 → base defaults
  .env.development     → dev overrides
  .env.production      → prod overrides (never committed)

Usage
-----
    from pipeline.settings import settings
    print(settings.db.url)
"""
from __future__ import annotations

import enum
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, enum.Enum):
    development = "development"
    staging = "staging"
    production = "production"


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore")

    host: str = Field(default="localhost", description="MySQL host")
    port: int = Field(default=3306, ge=1, le=65535)
    name: str = Field(default="data_pipeline")
    user: str = Field(default="root")
    password: SecretStr = Field(default=SecretStr(""))
    pool_size: int = Field(default=10, ge=1, le=100)
    max_overflow: int = Field(default=20, ge=0)
    pool_timeout: int = Field(default=30, ge=1)
    pool_recycle: int = Field(default=1800, ge=60)
    echo: bool = Field(default=False)

    @property
    def url(self) -> str:
        pwd = self.password.get_secret_value()
        return (
            f"mysql+pymysql://{self.user}:{pwd}"
            f"@{self.host}:{self.port}/{self.name}"
            f"?charset=utf8mb4"
        )

    @property
    def url_safe(self) -> str:
        """URL with password masked — safe for logging."""
        return (
            f"mysql+pymysql://{self.user}:***"
            f"@{self.host}:{self.port}/{self.name}"
        )


class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PIPELINE_", extra="ignore")

    batch_size: int = Field(default=500, ge=1)
    max_workers: int = Field(default=4, ge=1, le=32)
    chunk_size: int = Field(default=1000, ge=100)
    dead_letter_dir: str = Field(default="dead_letter")
    retry_max_attempts: int = Field(default=3, ge=1)
    retry_backoff_factor: float = Field(default=0.5, ge=0.0)


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBS_", extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "console"] = "console"
    metrics_enabled: bool = True
    metrics_port: int = Field(default=9090, ge=1024, le=65535)
    tracing_enabled: bool = False
    tracing_endpoint: str = "http://localhost:4317"


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104
    port: int = Field(default=8000, ge=1024, le=65535)
    reload: bool = False
    cors_origins: list[str] = Field(default=["*"])


class Settings(BaseSettings):
    """Root settings object — compose all sub-settings."""

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development", ".env.staging", ".env.production"),
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    environment: Environment = Environment.development
    app_name: str = "data-pipeline"
    version: str = "1.0.0"

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    obs: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    api: APISettings = Field(default_factory=APISettings)

    @property
    def is_production(self) -> bool:
        return self.environment == Environment.production

    @property
    def is_development(self) -> bool:
        return self.environment == Environment.development


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance — call once, reuse everywhere."""
    return Settings()


# Convenience singleton
settings = get_settings()
