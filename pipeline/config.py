"""
Pipeline configuration — reads from environment variables (.env file).
"""

import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "3306"))
    DB_NAME: str = os.getenv("DB_NAME", "data_pipeline")
    DB_USER: str = os.getenv("DB_USER", "root")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def mysql_url(cls) -> str:
        return (
            f"mysql+pymysql://{cls.DB_USER}:{cls.DB_PASSWORD}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        )

    @classmethod
    def sqlite_url(cls) -> str:
        """In-memory SQLite URL — used for testing."""
        return "sqlite:///:memory:"
