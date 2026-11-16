"""Centralized runtime configuration loaded from environment variables."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed pipeline configuration with safe defaults for local and Docker runs."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    storage_backend: Literal["local", "s3"] = "local"
    local_lakehouse_dir: Path = Path("./data/lakehouse")

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    s3_bucket: str = "lakehouse"

    oltp_db_uri: str = "postgresql+psycopg2://fintech_user:fintech_pass@localhost:5433/fintech_oltp"
    duckdb_path: Path = Path("./data/warehouse/financial_lakehouse.duckdb")

    fx_api_base_url: str = "https://api.frankfurter.app"
    max_quarantine_ratio: float = 0.25
    alert_webhook_url: str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
