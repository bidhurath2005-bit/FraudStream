"""Environment-based configuration. Secrets are never hardcoded; see .env.example."""
from __future__ import annotations

import logging
import secrets
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[3]
log = logging.getLogger("fraudstream.config")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(ROOT_DIR / ".env"), env_file_encoding="utf-8", extra="ignore")

    env: Literal["development", "test", "production"] = Field("development", alias="FRAUDSTREAM_ENV")
    profile: Literal["local", "distributed"] = Field("local", alias="FRAUDSTREAM_PROFILE")
    database_url: str = Field(f"sqlite:///{(ROOT_DIR / 'data' / 'fraudstream.db').as_posix()}", alias="DATABASE_URL")
    redis_url: str | None = Field(None, alias="REDIS_URL")
    kafka_bootstrap_servers: str | None = Field(None, alias="KAFKA_BOOTSTRAP_SERVERS")
    jwt_secret: str | None = Field(None, alias="JWT_SECRET")
    jwt_algorithm: str = "HS256"
    access_token_minutes: int = Field(30, alias="ACCESS_TOKEN_MINUTES")
    refresh_token_days: int = Field(7, alias="REFRESH_TOKEN_DAYS")
    cors_origins: str = Field("http://localhost:5173,http://127.0.0.1:5173", alias="CORS_ORIGINS")
    artifacts_dir: Path = Field(ROOT_DIR / "ml" / "artifacts", alias="FRAUDSTREAM_ARTIFACTS_DIR")
    data_dir: Path = Field(ROOT_DIR / "data", alias="FRAUDSTREAM_DATA_DIR")
    # Where runtime_config.json (derived, regenerated on startup) is written. Defaults to the
    # artifacts dir; containers point it at a writable state volume so ml/artifacts can stay read-only.
    runtime_config_dir: Path | None = Field(None, alias="RUNTIME_CONFIG_DIR")
    login_rate_limit_per_minute: int = Field(10, alias="LOGIN_RATE_LIMIT_PER_MINUTE")
    api_rate_limit_per_minute: int = Field(1200, alias="API_RATE_LIMIT_PER_MINUTE")
    warm_start_hours: int = Field(24, alias="FEATURE_WARM_START_HOURS")
    secure_cookies: bool = Field(False, alias="SECURE_COOKIES")
    max_upload_mb: int = Field(200, alias="MAX_UPLOAD_MB")
    distributed_consumer_in_api: bool = Field(False, alias="RUN_CONSUMER_IN_API")

    @field_validator("database_url")
    @classmethod
    def _db(cls, v: str) -> str:
        if not (v.startswith("sqlite") or v.startswith("postgresql")):
            raise ValueError("DATABASE_URL must be sqlite:// or postgresql+psycopg://")
        return v

    @model_validator(mode="after")
    def _secrets(self) -> Settings:
        if not self.jwt_secret:
            if self.env == "production":
                raise ValueError("JWT_SECRET must be set in production")
            # Development/test only: ephemeral random secret (tokens are invalidated on restart).
            self.jwt_secret = secrets.token_urlsafe(48)
            log.warning("JWT_SECRET not set: using an EPHEMERAL random secret (env=%s). Set it in .env.", self.env)
        elif len(self.jwt_secret) < 32:
            raise ValueError("JWT_SECRET must be at least 32 characters")
        if self.profile == "distributed":
            missing = [n for n, v in (("KAFKA_BOOTSTRAP_SERVERS", self.kafka_bootstrap_servers),
                                      ("REDIS_URL", self.redis_url)) if not v]
            if missing:
                raise ValueError(f"distributed profile requires {missing}")
            if not self.database_url.startswith("postgresql"):
                raise ValueError("distributed profile requires a PostgreSQL DATABASE_URL")
        return self

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def runtime_config_path_dir(self) -> Path:
        return self.runtime_config_dir or self.artifacts_dir

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")


@lru_cache
def get_settings() -> Settings:
    return Settings()
