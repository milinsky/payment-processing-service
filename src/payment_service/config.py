import logging
from functools import lru_cache
from typing import Annotated, Self

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _split_csv(value: object) -> object:
    if isinstance(value, str):
        return [chunk for chunk in value.split(",") if chunk.strip()]
    return value


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    api_keys: Annotated[list[str], NoDecode] = Field(
        min_length=1, validation_alias="API_KEYS"
    )
    docs_enabled: bool = Field(default=True, validation_alias="DOCS_ENABLED")

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, value: object) -> object:
        return _split_csv(value)


class DbSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore")

    host: str
    port: int = 5432
    user: str
    password: str
    name: str

    @property
    def dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class RabbitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RABBIT_", extra="ignore")

    url: str


class LogSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOG_", extra="ignore")

    level: str = "INFO"

    @field_validator("level")
    @classmethod
    def validate_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in logging.getLevelNamesMapping():
            raise ValueError(f"unknown log level: {value}")
        return normalized


class ProcessingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_", extra="ignore")

    success_rate: float = Field(0.9, ge=0, le=1)
    delay_min_s: float = 2.0
    delay_max_s: float = 5.0
    retry_ttls_ms: Annotated[list[int], NoDecode] = Field(
        default=[5000, 10000, 20000], validation_alias="RETRY_TTLS_MS"
    )
    retry_max_attempts: int = Field(default=3, validation_alias="RETRY_MAX_ATTEMPTS")

    @field_validator("retry_ttls_ms", mode="before")
    @classmethod
    def parse_retry_ttls_ms(cls, value: object) -> object:
        return _split_csv(value)

    @model_validator(mode="after")
    def validate_retry_shape(self) -> Self:
        if self.retry_max_attempts > len(self.retry_ttls_ms):
            raise ValueError(
                "retry_max_attempts cannot exceed number of RETRY_TTLS_MS entries"
            )
        return self


class WebhookSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WEBHOOK_", extra="ignore")

    timeout_s: float = 10.0
    secret: str | None = None

    @field_validator("secret", mode="before")
    @classmethod
    def normalize_blank_secret(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value


class OutboxSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OUTBOX_", extra="ignore")

    poll_interval_s: float = 0.5
    batch_size: int = 100
    retention_s: float = Field(default=86400.0, gt=0)
    cleanup_interval_s: float = Field(default=3600.0, gt=0)


class AppSettings(BaseModel):
    api: ApiSettings
    db: DbSettings
    processing: ProcessingSettings
    webhook: WebhookSettings
    outbox: OutboxSettings
    log: LogSettings


@lru_cache
def get_settings() -> AppSettings:
    return AppSettings(
        api=ApiSettings(),
        db=DbSettings(),
        processing=ProcessingSettings(),
        webhook=WebhookSettings(),
        outbox=OutboxSettings(),
        log=LogSettings(),
    )
