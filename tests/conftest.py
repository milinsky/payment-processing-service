import os
from collections.abc import Iterator

import pytest

from payment_service.config import get_settings

_COMPOSE_PG_DEFAULTS = {
    "DB_HOST": "localhost",
    "DB_PORT": "55432",
    "DB_USER": "payments",
    "DB_PASSWORD": "payments",
    "DB_NAME": "payments",
}

for _key, _value in {
    **_COMPOSE_PG_DEFAULTS,
    "API_KEYS": "test",
    "RABBIT_URL": "amqp://payments:payments@localhost:5672/",
}.items():
    os.environ.setdefault(_key, _value)

_SERVICE_ENV_KEYS = (
    "API_KEYS",
    "DOCS_ENABLED",
    "LOG_LEVEL",
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
    "RABBIT_URL",
    "GATEWAY_SUCCESS_RATE",
    "GATEWAY_DELAY_MIN_S",
    "GATEWAY_DELAY_MAX_S",
    "RETRY_TTLS_MS",
    "RETRY_MAX_ATTEMPTS",
    "WEBHOOK_TIMEOUT_S",
    "WEBHOOK_SECRET",
    "OUTBOX_POLL_INTERVAL_S",
    "OUTBOX_BATCH_SIZE",
    "OUTBOX_RETENTION_S",
    "OUTBOX_CLEANUP_INTERVAL_S",
)

_PG_PASSTHROUGH = {
    key: os.environ[key] for key in _COMPOSE_PG_DEFAULTS if key in os.environ
}


@pytest.fixture(scope="session")
def compose_pg_env() -> dict[str, str]:
    return {**_COMPOSE_PG_DEFAULTS, **_PG_PASSTHROUGH}


@pytest.fixture(autouse=True)
def isolated_service_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for key in _SERVICE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
