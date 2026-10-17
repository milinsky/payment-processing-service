import asyncio
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command
from payment_service.config import get_settings

pytestmark = pytest.mark.integration

_PROJECT_ROOT = Path(__file__).parents[2]
_DB_ENV_KEYS = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
_PASSTHROUGH_DB_ENV = {
    key: os.environ[key] for key in _DB_ENV_KEYS if key in os.environ
}
_DEFAULT_DB_ENV = {
    "DB_HOST": "localhost",
    "DB_PORT": "55432",
    "DB_USER": "payments",
    "DB_PASSWORD": "payments",
    "DB_NAME": "payments",
}

_EXPECTED_PAYMENTS_NULLABILITY = {
    "id": "NO",
    "amount": "NO",
    "currency": "NO",
    "description": "NO",
    "metadata": "YES",
    "status": "NO",
    "idempotency_key": "NO",
    "request_fingerprint": "NO",
    "webhook_url": "NO",
    "created_at": "NO",
    "processed_at": "YES",
    "webhook_delivered_at": "YES",
    "webhook_attempts": "NO",
}


@pytest.fixture
def db_dsn(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    for key, value in {**_DEFAULT_DB_ENV, **_PASSTHROUGH_DB_ENV}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("API_KEYS", "integration")
    monkeypatch.setenv("RABBIT_URL", "amqp://payments:payments@localhost:5672/")
    get_settings.cache_clear()
    yield get_settings().db.dsn
    get_settings.cache_clear()


def _alembic_config() -> Config:
    config = Config(str(_PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(_PROJECT_ROOT / "alembic"))
    return config


def _upgrade() -> None:
    command.upgrade(_alembic_config(), "head")


def _downgrade() -> None:
    command.downgrade(_alembic_config(), "base")


def _fetch_all(dsn: str, query: str) -> list[dict[str, Any]]:
    async def fetch() -> list[dict[str, Any]]:
        engine = create_async_engine(dsn)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(text(query))
                return [dict(row) for row in result.mappings()]
        finally:
            await engine.dispose()

    return asyncio.run(fetch())


def _public_tables(dsn: str) -> set[str]:
    return {
        row["table_name"]
        for row in _fetch_all(
            dsn,
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = 'public'",
        )
    }


def test_upgrade_creates_tables_and_enum_types(db_dsn: str) -> None:
    _upgrade()
    tables = _public_tables(db_dsn)
    assert {"payments", "outbox"} <= tables
    types = {
        row["typname"] for row in _fetch_all(db_dsn, "SELECT typname FROM pg_type")
    }
    assert {"payment_status", "currency_code"} <= types


def test_payment_status_enum_has_no_processing_value(db_dsn: str) -> None:
    _upgrade()
    labels = {
        row["enumlabel"]
        for row in _fetch_all(
            db_dsn,
            "SELECT e.enumlabel FROM pg_enum e"
            " JOIN pg_type t ON t.oid = e.enumtypid"
            " WHERE t.typname = 'payment_status'",
        )
    }
    assert labels == {"pending", "succeeded", "failed"}


def test_payments_columns_match_spec(db_dsn: str) -> None:
    _upgrade()
    rows = _fetch_all(
        db_dsn,
        "SELECT column_name, is_nullable, data_type"
        " FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = 'payments'",
    )
    columns = {row["column_name"]: row for row in rows}
    assert set(columns) == set(_EXPECTED_PAYMENTS_NULLABILITY)
    for name, nullable in _EXPECTED_PAYMENTS_NULLABILITY.items():
        assert columns[name]["is_nullable"] == nullable, name
    assert columns["id"]["data_type"] == "uuid"
    assert columns["amount"]["data_type"] == "numeric"
    assert columns["request_fingerprint"]["data_type"] == "character"
    assert columns["metadata"]["data_type"] == "jsonb"
    assert columns["currency"]["data_type"] == "USER-DEFINED"
    assert columns["status"]["data_type"] == "USER-DEFINED"
    assert columns["created_at"]["data_type"] == "timestamp with time zone"


def test_id_defaults_use_uuidv7(db_dsn: str) -> None:
    _upgrade()
    rows = _fetch_all(
        db_dsn,
        "SELECT table_name, column_default FROM information_schema.columns"
        " WHERE table_schema = 'public'"
        " AND ((table_name = 'payments' AND column_name = 'id')"
        " OR (table_name = 'outbox' AND column_name = 'event_id'))",
    )
    defaults = {row["table_name"]: row["column_default"] for row in rows}
    assert len(defaults) == 2
    assert "uuidv7" in defaults["payments"]
    assert "uuidv7" in defaults["outbox"]


def test_index_names_are_deterministic(db_dsn: str) -> None:
    _upgrade()
    indexes = {
        row["indexname"]
        for row in _fetch_all(
            db_dsn,
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
        )
    }
    assert {
        "payments_pkey",
        "outbox_pkey",
        "payments_idempotency_key_key",
        "outbox_event_id_key",
        "idx_outbox_pending",
    } <= indexes


def test_partial_index_has_where_clause(db_dsn: str) -> None:
    _upgrade()
    rows = _fetch_all(
        db_dsn,
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'public' AND tablename = 'outbox'"
        " AND indexname = 'idx_outbox_pending'",
    )
    assert len(rows) == 1
    assert "WHERE (dispatched_at IS NULL)" in rows[0]["indexdef"]


def test_upgrade_downgrade_upgrade_cycle(db_dsn: str) -> None:
    _upgrade()
    try:
        _downgrade()
        tables = _public_tables(db_dsn)
        assert "payments" not in tables
        assert "outbox" not in tables
        types = {
            row["typname"] for row in _fetch_all(db_dsn, "SELECT typname FROM pg_type")
        }
        assert "payment_status" not in types
        assert "currency_code" not in types
    finally:
        _upgrade()
    assert {"payments", "outbox"} <= _public_tables(db_dsn)
