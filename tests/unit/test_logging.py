import json
import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import structlog

from payment_service.config import get_settings
from payment_service.logging import configure_logging

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def clean_contextvars() -> Iterator[None]:
    structlog.contextvars.clear_contextvars()
    yield
    structlog.contextvars.clear_contextvars()


def emit_and_parse(capsys: pytest.CaptureFixture[str], event: str) -> dict[str, Any]:
    structlog.get_logger("test").info(event)
    out = capsys.readouterr().out
    parsed: dict[str, Any] = json.loads(out.strip().splitlines()[-1])
    return parsed


def test_event_is_valid_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    parsed = emit_and_parse(capsys, "service_started")
    assert parsed["event"] == "service_started"
    assert parsed["level"] == "info"
    assert parsed["logger"] == "test"
    assert "timestamp" in parsed


def test_timestamp_is_iso_utc(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    parsed = emit_and_parse(capsys, "tick")
    timestamp = parsed["timestamp"]
    assert isinstance(timestamp, str)
    assert timestamp.endswith("Z")
    parsed_ts = datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00")
    assert parsed_ts.tzinfo is not None


def test_bind_contextvars_reaches_event(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    structlog.contextvars.bind_contextvars(payment_id="X")
    parsed = emit_and_parse(capsys, "payment_processed")
    assert parsed["payment_id"] == "X"


def test_stdlib_logging_bridged_to_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    logging.getLogger("stdlib.source").warning("bridged %s", "event")
    out = capsys.readouterr().out
    parsed = json.loads(out.strip().splitlines()[-1])
    assert parsed["event"] == "bridged event"
    assert parsed["level"] == "warning"


def test_settings_secrets_not_logged(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("API_KEYS", "secret-key-one,secret-key-two")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_PASSWORD", "supersecret")
    monkeypatch.setenv("DB_NAME", "payments")
    monkeypatch.setenv("RABBIT_URL", "amqp://payments:payments@rabbitmq:5672/")
    settings = get_settings()
    configure_logging()
    structlog.get_logger("test").info(
        "settings_loaded",
        db_host=settings.db.host,
        outbox_batch=settings.outbox.batch_size,
    )
    out = capsys.readouterr().out
    assert "supersecret" not in out
    assert "secret-key-one" not in out
    assert "payments:payments@" not in out
