import importlib
import inspect
import json
import os
from typing import Any
from uuid import UUID

import pytest
import structlog
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from payment_service.config import get_settings

pytestmark = pytest.mark.integration

REQUIRED_ENV = {
    "API_KEYS": "integration",
    "DB_HOST": os.environ.get("DB_HOST", "localhost"),
    "DB_PORT": os.environ.get("DB_PORT", "55432"),
    "DB_USER": os.environ.get("DB_USER", "payments"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD", "payments"),
    "DB_NAME": os.environ.get("DB_NAME", "payments"),
    "RABBIT_URL": os.environ.get(
        "RABBIT_URL", "amqp://payments:payments@localhost:5672/"
    ),
}


class BrokenSession:
    async def __aenter__(self) -> BrokenSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def execute(self, *args: object, **kwargs: object) -> Any:
        raise RuntimeError("database unavailable")


def broken_session_maker() -> BrokenSession:
    return BrokenSession()


@pytest.fixture
def main_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    return importlib.import_module("payment_service.main")


@pytest.fixture
def app(main_module: Any) -> FastAPI:
    created: FastAPI = main_module.create_app(get_settings())
    return created


async def test_lifespan_starts_engine_and_health_serves(
    app: FastAPI,
) -> None:
    async with app.router.lifespan_context(app):
        assert app.state.engine is not None
        assert app.state.session_maker is not None
        assert not hasattr(app.state, "broker")
        assert not hasattr(app.state, "relay_task")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/health")
            ready = await client.get("/health/ready")
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}
        assert ready.status_code == 200
        assert ready.json() == {"database": "ok"}
        app.state.session_maker = broken_session_maker
        degraded_transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=degraded_transport, base_url="http://test"
        ) as client:
            degraded = await client.get("/health/ready")
        assert degraded.status_code == 503
        assert degraded.json() == {"database": "unavailable"}
    assert app.state.engine.pool.status() is not None


async def test_response_carries_generated_request_id(
    app: FastAPI,
) -> None:
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.get("/health")
            second = await client.get("/health")
    assert first.status_code == 200
    assert second.status_code == 200
    first_id = first.headers["x-request-id"]
    second_id = second.headers["x-request-id"]
    assert first_id != second_id
    for value in (first_id, second_id):
        assert len(value) == 36
        assert value.count("-") == 4


async def test_incoming_request_id_is_echoed(
    app: FastAPI,
) -> None:
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/health",
                headers={"X-Request-ID": "c0ffee00-0000-4000-8000-000000000001"},
            )
    assert response.headers["x-request-id"] == "c0ffee00-0000-4000-8000-000000000001"


async def test_request_id_reaches_log_context(
    app: FastAPI,
) -> None:
    import structlog

    captured: dict[str, object] = {}

    async with app.router.lifespan_context(app):
        original_maker = app.state.session_maker

        def capturing_maker() -> object:
            captured.update(structlog.contextvars.get_contextvars())
            return original_maker()

        app.state.session_maker = capturing_maker
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/health/ready",
                headers={"X-Request-ID": "c0ffee00-0000-4000-8000-000000000002"},
            )
    assert response.status_code == 200
    assert captured.get("request_id") == "c0ffee00-0000-4000-8000-000000000002"


async def test_invalid_request_id_replaced_with_generated_uuid(
    app: FastAPI,
) -> None:
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/health", headers={"X-Request-ID": "not-a-uuid"}
            )
    echoed = response.headers["x-request-id"]
    assert echoed != "not-a-uuid"
    parsed = UUID(echoed)
    assert str(parsed) == echoed


async def test_request_id_canonicalized_from_uppercase(
    app: FastAPI,
) -> None:
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/health",
                headers={"X-Request-ID": "C0FFEE00-0000-4000-8000-000000000003"},
            )
    assert response.headers["x-request-id"] == ("c0ffee00-0000-4000-8000-000000000003")


def test_create_app_configures_json_logging(
    main_module: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    main_module.create_app(get_settings())

    structlog.get_logger("json_probe").info("create_app_json_probe")

    captured = capsys.readouterr()
    lines = [
        line
        for line in (captured.out + captured.err).splitlines()
        if "create_app_json_probe" in line
    ]
    assert lines
    parsed = json.loads(lines[-1])
    assert parsed["event"] == "create_app_json_probe"
    assert parsed["level"] == "info"
    assert "timestamp" in parsed


def test_main_does_not_use_deprecated_on_event(main_module: Any) -> None:
    source = inspect.getsource(main_module)
    assert "on_event" not in source
