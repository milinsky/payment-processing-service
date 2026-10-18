import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from payment_service.config import get_settings
from payment_service.payments import dependencies
from payment_service.payments.dependencies import get_session, verify_api_key

pytestmark = pytest.mark.unit


@pytest.fixture
def two_key_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", "key-one,key-two")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_PASSWORD", "payments")
    monkeypatch.setenv("DB_NAME", "payments")
    monkeypatch.setenv("RABBIT_URL", "amqp://payments:payments@localhost:5672/")
    get_settings.cache_clear()


async def test_valid_first_key_passes(two_key_settings: None) -> None:
    result = await verify_api_key(x_api_key="key-one", settings=get_settings())
    assert result == "key-one"


async def test_valid_second_key_passes(two_key_settings: None) -> None:
    result = await verify_api_key(x_api_key="key-two", settings=get_settings())
    assert result == "key-two"


async def test_invalid_key_is_rejected(two_key_settings: None) -> None:
    with pytest.raises(HTTPException) as excinfo:
        await verify_api_key(x_api_key="wrong-key", settings=get_settings())
    assert excinfo.value.status_code == 401
    assert excinfo.value.headers is not None
    assert excinfo.value.headers["WWW-Authenticate"] == "ApiKey"


async def test_empty_key_is_rejected(two_key_settings: None) -> None:
    with pytest.raises(HTTPException) as excinfo:
        await verify_api_key(x_api_key="", settings=get_settings())
    assert excinfo.value.status_code == 401


async def test_missing_key_is_rejected(two_key_settings: None) -> None:
    with pytest.raises(HTTPException) as excinfo:
        await verify_api_key(x_api_key=None, settings=get_settings())
    assert excinfo.value.status_code == 401
    assert excinfo.value.headers is not None
    assert excinfo.value.headers["WWW-Authenticate"] == "ApiKey"


def test_verify_api_key_uses_constant_time_comparison() -> None:
    source = inspect.getsource(dependencies.verify_api_key)
    assert "compare_digest" in source
    assert "==" not in source


async def test_get_session_yields_from_state_maker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

    sessions: list[FakeSession] = []

    def maker() -> FakeSession:
        session = FakeSession()
        sessions.append(session)
        return session

    request: Any = MagicMock()
    request.app.state.session_maker = maker
    generator = get_session(request)
    yielded = None
    async for session in generator:
        yielded = session
        break
    assert isinstance(yielded, FakeSession)
    assert sessions == [yielded]
