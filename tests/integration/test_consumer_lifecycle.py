from typing import Any

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker

from payment_service.config import AppSettings

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def consumer_module(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    import importlib

    monkeypatch.setattr("payment_service.consumer.broker", clean_rabbit)
    return importlib.import_module("payment_service.consumer")


async def test_setup_creates_runtime_and_teardown_closes(
    consumer_module: Any,
    clean_rabbit: RabbitBroker,
) -> None:
    await consumer_module.setup()
    assert consumer_module.runtime.processor is not None
    assert consumer_module.runtime.webhook is not None
    assert consumer_module.runtime.engine is not None
    await consumer_module.teardown()
    consumer_module.runtime = consumer_module._Runtime()
    await consumer_module.teardown()
    assert consumer_module.runtime.processor is None
