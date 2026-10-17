import importlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import structlog.testing
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import TestRabbitBroker

from payment_service.config import get_settings
from payment_service.messaging.events import (
    PaymentCreatedEvent,
    PaymentEventData,
)
from payment_service.messaging.topology import new_queue, payments_exchange

pytestmark = pytest.mark.unit

REQUIRED_ENV = {
    "API_KEYS": "unit",
    "DB_HOST": "localhost",
    "DB_USER": "payments",
    "DB_PASSWORD": "payments",
    "DB_NAME": "payments",
    "RABBIT_URL": "amqp://payments:payments@localhost:5672/",
}


@pytest.fixture
def consumer_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    return importlib.import_module("payment_service.consumer")


def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(), data=PaymentEventData(payment_id=uuid4())
    )


async def test_valid_event_delegates_parsed_event_and_headers(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegate = AsyncMock()
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", MagicMock())
    event = make_event()
    async with TestRabbitBroker(consumer_module.broker) as test_broker:
        await test_broker.publish(
            event.model_dump_json(),
            queue=new_queue(),
            exchange=payments_exchange(),
            headers={"x-retry-count": 2},
            message_id="mid-1",
        )
    assert delegate.await_count == 1
    call = delegate.await_args
    assert call is not None
    forwarded_event = call.args[4]
    assert isinstance(forwarded_event, PaymentCreatedEvent)
    assert forwarded_event == event
    assert call.args[3] == {"x-retry-count": 2}
    assert call.kwargs["message_id"] == "mid-1"
    assert call.args[2] == event.model_dump_json().encode()


async def test_missing_retry_header_delegates_empty_headers(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegate = AsyncMock()
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", MagicMock())
    event = make_event()
    async with TestRabbitBroker(consumer_module.broker) as test_broker:
        await test_broker.publish(
            event.model_dump_json(),
            queue=new_queue(),
            exchange=payments_exchange(),
        )
    assert delegate.await_count == 1
    empty_call = delegate.await_args
    assert empty_call is not None
    assert empty_call.args[3] == {}


async def test_broken_json_is_poison_without_delegation(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegate = AsyncMock()
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", MagicMock())
    async with TestRabbitBroker(consumer_module.broker) as test_broker:
        await test_broker.publish(
            "not-a-json{", queue=new_queue(), exchange=payments_exchange()
        )
    delegate.assert_not_awaited()


async def test_reject_from_delegate_propagates_without_test_crash(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegate = AsyncMock(side_effect=RejectMessage())
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", MagicMock())
    event = make_event()
    async with TestRabbitBroker(consumer_module.broker) as test_broker:
        await test_broker.publish(
            event.model_dump_json(),
            queue=new_queue(),
            exchange=payments_exchange(),
        )
    assert delegate.await_count == 1


async def test_missing_processor_raises_nack(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_mock = AsyncMock()
    monkeypatch.setattr(consumer_module.asyncio, "sleep", sleep_mock)
    delegate = AsyncMock()
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", None)
    with pytest.raises(NackMessage):
        await consumer_module.process_payment(
            message=make_rabbit_message(make_event().model_dump_json())
        )
    delegate.assert_not_awaited()
    sleep_mock.assert_awaited_once_with(1.0)


async def test_retry_scheduled_logs_dedicated_event(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr(consumer_module, "logger", capture)
    delegate = AsyncMock(return_value="retry_scheduled")
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", MagicMock())
    event = make_event()
    await consumer_module.process_payment(
        message=make_rabbit_message(event.model_dump_json())
    )
    infos = [call for call in capture.calls if call.method_name == "info"]
    assert infos
    assert infos[-1].args == ("message_retry_scheduled",)
    assert infos[-1].kwargs["event_id"] == str(event.event_id)
    assert infos[-1].kwargs["payment_id"] == str(event.data.payment_id)


async def test_acked_outcome_logs_message_processed(
    consumer_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr(consumer_module, "logger", capture)
    delegate = AsyncMock(return_value="acked")
    monkeypatch.setattr(consumer_module, "handle_with_retry", delegate)
    monkeypatch.setattr(consumer_module.runtime, "processor", MagicMock())
    event = make_event()
    await consumer_module.process_payment(
        message=make_rabbit_message(event.model_dump_json())
    )
    infos = [call for call in capture.calls if call.method_name == "info"]
    assert infos
    assert infos[-1].args == ("message_processed",)
    assert infos[-1].kwargs["event_id"] == str(event.event_id)
    assert infos[-1].kwargs["payment_id"] == str(event.data.payment_id)


def make_rabbit_message(body: str) -> Any:
    message: Any = SimpleNamespace(body=body.encode(), headers={}, message_id="mid")
    return message
