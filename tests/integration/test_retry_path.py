import asyncio
from typing import Any
from uuid import uuid4

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractIncomingMessage
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitBroker

from payment_service.config import AppSettings, RabbitSettings, get_settings
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.messaging.topology import (
    QUEUE_DLQ,
    QUEUE_NEW,
    ROUTING_KEY_NEW,
    declare_full_topology,
    payments_exchange,
)
from payment_service.processing.retry import handle_with_retry
from payment_service.processing.service import ProcessingOutcome

pytestmark = pytest.mark.integration


class AlwaysFailingProcessor:
    def __init__(self) -> None:
        self.calls = 0

    async def handle(self, event: PaymentCreatedEvent) -> ProcessingOutcome:
        self.calls += 1
        raise RuntimeError("persistent technical failure")


class RecoveringProcessor:
    def __init__(self, failures: int) -> None:
        self._failures = failures
        self.calls = 0

    async def handle(self, event: PaymentCreatedEvent) -> ProcessingOutcome:
        self.calls += 1
        if self.calls <= self._failures:
            raise RuntimeError("transient technical failure")
        return ProcessingOutcome.ACKED_BUSINESS


@pytest_asyncio.fixture
async def retry_world(
    clean_rabbit: RabbitBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    monkeypatch.setenv("RETRY_TTLS_MS", "1000,1000,1000")
    monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "3")
    get_settings.cache_clear()
    settings = get_settings()
    await declare_full_topology(clean_rabbit, settings)
    connection = await aio_pika.connect(RabbitSettings().url)
    channel = await connection.channel()
    try:
        yield clean_rabbit, settings, channel
    finally:
        await connection.close()


def make_event_json() -> bytes:
    event = PaymentCreatedEvent(
        event_id=uuid4(), data=PaymentEventData(payment_id=uuid4())
    )
    return event.model_dump_json().encode()


async def drive(
    broker: RabbitBroker,
    channel: aio_pika.Channel,
    processor: Any,
    settings: AppSettings,
    body: bytes,
    dlq_timeout_s: float,
) -> tuple[list[dict[str, Any]], list[AbstractIncomingMessage]]:
    deliveries: list[dict[str, Any]] = []
    dlq_messages: list[AbstractIncomingMessage] = []
    queue = await channel.get_queue(QUEUE_NEW, ensure=True)
    dlq = await channel.get_queue(QUEUE_DLQ, ensure=True)
    loop = asyncio.get_running_loop()

    async def on_message(message: AbstractIncomingMessage) -> None:
        headers: dict[str, Any] = dict(message.headers or {})
        deliveries.append(headers)
        event = PaymentCreatedEvent.model_validate_json(message.body)
        try:
            await handle_with_retry(
                broker,
                processor,
                message.body,
                headers,
                event,
                settings.processing.retry_max_attempts,
                message_id=message.message_id,
            )
        except RejectMessage:
            await message.reject(requeue=False)
        except NackMessage:
            await message.nack(requeue=True)
        else:
            await message.ack()

    consumer_tag = await queue.consume(on_message)
    try:
        await broker.publish(
            body,
            exchange=payments_exchange(),
            routing_key=ROUTING_KEY_NEW,
            message_id=str(uuid4()),
        )
        deadline = loop.time() + dlq_timeout_s
        while loop.time() < deadline:
            message = await dlq.get(no_ack=True, fail=False)
            if message is not None:
                dlq_messages.append(message)
                break
            await asyncio.sleep(0.2)
    finally:
        await queue.cancel(consumer_tag)
    return deliveries, dlq_messages


async def test_persistent_failure_reaches_dlq_with_monotonic_counter(
    retry_world: Any,
) -> None:
    broker, settings, channel = retry_world
    body = make_event_json()
    deliveries, dlq_messages = await drive(
        broker, channel, AlwaysFailingProcessor(), settings, body, dlq_timeout_s=20.0
    )
    assert len(dlq_messages) == 1
    dead = dlq_messages[0]
    assert dead.body == body
    headers: dict[str, Any] = dict(dead.headers or {})
    assert "x-death" in headers
    x_death: Any = headers["x-death"]
    assert x_death[0]["count"] >= 1
    counters = [int(header.get("x-retry-count", 0)) for header in deliveries]
    assert counters == [0, 1, 2, 3]


async def test_recovered_message_is_never_dead_lettered(
    retry_world: Any,
) -> None:
    broker, settings, channel = retry_world
    body = make_event_json()
    processor = RecoveringProcessor(failures=2)
    deliveries, dlq_messages = await drive(
        broker, channel, processor, settings, body, dlq_timeout_s=4.0
    )
    assert processor.calls == 3
    headers_sequence = [dict(h) for h in deliveries]
    counters = [int(header.get("x-retry-count", 0)) for header in headers_sequence]
    assert counters == [0, 1, 2]
    assert dlq_messages == []
