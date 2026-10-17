import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any
from uuid import uuid4

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.config import AppSettings
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import (
    QUEUE_NEW,
    declare_full_topology,
)
from payment_service.outbox.models import Outbox
from payment_service.outbox.relay import OutboxRelay

pytestmark = pytest.mark.integration

Drain = Callable[[int], Awaitable[list[AbstractIncomingMessage]]]


class CrashingPublishBroker:
    def __init__(self, broker: Any) -> None:
        self._broker = broker

    async def publish(self, *args: Any, **kwargs: Any) -> None:
        await self._broker.publish(*args, **kwargs)
        raise RuntimeError("crash after publish")


async def insert_outbox_rows(db_session: AsyncSession, count: int) -> list[Outbox]:
    rows = [
        Outbox(
            event_id=uuid4(),
            aggregate_type="payment",
            aggregate_id=uuid4(),
            event_type="payment.created",
            payload={"payment_id": "stub"},
        )
        for _ in range(count)
    ]
    db_session.add_all(rows)
    await db_session.flush()
    for row in rows:
        await db_session.refresh(row)
    await db_session.commit()
    return rows


@pytest_asyncio.fixture
async def topology_rabbit(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
) -> RabbitBroker:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    return clean_rabbit


@pytest_asyncio.fixture
async def drain_new_queue(
    rabbit_url: str,
) -> AsyncGenerator[Drain]:
    connection = await aio_pika.connect(rabbit_url)
    channel = await connection.channel()
    queue = await channel.get_queue(QUEUE_NEW)

    async def drain(count: int) -> list[AbstractIncomingMessage]:
        messages = []
        for _ in range(count):
            message = await queue.get(no_ack=True, fail=False)
            if message is None:
                break
            messages.append(message)
        return messages

    yield drain
    await connection.close()


def relay_maker(
    db_engine: AsyncEngine,
    broker: Any,
    settings: AppSettings,
) -> OutboxRelay:
    return OutboxRelay(
        broker,
        async_sessionmaker(db_engine, expire_on_commit=False),
        settings,
    )


async def test_run_once_publishes_and_marks(
    topology_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    drain_new_queue: Drain,
) -> None:
    rows = await insert_outbox_rows(db_session, 2)
    relay = relay_maker(db_engine, topology_rabbit, rabbit_settings)
    await relay.run_once()
    messages = await drain_new_queue(2)
    assert len(messages) == 2
    published_ids = {message.message_id for message in messages}
    assert published_ids == {str(row.event_id) for row in rows}
    for message in messages:
        event = PaymentCreatedEvent.model_validate_json(message.body.decode())
        assert str(event.event_id) == message.message_id
    db_session.expire_all()
    refreshed = (
        (await db_session.execute(select(Outbox).order_by(Outbox.id))).scalars().all()
    )
    assert all(row.dispatched_at is not None for row in refreshed)


async def test_second_run_once_is_noop(
    topology_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    drain_new_queue: Drain,
) -> None:
    await insert_outbox_rows(db_session, 1)
    relay = relay_maker(db_engine, topology_rabbit, rabbit_settings)
    await relay.run_once()
    assert len(await drain_new_queue(1)) == 1
    await relay.run_once()
    assert await drain_new_queue(1) == []


async def test_publish_then_crash_republishes_same_message_id(
    topology_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    drain_new_queue: Drain,
) -> None:
    rows = await insert_outbox_rows(db_session, 1)
    crashing_relay = relay_maker(
        db_engine, CrashingPublishBroker(topology_rabbit), rabbit_settings
    )
    await crashing_relay.run_once()
    first_messages = await drain_new_queue(1)
    assert len(first_messages) == 1
    row = (
        await db_session.execute(select(Outbox).where(Outbox.id == rows[0].id))
    ).scalar_one()
    assert row.dispatched_at is None
    healthy_relay = relay_maker(db_engine, topology_rabbit, rabbit_settings)
    await healthy_relay.run_once()
    second_messages = await drain_new_queue(1)
    assert len(second_messages) == 1
    assert second_messages[0].message_id == str(rows[0].event_id)
    assert second_messages[0].message_id == first_messages[0].message_id


async def test_concurrent_run_once_each_row_exactly_once(
    topology_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    drain_new_queue: Drain,
) -> None:
    rows = await insert_outbox_rows(db_session, 2)
    first = relay_maker(db_engine, topology_rabbit, rabbit_settings)
    second = relay_maker(db_engine, topology_rabbit, rabbit_settings)
    await asyncio.gather(first.run_once(), second.run_once())
    messages = await drain_new_queue(2)
    assert len(messages) == 2
    message_ids = [message.message_id for message in messages]
    assert len(set(message_ids)) == 2
    assert set(message_ids) == {str(row.event_id) for row in rows}
    db_session.expire_all()
    refreshed = (
        (await db_session.execute(select(Outbox).order_by(Outbox.id))).scalars().all()
    )
    assert all(row.dispatched_at is not None for row in refreshed)


async def test_cleanup_expired_removes_only_aged_dispatched_rows(
    topology_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    drain_new_queue: Drain,
) -> None:
    aged, fresh = await insert_outbox_rows(db_session, 2)
    relay = relay_maker(db_engine, topology_rabbit, rabbit_settings)
    await relay.run_once()
    await drain_new_queue(2)
    (undispatched,) = await insert_outbox_rows(db_session, 1)
    await db_session.execute(
        sa_text(
            "UPDATE outbox SET dispatched_at = now() - interval '2 days'"
            " WHERE id = :row_id"
        ),
        {"row_id": aged.id},
    )
    await db_session.commit()

    removed = await relay.cleanup_expired()
    assert removed == 1

    db_session.expire_all()
    remaining = (
        (await db_session.execute(select(Outbox).order_by(Outbox.id))).scalars().all()
    )
    assert [row.id for row in remaining] == [fresh.id, undispatched.id]
