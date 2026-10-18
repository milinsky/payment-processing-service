from collections.abc import AsyncGenerator
from urllib.parse import urlsplit

import pytest
import pytest_asyncio
from aio_pika.exceptions import ChannelPreconditionFailed
from faststream.rabbit import RabbitBroker, RabbitQueue
from httpx import AsyncClient

from payment_service.config import AppSettings, get_settings
from payment_service.messaging.topology import (
    DEAD_ROUTING_KEY,
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    QUEUE_DLQ,
    QUEUE_NEW,
    ROUTING_KEY_NEW,
    declare_full_topology,
    new_queue,
)

pytestmark = pytest.mark.integration

RETRY_QUEUES = ["payments.retry.1", "payments.retry.2", "payments.retry.3"]


@pytest_asyncio.fixture
async def management(
    rabbit_url: str,
) -> AsyncGenerator[AsyncClient]:
    parts = urlsplit(rabbit_url)
    base_url = f"http://{parts.hostname}:15672"
    auth = (parts.username or "payments", parts.password or "payments")
    async with AsyncClient(base_url=base_url, auth=auth) as client:
        yield client


async def fetch_queue_arguments(
    management: AsyncClient,
    queue_name: str,
) -> dict[str, object]:
    response = await management.get(f"/api/queues/%2F/{queue_name}")
    assert response.status_code == 200, response.text
    payload = response.json()
    arguments: dict[str, object] = payload.get("arguments") or {}
    return arguments


async def test_full_topology_declares_all_entities(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    management: AsyncClient,
) -> None:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    for queue_name in [QUEUE_NEW, *RETRY_QUEUES, QUEUE_DLQ]:
        response = await management.get(f"/api/queues/%2F/{queue_name}")
        assert response.status_code == 200, queue_name
    for exchange_name in [PAYMENTS_EXCHANGE, DLX_EXCHANGE]:
        response = await management.get(f"/api/exchanges/%2F/{exchange_name}")
        assert response.status_code == 200, exchange_name


async def test_new_queue_dead_letter_arguments(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    management: AsyncClient,
) -> None:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    arguments = await fetch_queue_arguments(management, QUEUE_NEW)
    assert arguments["x-dead-letter-exchange"] == DLX_EXCHANGE
    assert arguments["x-dead-letter-routing-key"] == DEAD_ROUTING_KEY


async def test_retry_queue_arguments_use_settings_ttls(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    management: AsyncClient,
) -> None:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    arguments = await fetch_queue_arguments(management, "payments.retry.2")
    assert arguments["x-message-ttl"] == rabbit_settings.processing.retry_ttls_ms[1]
    assert arguments["x-message-ttl"] == 10000
    assert arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE
    assert arguments["x-dead-letter-routing-key"] == ROUTING_KEY_NEW


async def test_bindings_are_in_place(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    management: AsyncClient,
) -> None:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    queue = await clean_rabbit.declare_queue(new_queue())
    await queue.bind(PAYMENTS_EXCHANGE, routing_key=new_queue().routing())
    new_bindings = (
        await management.get(f"/api/bindings/%2F/e/{PAYMENTS_EXCHANGE}/q/{QUEUE_NEW}")
    ).json()
    assert len(new_bindings) == 1
    assert new_bindings[0]["source"] == PAYMENTS_EXCHANGE
    assert new_bindings[0]["destination"] == QUEUE_NEW
    assert new_bindings[0]["routing_key"] == ROUTING_KEY_NEW
    dlq_bindings = (
        await management.get(f"/api/bindings/%2F/e/{DLX_EXCHANGE}/q/{QUEUE_DLQ}")
    ).json()
    assert len(dlq_bindings) == 1
    assert dlq_bindings[0]["source"] == DLX_EXCHANGE
    assert dlq_bindings[0]["destination"] == QUEUE_DLQ
    assert dlq_bindings[0]["routing_key"] == DEAD_ROUTING_KEY


async def test_declare_full_topology_is_idempotent(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
) -> None:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    await declare_full_topology(clean_rabbit, rabbit_settings)


async def test_retry_ttls_from_env_change_queue_arguments(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    monkeypatch: pytest.MonkeyPatch,
    management: AsyncClient,
) -> None:
    monkeypatch.setenv("RETRY_TTLS_MS", "1000,2000,3000")
    get_settings.cache_clear()
    settings = get_settings()
    await declare_full_topology(clean_rabbit, settings)
    for index, expected_ttl in enumerate([1000, 2000, 3000], start=1):
        arguments = await fetch_queue_arguments(management, f"payments.retry.{index}")
        assert arguments["x-message-ttl"] == expected_ttl


async def test_changed_arguments_fail_with_precondition(
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
) -> None:
    await declare_full_topology(clean_rabbit, rabbit_settings)
    conflicting = RabbitQueue(
        QUEUE_NEW,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": "different.key",
        },
    )
    with pytest.raises(ChannelPreconditionFailed):
        await clean_rabbit.declare_queue(conflicting)
