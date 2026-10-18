import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from time import monotonic
from urllib.parse import urlsplit
from uuid import uuid4

import aio_pika
import httpx
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from integration.conftest import _delete_payments_entities
from payment_service.config import AppSettings, get_settings
from payment_service.messaging.events import (
    PaymentCreatedEvent,
    PaymentEventData,
)
from payment_service.messaging.topology import (
    QUEUE_DLQ,
    QUEUE_NEW,
    ROUTING_KEY_NEW,
    payments_exchange,
)
from payment_service.payments.models import Payment, PaymentStatus
from payment_service.payments.schemas import PaymentCreate
from payment_service.payments.service import create_idempotent

pytestmark = pytest.mark.integration

RABBIT_URL = os.environ.get("RABBIT_URL", "amqp://payments:payments@localhost:5672/")
MANAGEMENT_HOST = urlsplit(RABBIT_URL).hostname or "localhost"
MANAGEMENT_PORT = os.environ.get("MANAGEMENT_PORT", "15672")

CONSUMER_ENV = {
    "API_KEYS": "integration",
    "DB_HOST": os.environ.get("DB_HOST", "localhost"),
    "DB_PORT": os.environ.get("DB_PORT", "55432"),
    "DB_USER": os.environ.get("DB_USER", "payments"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD", "payments"),
    "DB_NAME": os.environ.get("DB_NAME", "payments"),
    "RABBIT_URL": RABBIT_URL,
    "RETRY_TTLS_MS": "1000,1000,1000",
    "RETRY_MAX_ATTEMPTS": "3",
    "GATEWAY_SUCCESS_RATE": "1.0",
    "GATEWAY_DELAY_MIN_S": "0",
    "GATEWAY_DELAY_MAX_S": "0",
    "WEBHOOK_TIMEOUT_S": "2",
}


@pytest.fixture
def consumer_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    for key, value in CONSUMER_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    return {**os.environ}


async def publish_event(event: PaymentCreatedEvent) -> None:
    publisher = RabbitBroker(RABBIT_URL)
    await publisher.connect()
    await publisher.publish(
        event.model_dump_json(),
        exchange=payments_exchange(),
        routing_key=ROUTING_KEY_NEW,
        message_id=str(event.event_id),
        persist=True,
    )
    await publisher.stop()


async def poll_dlq(timeout_s: float) -> AbstractIncomingMessage | None:
    connection = await aio_pika.connect(RABBIT_URL)
    try:
        channel = await connection.channel()
        queue = await channel.get_queue(QUEUE_DLQ)
        deadline = monotonic() + timeout_s
        while monotonic() < deadline:
            message = await queue.get(no_ack=True, fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.2)
        return None
    finally:
        await connection.close()


async def fetch_payment(db_engine: AsyncEngine, payment_id: object) -> Payment:
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    async with maker() as session:
        stored = await session.get(Payment, payment_id)
        assert stored is not None
        return stored


async def wait_for_attempts(
    db_engine: AsyncEngine,
    payment_id: object,
    expected: int,
    timeout_s: float = 8.0,
) -> Payment:
    deadline = monotonic() + timeout_s
    stored = await fetch_payment(db_engine, payment_id)
    while monotonic() < deadline and stored.webhook_attempts < expected:
        await asyncio.sleep(0.3)
        stored = await fetch_payment(db_engine, payment_id)
    assert stored.webhook_attempts == expected
    return stored


@pytest_asyncio.fixture
async def consumer_process(
    consumer_env: dict[str, str],
    rabbit_settings: AppSettings,
    clean_rabbit: RabbitBroker,
) -> AsyncGenerator[subprocess.Popen[bytes]]:
    await _delete_payments_entities(rabbit_settings)
    process = spawn_consumer(consumer_env)
    deadline = monotonic() + 20.0
    declared = False
    async with httpx.AsyncClient(
        base_url=f"http://{MANAGEMENT_HOST}:{MANAGEMENT_PORT}",
        auth=("payments", "payments"),
    ) as management:
        while monotonic() < deadline:
            response = await management.get(f"/api/queues/%2F/{QUEUE_NEW}")
            if response.status_code == 200:
                declared = True
                break
            await asyncio.sleep(0.2)
    assert declared, read_startup_failure(process)
    yield process
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def spawn_consumer(env: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "payment_service.consumer"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=os.getcwd(),
    )


def read_startup_failure(process: subprocess.Popen[bytes]) -> str:
    process.kill()
    process.wait()
    assert process.stdout is not None
    output = process.stdout.read().decode()
    return output[-3000:]


def read_consumer_logs(process: subprocess.Popen[bytes]) -> list[dict[str, object]]:
    process.terminate()
    process.wait()
    assert process.stdout is not None
    output = process.stdout.read().decode()
    parsed: list[dict[str, object]] = []
    for line in output.splitlines():
        if line.startswith("{") and line.endswith("}"):
            try:
                import json

                parsed.append(json.loads(line))
            except ValueError:
                continue
    return parsed


async def test_dead_webhook_reaches_dlq_payment_terminal_idempotent_redelivery(
    consumer_env: dict[str, str],
    consumer_process: subprocess.Popen[bytes],
    clean_rabbit: RabbitBroker,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    creation = await create_idempotent(
        db_session,
        PaymentCreate(
            amount="100.00",
            currency="RUB",
            description="DLQ scenario",
            webhook_url="http://127.0.0.1:9/hook",
        ),
        f"dlq-{uuid4()}",
    )
    payment_id = creation.payment.id
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=payment_id),
    )
    await publish_event(event)
    dead = await poll_dlq(timeout_s=20.0)
    assert dead is not None
    assert dead.body == event.model_dump_json().encode()
    assert "x-death" in dict(dead.headers or {})

    stored = await wait_for_attempts(db_engine, payment_id, expected=4)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None
    first_processed_at = stored.processed_at

    await publish_event(event)
    stored_again = await wait_for_attempts(db_engine, payment_id, expected=5)
    assert stored_again.processed_at == first_processed_at
    assert stored_again.status is PaymentStatus.SUCCEEDED
    assert stored_again.webhook_delivered_at is None

    logs = read_consumer_logs(consumer_process)
    correlated = [
        entry for entry in logs if "event_id" in entry and "payment_id" in entry
    ]
    assert correlated
