import asyncio
import subprocess
import threading
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic
from uuid import uuid4

import httpx
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from integration.test_dlq_scenario import (
    CONSUMER_ENV,
    MANAGEMENT_HOST,
    MANAGEMENT_PORT,
    fetch_payment,
    poll_dlq,
    publish_event,
    read_consumer_logs,
    read_startup_failure,
    spawn_consumer,
)
from payment_service.config import AppSettings
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.messaging.topology import QUEUE_NEW
from payment_service.payments.models import Payment, PaymentStatus
from payment_service.payments.repository import PaymentRepository
from payment_service.payments.schemas import PaymentCreate
from payment_service.payments.service import create_idempotent

pytestmark = pytest.mark.integration

CRASH_LEASE_S = 2.0


class _AcceptingHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return None


@pytest.fixture
def webhook_base_url() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _AcceptingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(server.server_address[1])
        yield f"http://127.0.0.1:{port}/hook"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


async def _wait_consumer_declared(process: subprocess.Popen[bytes]) -> None:
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


@asynccontextmanager
async def _run_consumer(
    env: dict[str, str],
) -> AsyncGenerator[subprocess.Popen[bytes]]:
    process = spawn_consumer(env)
    try:
        await _wait_consumer_declared(process)
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


async def _wait_terminal(
    db_engine: AsyncEngine, payment_id: object, timeout_s: float
) -> Payment:
    deadline = monotonic() + timeout_s
    stored = await fetch_payment(db_engine, payment_id)
    while monotonic() < deadline and stored.status is PaymentStatus.PENDING:
        await asyncio.sleep(0.3)
        stored = await fetch_payment(db_engine, payment_id)
    assert stored.status is not PaymentStatus.PENDING
    return stored


async def _make_payment(
    db_session: AsyncSession, webhook_url: str, key_prefix: str
) -> object:
    creation = await create_idempotent(
        PaymentRepository(db_session),
        PaymentCreate(
            amount="100.00",
            currency="RUB",
            description="qa crash recovery",
            webhook_url=webhook_url,
        ),
        f"{key_prefix}-{uuid4()}",
    )
    return creation.payment.id


async def test_crash_after_claim_is_reclaimed_by_redelivery(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    webhook_base_url: str,
) -> None:
    payment_id = await _make_payment(db_session, webhook_base_url, "qa-crash")
    await db_session.execute(
        text(
            "UPDATE payments SET processing_claimed_until"
            " = now() + make_interval(secs => :ttl_s) WHERE id = :id"
        ),
        {"ttl_s": CRASH_LEASE_S, "id": payment_id},
    )
    await db_session.commit()
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=payment_id),
    )

    async with _run_consumer(CONSUMER_ENV) as process:
        await publish_event(event)
        stored = await _wait_terminal(db_engine, payment_id, timeout_s=25.0)
        dead = await poll_dlq(timeout_s=2.0)
        logs = read_consumer_logs(process)

    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert stored.webhook_attempts == 1
    assert dead is None
    assert any(entry.get("event") == "message_retry_scheduled" for entry in logs)


async def test_persistent_gateway_timeout_reaches_dlq_and_keeps_pending(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    clean_rabbit: RabbitBroker,
    rabbit_settings: AppSettings,
    webhook_base_url: str,
) -> None:
    payment_id = await _make_payment(db_session, webhook_base_url, "qa-timeout")
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=payment_id),
    )
    hung_env = {
        **CONSUMER_ENV,
        "GATEWAY_DELAY_MIN_S": "60",
        "GATEWAY_DELAY_MAX_S": "60",
        "GATEWAY_TIMEOUT_S": "0.1",
    }

    async with _run_consumer(hung_env):
        await publish_event(event)
        dead = await poll_dlq(timeout_s=20.0)
        stored = await fetch_payment(db_engine, payment_id)

    assert dead is not None
    assert dead.body == event.model_dump_json().encode()
    assert "x-death" in dict(dead.headers or {})
    assert stored.status is PaymentStatus.PENDING
    assert stored.processed_at is None
    assert stored.webhook_delivered_at is None
    assert stored.webhook_attempts == 0
