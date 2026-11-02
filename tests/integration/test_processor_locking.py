import asyncio
from decimal import Decimal
from time import monotonic
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.config import ProcessingSettings
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.payments.exceptions import PaymentBusyError
from payment_service.payments.models import Currency, Payment, PaymentStatus
from payment_service.processing.gateway import GatewayResult, GatewayStatus
from payment_service.processing.service import PaymentProcessor, ProcessingOutcome
from payment_service.processing.webhook import WebhookPayload

pytestmark = pytest.mark.integration

GATEWAY_DELAY_S = 0.3
LEASE_TTL_S = 25.0


class SlowGateway:
    def __init__(self, delay_s: float = GATEWAY_DELAY_S) -> None:
        self.calls: list[tuple[UUID, Decimal, Currency]] = []
        self.entered_gateway = asyncio.Event()
        self._delay_s = delay_s

    async def process(
        self, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult:
        self.calls.append((payment_id, amount, currency))
        self.entered_gateway.set()
        await asyncio.sleep(self._delay_s)
        return GatewayResult(status=GatewayStatus.SUCCEEDED)


class RecordingWebhook:
    def __init__(self) -> None:
        self.calls: list[tuple[str, WebhookPayload]] = []

    async def deliver(self, url: str, payload: WebhookPayload) -> None:
        self.calls.append((url, payload))

    async def aclose(self) -> None:
        return None


def make_pending_payment() -> Payment:
    return Payment(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="concurrency probe",
        idempotency_key=f"lock-{uuid4()}",
        request_fingerprint="0" * 64,
        webhook_url="https://example.com/hook",
    )


def make_event(payment_id: UUID) -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=payment_id),
    )


def make_processor(
    db_engine: AsyncEngine,
    gateway: SlowGateway,
    webhook: RecordingWebhook,
) -> PaymentProcessor:
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    webhook_any: Any = webhook
    settings = ProcessingSettings(lease_ttl_s=LEASE_TTL_S)
    return PaymentProcessor(maker, gateway, webhook_any, settings)


async def set_claim_lease(
    db_session: AsyncSession, payment_id: UUID, ttl_s: float
) -> None:
    await db_session.execute(
        text(
            "UPDATE payments SET processing_claimed_until"
            " = now() + make_interval(secs => :ttl_s) WHERE id = :id"
        ),
        {"ttl_s": ttl_s, "id": payment_id},
    )
    await db_session.commit()


async def test_concurrent_handle_runs_gateway_exactly_once(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()
    payment_id = payment.id

    gateway = SlowGateway()
    webhook = RecordingWebhook()
    processors = [make_processor(db_engine, gateway, webhook) for _ in range(2)]

    first_task = asyncio.create_task(processors[0].handle(make_event(payment.id)))
    await gateway.entered_gateway.wait()
    busy = False
    try:
        second: Any = await processors[1].handle(make_event(payment.id))
    except PaymentBusyError:
        busy = True
        second = None
    first = await first_task

    assert len(gateway.calls) == 1
    assert gateway.calls[0][0] == payment.id
    assert first is ProcessingOutcome.ACKED_BUSINESS
    assert busy
    assert second is None
    assert len(webhook.calls) == 1

    db_session.expire_all()
    stored = await db_session.get(Payment, payment_id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert stored.webhook_attempts == len(webhook.calls)


async def test_no_row_lock_held_during_gateway(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()
    payment_id = payment.id

    gateway = SlowGateway()
    webhook = RecordingWebhook()
    processor = make_processor(db_engine, gateway, webhook)
    handle_task = asyncio.create_task(processor.handle(make_event(payment_id)))
    await gateway.entered_gateway.wait()

    probe_maker = async_sessionmaker(db_engine, expire_on_commit=False)
    async with probe_maker() as probe_session:
        locked = await probe_session.execute(
            select(Payment).where(Payment.id == payment_id).with_for_update(nowait=True)
        )
        assert locked.scalar_one().id == payment_id

    outcome = await handle_task
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert len(gateway.calls) == 1

    db_session.expire_all()
    stored = await db_session.get(Payment, payment_id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None


async def test_hung_claim_blocks_then_expired_lease_reclaims(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()
    payment_id = payment.id

    gateway = SlowGateway()
    webhook = RecordingWebhook()
    processor = make_processor(db_engine, gateway, webhook)

    await set_claim_lease(db_session, payment_id, 60.0)
    with pytest.raises(PaymentBusyError):
        await processor.handle(make_event(payment_id))
    assert gateway.calls == []

    await set_claim_lease(db_session, payment_id, -1.0)
    outcome = await processor.handle(make_event(payment_id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert len(gateway.calls) == 1

    db_session.expire_all()
    stored = await db_session.get(Payment, payment_id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None


async def test_gateway_timeout_leaves_pending_then_expired_lease_reclaims(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()
    payment_id = payment.id

    hung_gateway = SlowGateway(delay_s=60.0)
    webhook = RecordingWebhook()
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    webhook_any: Any = webhook
    settings = ProcessingSettings(timeout_s=0.1, lease_ttl_s=LEASE_TTL_S)
    processor = PaymentProcessor(maker, hung_gateway, webhook_any, settings)

    with pytest.raises(TimeoutError):
        await processor.handle(make_event(payment_id))

    db_session.expire_all()
    stored = await db_session.get(Payment, payment_id)
    assert stored is not None
    assert stored.status is PaymentStatus.PENDING
    assert len(hung_gateway.calls) == 1
    assert webhook.calls == []

    await set_claim_lease(db_session, payment_id, -1.0)
    fast_gateway = SlowGateway()
    retried = make_processor(db_engine, fast_gateway, webhook)
    outcome = await retried.handle(make_event(payment_id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert len(fast_gateway.calls) == 1

    db_session.expire_all()
    stored = await db_session.get(Payment, payment_id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert len(hung_gateway.calls) + len(fast_gateway.calls) == 2


async def test_busy_handle_returns_quickly(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()

    gateway = SlowGateway()
    webhook = RecordingWebhook()
    processor = make_processor(db_engine, gateway, webhook)

    await set_claim_lease(db_session, payment.id, 60.0)
    start = monotonic()
    with pytest.raises(PaymentBusyError):
        await processor.handle(make_event(payment.id))
    elapsed = monotonic() - start
    assert elapsed < 0.5
    assert gateway.calls == []


async def test_sequential_redelivery_after_delivery_is_duplicate(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()

    gateway = SlowGateway()
    webhook = RecordingWebhook()
    processor = make_processor(db_engine, gateway, webhook)
    first = await processor.handle(make_event(payment.id))
    second = await processor.handle(make_event(payment.id))
    assert first is ProcessingOutcome.ACKED_BUSINESS
    assert second is ProcessingOutcome.ACKED_DUPLICATE
    assert len(gateway.calls) == 1
    assert len(webhook.calls) == 1
