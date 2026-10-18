import asyncio
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.payments.models import Currency, Payment, PaymentStatus
from payment_service.processing.gateway import GatewayResult, GatewayStatus
from payment_service.processing.service import PaymentProcessor, ProcessingOutcome
from payment_service.processing.webhook import WebhookPayload

pytestmark = pytest.mark.integration

GATEWAY_DELAY_S = 0.3


class SlowGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, Decimal, Currency]] = []

    async def process(
        self, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult:
        self.calls.append((payment_id, amount, currency))
        await asyncio.sleep(GATEWAY_DELAY_S)
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
    webhook_any: Any = webhook
    processors = [
        PaymentProcessor(
            async_sessionmaker(db_engine, expire_on_commit=False), gateway, webhook_any
        )
        for _ in range(2)
    ]
    outcomes = await asyncio.gather(
        processors[0].handle(make_event(payment.id)),
        processors[1].handle(make_event(payment.id)),
        return_exceptions=True,
    )

    assert len(gateway.calls) == 1
    assert gateway.calls[0][0] == payment.id
    assert all(not isinstance(outcome, BaseException) for outcome in outcomes)
    assert {
        outcome for outcome in outcomes if isinstance(outcome, ProcessingOutcome)
    } <= {
        ProcessingOutcome.ACKED_BUSINESS,
        ProcessingOutcome.ACKED_DUPLICATE,
    }
    assert 1 <= len(webhook.calls) <= 2

    db_session.expire_all()
    stored = await db_session.get(Payment, payment_id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert stored.webhook_attempts == len(webhook.calls)


async def test_sequential_redelivery_after_delivery_is_duplicate(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    payment = make_pending_payment()
    db_session.add(payment)
    await db_session.commit()

    gateway = SlowGateway()
    webhook = RecordingWebhook()
    webhook_any: Any = webhook
    processor = PaymentProcessor(
        async_sessionmaker(db_engine, expire_on_commit=False), gateway, webhook_any
    )
    first: Any = await processor.handle(make_event(payment.id))
    second: Any = await processor.handle(make_event(payment.id))
    assert first is ProcessingOutcome.ACKED_BUSINESS
    assert second is ProcessingOutcome.ACKED_DUPLICATE
    assert len(gateway.calls) == 1
    assert len(webhook.calls) == 1
