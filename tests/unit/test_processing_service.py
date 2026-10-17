import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.payments.models import Currency, Payment, PaymentStatus
from payment_service.processing.gateway import GatewayResult, GatewayStatus
from payment_service.processing.service import PaymentProcessor, ProcessingOutcome
from payment_service.processing.webhook import WebhookDeliveryError, WebhookPayload

pytestmark = pytest.mark.unit

PROCESSED_AT = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)


class FakeSession:
    def __init__(self, store: dict[UUID, Payment], journal: list[str]) -> None:
        self._store = store
        self._journal = journal
        self.locked_with_for_update: list[bool] = []

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def get(
        self,
        model: type[object],
        payment_id: UUID,
        with_for_update: bool = False,
    ) -> Payment | None:
        self._journal.append("get")
        self.locked_with_for_update.append(with_for_update)
        return self._store.get(payment_id)

    async def commit(self) -> None:
        self._journal.append("commit")

    async def refresh(
        self,
        instance: object,
        attribute_names: list[str] | None = None,
    ) -> None:
        self._journal.append("refresh")
        if isinstance(instance, Payment):
            instance.processed_at = PROCESSED_AT

    async def execute(self, statement: Any) -> Any:
        self._journal.append("execute")
        params = statement.compile().params
        target_id = params.get("id_1")
        if "webhook_delivered_at" in str(statement):
            assert isinstance(target_id, UUID)
            payment = self._store[target_id]
            payment.webhook_delivered_at = datetime.now(UTC)
        return None


class FakeSessionmaker:
    def __init__(self, store: dict[UUID, Payment], journal: list[str]) -> None:
        self._store = store
        self._journal = journal
        self.sessions: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        session = FakeSession(self._store, self._journal)
        self.sessions.append(session)
        return session


class FakeGateway:
    def __init__(
        self,
        journal: list[str],
        result: GatewayResult | BaseException,
        delay_s: float = 0.0,
    ) -> None:
        self._journal = journal
        self._result = result
        self._delay_s = delay_s
        self.calls: list[tuple[UUID, Decimal, Currency]] = []

    async def process(
        self, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult:
        self._journal.append("gateway")
        self.calls.append((payment_id, amount, currency))
        if self._delay_s:
            await asyncio.sleep(self._delay_s)
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


class FakeWebhook:
    def __init__(self, journal: list[str]) -> None:
        self._journal = journal
        self.calls: list[tuple[str, WebhookPayload]] = []

    async def deliver(self, url: str, payload: WebhookPayload) -> None:
        self._journal.append("webhook")
        self.calls.append((url, payload))

    async def aclose(self) -> None:
        return None


def make_payment(
    status: PaymentStatus,
    webhook_delivered_at: datetime | None = None,
    webhook_attempts: int = 0,
    processed_at: datetime | None = None,
) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Test payment",
        idempotency_key=f"key-{uuid4()}",
        request_fingerprint="0" * 64,
        webhook_url="https://example.com/webhook",
        status=status,
        webhook_delivered_at=webhook_delivered_at,
        webhook_attempts=webhook_attempts,
        processed_at=processed_at,
    )


def make_processor(
    payment: Payment | None,
    gateway_result: GatewayResult | BaseException,
    gateway_delay_s: float = 0.0,
) -> tuple[
    PaymentProcessor,
    FakeSessionmaker,
    FakeGateway,
    FakeWebhook,
    list[str],
    Payment | None,
]:
    journal: list[str] = []
    store: dict[UUID, Payment] = {}
    if payment is not None:
        store[payment.id] = payment
    maker = FakeSessionmaker(store, journal)
    gateway = FakeGateway(journal, gateway_result, delay_s=gateway_delay_s)
    webhook = FakeWebhook(journal)
    maker_any: Any = maker
    webhook_any: Any = webhook
    processor = PaymentProcessor(maker_any, gateway, webhook_any)
    return processor, maker, gateway, webhook, journal, payment


def make_event(payment_id: UUID) -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=payment_id),
    )


async def test_pending_to_succeeded_orders_gateway_commit_webhook_commit() -> None:
    payment = make_payment(PaymentStatus.PENDING)
    processor, maker, gateway, webhook, journal, _ = make_processor(
        payment, GatewayResult(status=GatewayStatus.SUCCEEDED)
    )
    outcome = await processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert journal == [
        "get",
        "gateway",
        "commit",
        "refresh",
        "webhook",
        "execute",
        "commit",
    ]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at == PROCESSED_AT
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at is not None
    url, payload = webhook.calls[0]
    assert url == payment.webhook_url
    assert payload.type == "payment.succeeded"
    assert payload.status == "succeeded"
    assert payload.amount == Decimal("100.00")
    assert payload.occurred_at == PROCESSED_AT


async def test_payment_row_is_locked_with_for_update() -> None:
    payment = make_payment(PaymentStatus.PENDING)
    processor, maker, _, _, journal, _ = make_processor(
        payment, GatewayResult(status=GatewayStatus.SUCCEEDED)
    )
    await processor.handle(make_event(payment.id))
    assert maker.sessions[0].locked_with_for_update == [True]


async def test_pending_to_failed_is_business_as_usual() -> None:
    payment = make_payment(PaymentStatus.PENDING)
    processor, maker, gateway, webhook, journal, _ = make_processor(
        payment, GatewayResult(status=GatewayStatus.FAILED)
    )
    outcome = await processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert payment.status is PaymentStatus.FAILED
    assert webhook.calls[0][1].type == "payment.failed"
    assert webhook.calls[0][1].status == "failed"


async def test_terminal_delivered_is_ack_duplicate() -> None:
    payment = make_payment(
        PaymentStatus.SUCCEEDED,
        webhook_delivered_at=datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC),
        webhook_attempts=1,
        processed_at=PROCESSED_AT,
    )
    processor, maker, gateway, webhook, journal, _ = make_processor(
        payment,
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    outcome = await processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_DUPLICATE
    assert gateway.calls == []
    assert webhook.calls == []
    assert journal == ["get"]


async def test_terminal_without_delivery_skips_gateway_and_resends() -> None:
    payment = make_payment(
        PaymentStatus.FAILED,
        webhook_delivered_at=None,
        webhook_attempts=1,
        processed_at=PROCESSED_AT,
    )
    processor, maker, gateway, webhook, journal, _ = make_processor(
        payment,
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    outcome = await processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert gateway.calls == []
    assert len(webhook.calls) == 1
    assert webhook.calls[0][1].type == "payment.failed"
    assert webhook.calls[0][1].occurred_at == PROCESSED_AT
    assert payment.webhook_attempts == 2
    assert payment.webhook_delivered_at is not None
    assert journal == ["get", "commit", "webhook", "execute", "commit"]


async def test_terminal_without_processed_at_is_rejected_poison() -> None:
    payment = make_payment(
        PaymentStatus.SUCCEEDED,
        webhook_delivered_at=None,
        webhook_attempts=1,
        processed_at=None,
    )
    processor, maker, gateway, webhook, journal, _ = make_processor(
        payment,
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    outcome = await processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.REJECTED_POISON
    assert gateway.calls == []
    assert webhook.calls == []


async def test_missing_payment_is_rejected_poison() -> None:
    processor, maker, gateway, webhook, journal, _ = make_processor(
        None,
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    outcome = await processor.handle(make_event(uuid4()))
    assert outcome is ProcessingOutcome.REJECTED_POISON
    assert gateway.calls == []
    assert webhook.calls == []
    assert journal == ["get"]


async def test_webhook_failure_keeps_status_and_nulls_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = make_payment(PaymentStatus.PENDING)
    processor, maker, gateway, webhook, journal, stored = make_processor(
        payment, GatewayResult(status=GatewayStatus.SUCCEEDED)
    )

    async def failing_deliver(url: str, payload: WebhookPayload) -> None:
        await FakeWebhook.deliver(webhook, url, payload)
        raise WebhookDeliveryError(url, status_code=500)

    monkeypatch.setattr(webhook, "deliver", failing_deliver)
    with pytest.raises(WebhookDeliveryError):
        await processor.handle(make_event(payment.id))
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None
    assert stored.webhook_attempts == 1
    assert journal == ["get", "gateway", "commit", "refresh", "webhook"]


async def test_gateway_failure_leaves_no_partial_commit() -> None:
    payment = make_payment(PaymentStatus.PENDING)
    processor, maker, gateway, webhook, journal, stored = make_processor(
        payment, RuntimeError("gateway exploded")
    )
    with pytest.raises(RuntimeError):
        await processor.handle(make_event(payment.id))
    assert stored is not None
    assert stored.status is PaymentStatus.PENDING
    assert stored.webhook_attempts == 0
    assert stored.webhook_delivered_at is None
    assert "commit" not in journal
    assert webhook.calls == []
