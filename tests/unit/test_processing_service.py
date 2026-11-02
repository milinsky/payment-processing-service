import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID, uuid4

import pytest
import structlog.testing

import payment_service.processing.service as processing_service
from payment_service.config import ProcessingSettings
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.payments.exceptions import PaymentBusyError
from payment_service.payments.models import Currency, Payment, PaymentStatus
from payment_service.processing.gateway import GatewayResult, GatewayStatus
from payment_service.processing.service import PaymentProcessor, ProcessingOutcome
from payment_service.processing.webhook import WebhookDeliveryError, WebhookPayload

pytestmark = pytest.mark.unit

FINALIZED_AT = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
DELIVERED_AT = datetime(2026, 11, 3, 12, 0, 1, tzinfo=UTC)
LEASE_TTL_S = 5.0
GATEWAY_TIMEOUT_S = 4.0


class FakeSession:
    def __init__(self, journal: list[str]) -> None:
        self._journal = journal

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._journal.append("close")
        return None


class FakeSessionmaker:
    def __init__(self, journal: list[str]) -> None:
        self._journal = journal
        self.opened = 0

    def __call__(self) -> FakeSession:
        self.opened += 1
        return FakeSession(self._journal)


class FakePaymentRepository:
    def __init__(
        self,
        store: dict[UUID, Payment],
        journal: list[str],
        claim_will_succeed: bool,
        finalize_processed_at: datetime | None,
    ) -> None:
        self._store = store
        self._journal = journal
        self._claim_will_succeed = claim_will_succeed
        self._finalize_processed_at = finalize_processed_at
        self.claimed_ttl_s: float | None = None

    async def claim_for_processing(
        self, payment_id: UUID, lease_ttl_s: float
    ) -> Payment | None:
        self._journal.append("claim")
        self.claimed_ttl_s = lease_ttl_s
        if not self._claim_will_succeed:
            return None
        return await self.get(payment_id)

    async def get(self, payment_id: UUID) -> Payment | None:
        self._journal.append("get")
        return self._store.get(payment_id)

    async def commit(self) -> None:
        self._journal.append("commit")

    async def finalize_processing(
        self, payment_id: UUID, status: PaymentStatus
    ) -> datetime | None:
        self._journal.append("finalize")
        payment = self._store.get(payment_id)
        if payment is None or self._finalize_processed_at is None:
            return None
        payment.status = status
        payment.processed_at = self._finalize_processed_at
        payment.webhook_attempts += 1
        return self._finalize_processed_at

    async def increment_webhook_attempts(self, payment_id: UUID) -> None:
        self._journal.append("increment_webhook_attempts")
        payment = self._store.get(payment_id)
        if payment is not None:
            payment.webhook_attempts += 1

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        self._journal.append("mark_delivered")
        payment = self._store.get(payment_id)
        if payment is not None:
            payment.webhook_delivered_at = DELIVERED_AT


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


class RepositoryFactory:
    def __init__(
        self,
        store: dict[UUID, Payment],
        journal: list[str],
        claim_will_succeed: bool,
        finalize_processed_at: datetime | None,
    ) -> None:
        self._store = store
        self._journal = journal
        self._claim_will_succeed = claim_will_succeed
        self._finalize_processed_at = finalize_processed_at
        self.instances: list[FakePaymentRepository] = []

    def __call__(self, session: object) -> FakePaymentRepository:
        repository = FakePaymentRepository(
            self._store,
            self._journal,
            self._claim_will_succeed,
            self._finalize_processed_at,
        )
        self.instances.append(repository)
        return repository


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


class ProcessorFixture:
    def __init__(
        self,
        processor: PaymentProcessor,
        maker: FakeSessionmaker,
        gateway: FakeGateway,
        webhook: FakeWebhook,
        journal: list[str],
        factory: RepositoryFactory,
        payment: Payment | None,
    ) -> None:
        self.processor = processor
        self.maker = maker
        self.gateway = gateway
        self.webhook = webhook
        self.journal = journal
        self.factory = factory
        self.payment = payment


def make_processor(
    monkeypatch: pytest.MonkeyPatch,
    payment: Payment | None,
    gateway_result: GatewayResult | BaseException,
    *,
    claim_will_succeed: bool = True,
    finalize_processed_at: datetime | None = FINALIZED_AT,
) -> ProcessorFixture:
    journal: list[str] = []
    store: dict[UUID, Payment] = {}
    if payment is not None:
        store[payment.id] = payment
    factory = RepositoryFactory(
        store, journal, claim_will_succeed, finalize_processed_at
    )
    monkeypatch.setattr(processing_service, "PaymentRepository", factory)
    maker = FakeSessionmaker(journal)
    gateway = FakeGateway(journal, gateway_result)
    webhook = FakeWebhook(journal)
    settings = ProcessingSettings(timeout_s=GATEWAY_TIMEOUT_S, lease_ttl_s=LEASE_TTL_S)
    maker_any: Any = maker
    webhook_any: Any = webhook
    processor = PaymentProcessor(maker_any, gateway, webhook_any, settings)
    return ProcessorFixture(
        processor, maker, gateway, webhook, journal, factory, payment
    )


def make_event(payment_id: UUID) -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=payment_id),
    )


async def test_claim_path_orders_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    payment = fixture.payment
    assert payment is not None
    outcome = await fixture.processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert fixture.journal == [
        "claim",
        "get",
        "commit",
        "close",
        "gateway",
        "finalize",
        "commit",
        "close",
        "webhook",
        "mark_delivered",
        "commit",
        "close",
    ]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at == FINALIZED_AT
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at == DELIVERED_AT
    assert len(fixture.webhook.calls) == 1
    url, payload = fixture.webhook.calls[0]
    assert url == payment.webhook_url
    assert payload.type == "payment.succeeded"
    assert payload.status == "succeeded"
    assert payload.amount == Decimal("100.00")
    assert payload.occurred_at == FINALIZED_AT
    assert fixture.gateway.calls == [(payment.id, payment.amount, payment.currency)]


async def test_claim_forwards_lease_ttl_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    payment = fixture.payment
    assert payment is not None
    await fixture.processor.handle(make_event(payment.id))
    assert fixture.factory.instances[0].claimed_ttl_s == LEASE_TTL_S


async def test_busy_raises_without_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
        claim_will_succeed=False,
    )
    payment = fixture.payment
    assert payment is not None
    with pytest.raises(PaymentBusyError):
        await fixture.processor.handle(make_event(payment.id))
    assert fixture.gateway.calls == []
    assert fixture.webhook.calls == []
    assert fixture.journal == ["claim", "commit", "close", "get", "close"]


async def test_finalize_zero_rows_returns_duplicate_without_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
        finalize_processed_at=None,
    )
    payment = fixture.payment
    assert payment is not None
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr(processing_service, "logger", capture)
    outcome = await fixture.processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_DUPLICATE
    assert len(fixture.gateway.calls) == 1
    assert fixture.webhook.calls == []
    assert fixture.journal == [
        "claim",
        "get",
        "commit",
        "close",
        "gateway",
        "finalize",
        "get",
        "commit",
        "close",
    ]
    assert payment.status is PaymentStatus.PENDING
    assert payment.webhook_attempts == 0
    warnings = [call for call in capture.calls if call.method_name == "warning"]
    assert warnings
    assert warnings[0].kwargs["payment_id"] == str(payment.id)


async def test_terminal_delivered_is_ack_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(
            PaymentStatus.SUCCEEDED,
            webhook_delivered_at=FINALIZED_AT,
            webhook_attempts=1,
            processed_at=FINALIZED_AT,
        ),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
        claim_will_succeed=False,
    )
    payment = fixture.payment
    assert payment is not None
    outcome = await fixture.processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_DUPLICATE
    assert fixture.gateway.calls == []
    assert fixture.webhook.calls == []
    assert fixture.journal == ["claim", "commit", "close", "get", "close"]


async def test_terminal_without_delivery_skips_gateway_and_resends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(
            PaymentStatus.FAILED,
            webhook_delivered_at=None,
            webhook_attempts=1,
            processed_at=FINALIZED_AT,
        ),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
        claim_will_succeed=False,
    )
    payment = fixture.payment
    assert payment is not None
    outcome = await fixture.processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert fixture.gateway.calls == []
    assert len(fixture.webhook.calls) == 1
    assert fixture.webhook.calls[0][1].type == "payment.failed"
    assert fixture.webhook.calls[0][1].occurred_at == FINALIZED_AT
    assert payment.webhook_attempts == 2
    assert payment.webhook_delivered_at == DELIVERED_AT
    assert fixture.journal == [
        "claim",
        "commit",
        "close",
        "get",
        "close",
        "increment_webhook_attempts",
        "commit",
        "close",
        "webhook",
        "mark_delivered",
        "commit",
        "close",
    ]


async def test_terminal_without_processed_at_is_rejected_poison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(
            PaymentStatus.SUCCEEDED,
            webhook_delivered_at=None,
            webhook_attempts=1,
            processed_at=None,
        ),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
        claim_will_succeed=False,
    )
    payment = fixture.payment
    assert payment is not None
    outcome = await fixture.processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.REJECTED_POISON
    assert fixture.gateway.calls == []
    assert fixture.webhook.calls == []
    assert payment.webhook_attempts == 1
    assert fixture.journal == ["claim", "commit", "close", "get", "close"]


async def test_missing_payment_is_rejected_poison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        None,
        GatewayResult(status=GatewayStatus.SUCCEEDED),
        claim_will_succeed=False,
    )
    outcome = await fixture.processor.handle(make_event(uuid4()))
    assert outcome is ProcessingOutcome.REJECTED_POISON
    assert fixture.gateway.calls == []
    assert fixture.webhook.calls == []
    assert fixture.journal == ["claim", "commit", "close", "get", "close"]


async def test_pending_to_failed_is_business_as_usual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        GatewayResult(status=GatewayStatus.FAILED),
    )
    payment = fixture.payment
    assert payment is not None
    outcome = await fixture.processor.handle(make_event(payment.id))
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert payment.status is PaymentStatus.FAILED
    assert fixture.webhook.calls[0][1].type == "payment.failed"
    assert fixture.webhook.calls[0][1].status == "failed"


async def test_webhook_failure_keeps_status_and_nulls_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        GatewayResult(status=GatewayStatus.SUCCEEDED),
    )
    payment = fixture.payment
    assert payment is not None
    webhook = fixture.webhook

    async def failing_deliver(url: str, payload: WebhookPayload) -> None:
        await FakeWebhook.deliver(webhook, url, payload)
        raise WebhookDeliveryError(url, status_code=500)

    monkeypatch.setattr(webhook, "deliver", failing_deliver)
    with pytest.raises(WebhookDeliveryError):
        await fixture.processor.handle(make_event(payment.id))
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None
    assert payment.webhook_attempts == 1
    assert fixture.journal == [
        "claim",
        "get",
        "commit",
        "close",
        "gateway",
        "finalize",
        "commit",
        "close",
        "webhook",
    ]


async def test_gateway_failure_leaves_pending_with_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        RuntimeError("gateway exploded"),
    )
    payment = fixture.payment
    assert payment is not None
    with pytest.raises(RuntimeError):
        await fixture.processor.handle(make_event(payment.id))
    assert payment.status is PaymentStatus.PENDING
    assert payment.webhook_attempts == 0
    assert payment.webhook_delivered_at is None
    assert len(fixture.gateway.calls) == 1
    assert fixture.webhook.calls == []
    assert "finalize" not in fixture.journal
    assert "mark_delivered" not in fixture.journal


async def test_gateway_timeout_leaves_pending_without_finalize_or_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_processor(
        monkeypatch,
        make_payment(PaymentStatus.PENDING),
        TimeoutError(),
    )
    payment = fixture.payment
    assert payment is not None
    with pytest.raises(TimeoutError):
        await fixture.processor.handle(make_event(payment.id))
    assert payment.status is PaymentStatus.PENDING
    assert payment.webhook_attempts == 0
    assert payment.webhook_delivered_at is None
    assert len(fixture.gateway.calls) == 1
    assert fixture.webhook.calls == []
    assert fixture.journal == ["claim", "get", "commit", "close", "gateway"]
    assert "finalize" not in fixture.journal
    assert "mark_delivered" not in fixture.journal
