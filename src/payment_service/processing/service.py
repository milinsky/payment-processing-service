import asyncio
import enum
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import ProcessingSettings
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.payments.exceptions import PaymentBusyError
from payment_service.payments.models import Currency, Payment, PaymentStatus
from payment_service.payments.repository import PaymentRepository
from payment_service.processing.gateway import (
    GatewayStatus,
    PaymentGateway,
)
from payment_service.processing.webhook import WebhookClient, WebhookPayload

logger = structlog.get_logger(__name__)


class ProcessingOutcome(enum.StrEnum):
    ACKED_DUPLICATE = "acked_duplicate"
    ACKED_BUSINESS = "acked_business"
    REJECTED_POISON = "rejected_poison"


_TERMINAL_STATUSES = frozenset({PaymentStatus.SUCCEEDED, PaymentStatus.FAILED})

_TO_PAYMENT_STATUS = {
    GatewayStatus.SUCCEEDED: PaymentStatus.SUCCEEDED,
    GatewayStatus.FAILED: PaymentStatus.FAILED,
}


def _build_webhook_payload(
    status: PaymentStatus,
    event_id: UUID,
    payment_id: UUID,
    amount: Decimal,
    currency: Currency,
    processed_at: datetime,
) -> WebhookPayload:
    return WebhookPayload(
        type=f"payment.{status.value}",
        event_id=event_id,
        payment_id=payment_id,
        status=status.value,
        amount=amount,
        currency=currency,
        occurred_at=processed_at,
    )


class PaymentProcessor:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookClient,
        processing: ProcessingSettings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._gateway = gateway
        self._webhook = webhook
        self._processing = processing

    async def handle(self, event: PaymentCreatedEvent) -> ProcessingOutcome:
        payment = await self._claim(event.data.payment_id)
        if payment is not None:
            return await self._process_claimed(payment, event)
        return await self._handle_unclaimed(event)

    async def _claim(self, payment_id: UUID) -> Payment | None:
        async with self._sessionmaker() as session:
            repo = PaymentRepository(session)
            payment = await repo.claim_for_processing(
                payment_id, self._processing.lease_ttl_s
            )
            await repo.commit()
            return payment

    async def _process_claimed(
        self, payment: Payment, event: PaymentCreatedEvent
    ) -> ProcessingOutcome:
        async with asyncio.timeout(self._processing.timeout_s):
            result = await self._gateway.process(
                payment.id, payment.amount, payment.currency
            )
        status = _TO_PAYMENT_STATUS[result.status]
        async with self._sessionmaker() as session:
            repo = PaymentRepository(session)
            processed_at = await repo.finalize_processing(payment.id, status)
            if processed_at is None:
                stored = await repo.get(payment.id)
                await repo.commit()
                logger.warning(
                    "finalize_skipped_not_pending",
                    payment_id=str(payment.id),
                    event_id=str(event.event_id),
                    status=stored.status.value if stored is not None else None,
                )
                return ProcessingOutcome.ACKED_DUPLICATE
            await repo.commit()
        payload = _build_webhook_payload(
            status,
            event.event_id,
            payment.id,
            payment.amount,
            payment.currency,
            processed_at,
        )
        await self._webhook.deliver(payment.webhook_url, payload)
        async with self._sessionmaker() as session:
            repo = PaymentRepository(session)
            await repo.mark_webhook_delivered(payment.id)
            await repo.commit()
        return ProcessingOutcome.ACKED_BUSINESS

    async def _handle_unclaimed(self, event: PaymentCreatedEvent) -> ProcessingOutcome:
        async with self._sessionmaker() as session:
            repo = PaymentRepository(session)
            payment = await repo.get(event.data.payment_id)
            if payment is None:
                logger.warning(
                    "payment_not_found",
                    payment_id=str(event.data.payment_id),
                    event_id=str(event.event_id),
                )
                return ProcessingOutcome.REJECTED_POISON
            if payment.status not in _TERMINAL_STATUSES:
                raise PaymentBusyError("payment processing row is claimed elsewhere")
            if payment.webhook_delivered_at is not None:
                return ProcessingOutcome.ACKED_DUPLICATE
            if payment.processed_at is None:
                logger.warning(
                    "payment_missing_processed_at",
                    payment_id=str(payment.id),
                    event_id=str(event.event_id),
                )
                return ProcessingOutcome.REJECTED_POISON
            processed_at = payment.processed_at
        return await self._redeliver_webhook(payment, event, processed_at)

    async def _redeliver_webhook(
        self,
        payment: Payment,
        event: PaymentCreatedEvent,
        processed_at: datetime,
    ) -> ProcessingOutcome:
        async with self._sessionmaker() as session:
            repo = PaymentRepository(session)
            await repo.increment_webhook_attempts(payment.id)
            await repo.commit()
        payload = _build_webhook_payload(
            payment.status,
            event.event_id,
            payment.id,
            payment.amount,
            payment.currency,
            processed_at,
        )
        await self._webhook.deliver(payment.webhook_url, payload)
        async with self._sessionmaker() as session:
            repo = PaymentRepository(session)
            await repo.mark_webhook_delivered(payment.id)
            await repo.commit()
        return ProcessingOutcome.ACKED_BUSINESS
