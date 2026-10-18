import enum
from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.payments.models import Payment, PaymentStatus
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
    payment: Payment, event_id: UUID, processed_at: datetime
) -> WebhookPayload:
    return WebhookPayload(
        type=f"payment.{payment.status.value}",
        event_id=event_id,
        payment_id=payment.id,
        status=payment.status.value,
        amount=payment.amount,
        currency=payment.currency,
        occurred_at=processed_at,
    )


class PaymentProcessor:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookClient,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._gateway = gateway
        self._webhook = webhook

    async def handle(self, event: PaymentCreatedEvent) -> ProcessingOutcome:
        async with self._sessionmaker() as session:
            payment = await session.get(
                Payment, event.data.payment_id, with_for_update=True
            )
            if payment is None:
                logger.warning(
                    "payment_not_found",
                    payment_id=str(event.data.payment_id),
                    event_id=str(event.event_id),
                )
                return ProcessingOutcome.REJECTED_POISON
            if (
                payment.status in _TERMINAL_STATUSES
                and payment.webhook_delivered_at is not None
            ):
                return ProcessingOutcome.ACKED_DUPLICATE
            status_changed = False
            if payment.status is PaymentStatus.PENDING:
                result = await self._gateway.process(
                    payment.id, payment.amount, payment.currency
                )
                payment.status = _TO_PAYMENT_STATUS[result.status]
                payment.processed_at = func.now()
                status_changed = True
            payment.webhook_attempts += 1
            await session.commit()
            if status_changed:
                await session.refresh(payment, attribute_names=["processed_at"])
            processed_at = payment.processed_at
            if processed_at is None:
                logger.warning(
                    "payment_missing_processed_at",
                    payment_id=str(payment.id),
                    event_id=str(event.event_id),
                )
                return ProcessingOutcome.REJECTED_POISON
            payload = _build_webhook_payload(payment, event.event_id, processed_at)
            webhook_url = payment.webhook_url
            payment_id = payment.id
        await self._webhook.deliver(webhook_url, payload)
        async with self._sessionmaker() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(webhook_delivered_at=func.now())
            )
            await session.commit()
        return ProcessingOutcome.ACKED_BUSINESS
