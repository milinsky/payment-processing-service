from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    Float,
    bindparam,
    func,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.outbox.models import Outbox
from payment_service.payments.models import Payment, PaymentStatus

_LEASE_INTERVAL_SQL = text("make_interval(secs => :lease_ttl_s)").bindparams(
    bindparam("lease_ttl_s", type_=Float())
)


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    def add_created_event(self, payment: Payment) -> None:
        self._session.add(
            Outbox(
                aggregate_type="payment",
                aggregate_id=payment.id,
                event_type="payment.created",
                payload={"payment_id": str(payment.id)},
            )
        )

    async def flush(self) -> None:
        await self._session.flush()

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def claim_for_processing(
        self, payment_id: UUID, lease_ttl_s: float
    ) -> Payment | None:
        result = await self._session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.status == PaymentStatus.PENDING,
                or_(
                    Payment.processing_claimed_until.is_(None),
                    Payment.processing_claimed_until < func.now(),
                ),
            )
            .values(processing_claimed_until=func.now() + _LEASE_INTERVAL_SQL)
            .returning(Payment.id),
            {"lease_ttl_s": lease_ttl_s},
        )
        claimed_id = result.scalar_one_or_none()
        if claimed_id is None:
            return None
        return await self.get(payment_id)

    async def finalize_processing(
        self, payment_id: UUID, status: PaymentStatus
    ) -> datetime | None:
        result = await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(
                status=status,
                processed_at=func.now(),
                webhook_attempts=Payment.webhook_attempts + 1,
            )
            .returning(Payment.processed_at)
        )
        return result.scalar_one_or_none()

    async def increment_webhook_attempts(self, payment_id: UUID) -> None:
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        await self._session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.webhook_delivered_at.is_(None),
            )
            .values(webhook_delivered_at=func.now())
        )
