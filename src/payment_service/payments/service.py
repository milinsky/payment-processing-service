import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

import structlog
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.outbox.models import Outbox
from payment_service.payments.exceptions import IdempotencyKeyConflict
from payment_service.payments.models import Payment, PaymentStatus
from payment_service.payments.schemas import PaymentCreate

logger = structlog.get_logger(__name__)

IDEMPOTENCY_CONSTRAINT_NAME = "payments_idempotency_key_key"
UNIQUE_VIOLATION_SQLSTATE = "23505"


def _is_idempotency_violation(exc: IntegrityError) -> bool:
    orig = exc.orig
    sqlstate = getattr(orig, "sqlstate", None)
    if sqlstate is not None and str(sqlstate) != UNIQUE_VIOLATION_SQLSTATE:
        return False
    diag = getattr(orig, "diag", None)
    constraint_name: object = getattr(diag, "constraint_name", None)
    if constraint_name is None:
        return True
    return str(constraint_name) == IDEMPOTENCY_CONSTRAINT_NAME


@dataclass(frozen=True, slots=True)
class PaymentCreationResult:
    payment: Payment
    replayed: bool


def canonical_json(model: BaseModel) -> str:
    return json.dumps(
        model.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )


def fingerprint(cmd: PaymentCreate) -> str:
    return hashlib.sha256(canonical_json(cmd).encode()).hexdigest()


async def get_by_id(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def _get_by_key(session: AsyncSession, idempotency_key: str) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


def _created_event(payment: Payment) -> Outbox:
    return Outbox(
        aggregate_type="payment",
        aggregate_id=payment.id,
        event_type="payment.created",
        payload={"payment_id": str(payment.id)},
    )


async def create_idempotent(
    session: AsyncSession,
    cmd: PaymentCreate,
    idempotency_key: str,
) -> PaymentCreationResult:
    payment = Payment(
        amount=cmd.amount,
        currency=cmd.currency,
        description=cmd.description,
        meta=cmd.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint(cmd),
        webhook_url=str(cmd.webhook_url),
    )
    session.add(payment)
    try:
        await session.flush()
        session.add(_created_event(payment))
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        if not _is_idempotency_violation(exc):
            raise
        existing = await _get_by_key(session, idempotency_key)
        if existing is None:
            raise
        if existing.request_fingerprint != payment.request_fingerprint:
            raise IdempotencyKeyConflict(idempotency_key) from None
        logger.info(
            "payment_replayed",
            payment_id=str(existing.id),
            idempotency_key=idempotency_key,
            replayed=True,
        )
        return PaymentCreationResult(existing, replayed=True)
    logger.info(
        "payment_created",
        payment_id=str(payment.id),
        idempotency_key=idempotency_key,
    )
    return PaymentCreationResult(payment, replayed=False)
