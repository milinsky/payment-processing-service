import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

import structlog
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError

from payment_service.payments.exceptions import IdempotencyKeyConflict
from payment_service.payments.models import Payment, PaymentStatus
from payment_service.payments.repository import PaymentRepository
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


async def get_by_id(repo: PaymentRepository, payment_id: UUID) -> Payment | None:
    return await repo.get(payment_id)


async def create_idempotent(
    repo: PaymentRepository,
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
    repo.add(payment)
    try:
        await repo.flush()
        repo.add_created_event(payment)
        await repo.commit()
    except IntegrityError as exc:
        await repo.rollback()
        if not _is_idempotency_violation(exc):
            raise
        existing = await repo.get_by_idempotency_key(idempotency_key)
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
