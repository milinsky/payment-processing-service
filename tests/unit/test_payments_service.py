import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from payment_service.outbox.models import Outbox
from payment_service.payments.exceptions import IdempotencyKeyConflict
from payment_service.payments.models import Currency, Payment
from payment_service.payments.schemas import PaymentCreate
from payment_service.payments.service import (
    _is_idempotency_violation,
    create_idempotent,
    fingerprint,
)

pytestmark = pytest.mark.unit

EXPECTED_FINGERPRINT = (
    "e90b5c353e98efc6b2ddb7b91105111b5d6e3cb382d03e626599eb9d12f91c85"
)


def sample_cmd() -> PaymentCreate:
    return PaymentCreate(
        amount="100.00",
        currency=Currency.RUB,
        description="Test payment",
        metadata=None,
        webhook_url="https://example.com/webhook",
    )


def make_session() -> MagicMock:
    session = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=execute_result)
    return session


def existing_payment(cmd: PaymentCreate, key: str) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=cmd.amount,
        currency=cmd.currency,
        description=cmd.description,
        idempotency_key=key,
        request_fingerprint=fingerprint(cmd),
        webhook_url=str(cmd.webhook_url),
    )


def test_fingerprint_reference_vector() -> None:
    assert fingerprint(sample_cmd()) == EXPECTED_FINGERPRINT


async def test_insert_first_statement_order() -> None:
    session = make_session()
    await create_idempotent(session, sample_cmd(), "key-unit-1")
    call_names = [call[0] for call in session.method_calls]
    assert call_names == ["add", "flush", "add", "commit"]
    assert session.execute.await_count == 0


async def test_happy_path_adds_outbox_before_commit() -> None:
    session = make_session()
    result = await create_idempotent(session, sample_cmd(), "key-unit-2")
    assert result.replayed is False
    added = [call.args[0] for call in session.method_calls if call[0] == "add"]
    assert isinstance(added[0], Payment)
    assert isinstance(added[1], Outbox)
    event = added[1]
    assert event.event_type == "payment.created"
    assert event.aggregate_type == "payment"
    assert event.aggregate_id == result.payment.id
    assert event.payload == {"payment_id": str(result.payment.id)}
    commit_index = next(
        i for i, call in enumerate(session.method_calls) if call[0] == "commit"
    )
    add_indices = [i for i, call in enumerate(session.method_calls) if call[0] == "add"]
    assert all(index < commit_index for index in add_indices)


async def test_integrity_error_replays_existing_payment() -> None:
    session = make_session()
    cmd = sample_cmd()
    key = "key-unit-3"
    existing = existing_payment(cmd, key)
    session.flush = AsyncMock(side_effect=IntegrityError("stmt", {}, RuntimeError()))
    session.rollback = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = existing
    session.execute = AsyncMock(return_value=execute_result)
    result = await create_idempotent(session, cmd, key)
    assert result.replayed is True
    assert result.payment is existing
    session.rollback.assert_awaited_once()


async def test_integrity_error_with_other_fingerprint_conflicts() -> None:
    session = make_session()
    cmd = sample_cmd()
    key = "key-unit-4"
    other = Payment(
        id=uuid.uuid4(),
        amount=cmd.amount,
        currency=cmd.currency,
        description="different body",
        idempotency_key=key,
        request_fingerprint="f" * 64,
        webhook_url=str(cmd.webhook_url),
    )
    session.flush = AsyncMock(side_effect=IntegrityError("stmt", {}, RuntimeError()))
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = other
    session.execute = AsyncMock(return_value=execute_result)
    with pytest.raises(IdempotencyKeyConflict) as excinfo:
        await create_idempotent(session, cmd, key)
    assert excinfo.value.idempotency_key == key


async def test_foreign_integrity_error_is_reraised() -> None:
    session = make_session()
    session.flush = AsyncMock(side_effect=IntegrityError("stmt", {}, RuntimeError()))
    with pytest.raises(IntegrityError):
        await create_idempotent(session, sample_cmd(), "key-unit-5")
    session.rollback.assert_awaited_once()


def integrity_error_with_constraint(constraint_name: str | None) -> IntegrityError:
    orig = MagicMock()
    orig.sqlstate = "23505"
    orig.diag.constraint_name = constraint_name
    return IntegrityError("stmt", {}, orig)


def integrity_error_with_sqlstate(sqlstate: str) -> IntegrityError:
    orig = MagicMock()
    orig.sqlstate = sqlstate
    orig.diag.constraint_name = None
    return IntegrityError("stmt", {}, orig)


async def test_foreign_sqlstate_error_is_reraised_even_with_existing_row() -> None:
    session = make_session()
    cmd = sample_cmd()
    key = "key-unit-6"
    existing = existing_payment(cmd, key)
    session.flush = AsyncMock(side_effect=integrity_error_with_sqlstate("23502"))
    session.rollback = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = existing
    session.execute = AsyncMock(return_value=execute_result)
    with pytest.raises(IntegrityError):
        await create_idempotent(session, cmd, key)


async def test_idempotency_constraint_error_replays() -> None:
    session = make_session()
    cmd = sample_cmd()
    key = "key-unit-7"
    existing = existing_payment(cmd, key)
    session.flush = AsyncMock(
        side_effect=integrity_error_with_constraint("payments_idempotency_key_key")
    )
    session.rollback = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = existing
    session.execute = AsyncMock(return_value=execute_result)
    result = await create_idempotent(session, cmd, key)
    assert result.replayed is True


def test_is_idempotency_violation_decision_table() -> None:
    assert (
        _is_idempotency_violation(
            integrity_error_with_constraint("payments_idempotency_key_key")
        )
        is True
    )
    assert _is_idempotency_violation(integrity_error_with_constraint(None)) is True
    assert (
        _is_idempotency_violation(
            integrity_error_with_constraint("payments_amount_positive_check")
        )
        is False
    )
    assert _is_idempotency_violation(integrity_error_with_sqlstate("23502")) is False
    assert _is_idempotency_violation(integrity_error_with_sqlstate("23503")) is False
    bare = IntegrityError("stmt", {}, RuntimeError("no driver details"))
    assert _is_idempotency_violation(bare) is True
