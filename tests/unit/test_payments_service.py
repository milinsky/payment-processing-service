import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from payment_service.payments.exceptions import IdempotencyKeyConflict
from payment_service.payments.models import Currency, Payment
from payment_service.payments.schemas import PaymentCreate
from payment_service.payments.service import (
    _is_idempotency_violation,
    create_idempotent,
    fingerprint,
    get_by_id,
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


def make_repo() -> MagicMock:
    repo = MagicMock()
    repo.get = AsyncMock()
    repo.get_by_idempotency_key = AsyncMock(return_value=None)
    repo.add = MagicMock()
    repo.add_created_event = MagicMock()
    repo.flush = AsyncMock()
    repo.commit = AsyncMock()
    repo.rollback = AsyncMock()
    return repo


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


async def test_get_by_id_delegates_to_repository() -> None:
    repo = make_repo()
    payment_id = uuid.uuid4()
    await get_by_id(repo, payment_id)
    repo.get.assert_awaited_once_with(payment_id)


async def test_get_by_id_returns_payment_from_repository() -> None:
    repo = make_repo()
    payment = existing_payment(sample_cmd(), "key-get-by-id")
    repo.get = AsyncMock(return_value=payment)
    assert await get_by_id(repo, payment.id) is payment


async def test_insert_first_statement_order() -> None:
    repo = make_repo()
    await create_idempotent(repo, sample_cmd(), "key-unit-1")
    call_names = [call[0] for call in repo.method_calls]
    assert call_names == ["add", "flush", "add_created_event", "commit"]
    assert repo.get_by_idempotency_key.await_count == 0


async def test_happy_path_adds_payment_and_event_before_commit() -> None:
    repo = make_repo()
    result = await create_idempotent(repo, sample_cmd(), "key-unit-2")
    assert result.replayed is False
    forwarded = [
        call.args[0]
        for call in repo.method_calls
        if call[0] in {"add", "add_created_event"}
    ]
    assert isinstance(forwarded[0], Payment)
    assert forwarded[1] is result.payment
    commit_index = next(
        i for i, call in enumerate(repo.method_calls) if call[0] == "commit"
    )
    write_indices = [
        i
        for i, call in enumerate(repo.method_calls)
        if call[0] in {"add", "add_created_event"}
    ]
    assert all(index < commit_index for index in write_indices)


async def test_integrity_error_replays_existing_payment() -> None:
    repo = make_repo()
    cmd = sample_cmd()
    key = "key-unit-3"
    existing = existing_payment(cmd, key)
    repo.flush = AsyncMock(side_effect=IntegrityError("stmt", {}, RuntimeError()))
    repo.get_by_idempotency_key = AsyncMock(return_value=existing)
    result = await create_idempotent(repo, cmd, key)
    assert result.replayed is True
    assert result.payment is existing
    repo.rollback.assert_awaited_once()


async def test_integrity_error_with_other_fingerprint_conflicts() -> None:
    repo = make_repo()
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
    repo.flush = AsyncMock(side_effect=IntegrityError("stmt", {}, RuntimeError()))
    repo.get_by_idempotency_key = AsyncMock(return_value=other)
    with pytest.raises(IdempotencyKeyConflict) as excinfo:
        await create_idempotent(repo, cmd, key)
    assert excinfo.value.idempotency_key == key


async def test_foreign_integrity_error_is_reraised() -> None:
    repo = make_repo()
    repo.flush = AsyncMock(side_effect=IntegrityError("stmt", {}, RuntimeError()))
    with pytest.raises(IntegrityError):
        await create_idempotent(repo, sample_cmd(), "key-unit-5")
    repo.rollback.assert_awaited_once()


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
    repo = make_repo()
    cmd = sample_cmd()
    key = "key-unit-6"
    existing = existing_payment(cmd, key)
    repo.flush = AsyncMock(side_effect=integrity_error_with_sqlstate("23502"))
    repo.get_by_idempotency_key = AsyncMock(return_value=existing)
    with pytest.raises(IntegrityError):
        await create_idempotent(repo, cmd, key)


async def test_idempotency_constraint_error_replays() -> None:
    repo = make_repo()
    cmd = sample_cmd()
    key = "key-unit-7"
    existing = existing_payment(cmd, key)
    repo.flush = AsyncMock(
        side_effect=integrity_error_with_constraint("payments_idempotency_key_key")
    )
    repo.get_by_idempotency_key = AsyncMock(return_value=existing)
    result = await create_idempotent(repo, cmd, key)
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
