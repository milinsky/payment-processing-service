import asyncio

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.outbox.models import Outbox
from payment_service.payments.exceptions import IdempotencyKeyConflict
from payment_service.payments.models import Currency, Payment, PaymentStatus
from payment_service.payments.schemas import PaymentCreate
from payment_service.payments.service import create_idempotent

pytestmark = pytest.mark.integration


def make_cmd(amount: str = "100.00") -> PaymentCreate:
    return PaymentCreate(
        amount=amount,
        currency=Currency.RUB,
        description="Test payment",
        webhook_url="https://example.com/webhook",
    )


async def count_payments(session: AsyncSession, key: str) -> int:
    result = await session.execute(
        select(func.count()).select_from(Payment).where(Payment.idempotency_key == key)
    )
    return result.scalar_one()


async def test_create_is_atomic_payment_and_outbox(db_session: AsyncSession) -> None:
    result = await create_idempotent(db_session, make_cmd(), "key-atomic-1")
    assert result.replayed is False
    assert result.payment.status is PaymentStatus.PENDING
    row = (
        await db_session.execute(
            text(
                "SELECT event_type, aggregate_type, dispatched_at,"
                " payload->>'payment_id' AS payment_id"
                " FROM outbox WHERE aggregate_id = :aggregate_id"
            ),
            {"aggregate_id": result.payment.id},
        )
    ).one()
    assert row.event_type == "payment.created"
    assert row.aggregate_type == "payment"
    assert row.dispatched_at is None
    assert row.payment_id == str(result.payment.id)


async def test_concurrent_same_key_creates_single_row(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    cmd = make_cmd()
    key = "key-race-1"

    async def run() -> tuple[str, bool]:
        async with maker() as session:
            result = await create_idempotent(session, cmd, key)
            return str(result.payment.id), result.replayed

    outcomes = await asyncio.gather(run(), run())
    assert {replayed for _, replayed in outcomes} == {False, True}
    ids = {payment_id for payment_id, _ in outcomes}
    assert len(ids) == 1
    assert await count_payments(db_session, key) == 1


async def test_same_key_different_body_conflicts(
    db_session: AsyncSession,
) -> None:
    first = await create_idempotent(db_session, make_cmd("100.00"), "key-conflict-1")
    assert first.replayed is False
    with pytest.raises(IdempotencyKeyConflict):
        await create_idempotent(db_session, make_cmd("200.00"), "key-conflict-1")
    assert await count_payments(db_session, "key-conflict-1") == 1


async def test_failure_between_inserts_leaves_no_rows(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    async with maker() as session:

        async def failing_commit() -> None:
            raise RuntimeError("injected failure")

        monkeypatch.setattr(session, "commit", failing_commit)
        with pytest.raises(RuntimeError):
            await create_idempotent(session, make_cmd(), "key-failure-1")
        await session.rollback()
    payments_count = (
        await db_session.execute(select(func.count()).select_from(Payment))
    ).scalar_one()
    outbox_count = (
        await db_session.execute(select(func.count()).select_from(Outbox))
    ).scalar_one()
    assert payments_count == 0
    assert outbox_count == 0
