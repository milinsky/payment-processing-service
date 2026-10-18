from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.payments.models import Currency, Payment

pytestmark = pytest.mark.integration


async def test_first_test_inserts_payment_row(db_session: AsyncSession) -> None:
    payment = Payment(
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="isolation probe",
        idempotency_key="isolation-probe-1",
        request_fingerprint="0" * 64,
        webhook_url="https://example.com/hook",
    )
    db_session.add(payment)
    await db_session.commit()
    count = (
        await db_session.execute(select(func.count()).select_from(Payment))
    ).scalar()
    assert count == 1


async def test_second_test_sees_empty_payments(db_session: AsyncSession) -> None:
    count = (
        await db_session.execute(select(func.count()).select_from(Payment))
    ).scalar()
    assert count == 0
