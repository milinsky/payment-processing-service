from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from payment_service.outbox.models import Outbox
from payment_service.outbox.repository import OutboxRepository
from payment_service.payments.models import Currency, Payment
from payment_service.payments.repository import PaymentRepository

pytestmark = pytest.mark.unit


def make_payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Test payment",
        idempotency_key=f"key-{uuid4()}",
        request_fingerprint="0" * 64,
        webhook_url="https://example.com/webhook",
    )


def make_session_mock() -> Any:
    session = MagicMock()
    session.add = MagicMock()
    return session


def test_payment_add_delegates_to_session() -> None:
    session = make_session_mock()
    repo = PaymentRepository(session)
    payment = make_payment()
    repo.add(payment)
    session.add.assert_called_once_with(payment)


def test_add_created_event_builds_payment_created_outbox() -> None:
    session = make_session_mock()
    repo = PaymentRepository(session)
    payment = make_payment()
    repo.add_created_event(payment)
    session.add.assert_called_once()
    event = session.add.call_args.args[0]
    assert isinstance(event, Outbox)
    assert event.aggregate_type == "payment"
    assert event.aggregate_id == payment.id
    assert event.event_type == "payment.created"
    assert event.payload == {"payment_id": str(payment.id)}


def test_outbox_add_delegates_to_session() -> None:
    session = make_session_mock()
    repo = OutboxRepository(session)
    event = Outbox(
        event_id=uuid4(),
        aggregate_type="payment",
        aggregate_id=uuid4(),
        event_type="payment.created",
        payload={"payment_id": "x"},
    )
    repo.add(event)
    session.add.assert_called_once_with(event)


async def test_fetch_pending_batch_uses_locked_ordered_select() -> None:
    session = make_session_mock()
    repo = OutboxRepository(session)
    row = Outbox(
        event_id=uuid4(),
        aggregate_type="payment",
        aggregate_id=uuid4(),
        event_type="payment.created",
        payload={"payment_id": "x"},
    )
    result = MagicMock()
    result.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=result)
    rows = await repo.fetch_pending_batch(5)
    assert rows == [row]
    statement = session.execute.call_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))
    assert "FOR UPDATE" in compiled
    assert "ORDER BY outbox.id" in compiled
    assert "LIMIT 5" in compiled
    for_update = statement._for_update_arg
    assert for_update is not None
    assert for_update.skip_locked is True
