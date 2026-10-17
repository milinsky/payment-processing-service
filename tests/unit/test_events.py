from datetime import UTC, datetime
from uuid import uuid4

import pytest

from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData

pytestmark = pytest.mark.unit


def test_round_trip_preserves_all_fields() -> None:
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        occurred_at=datetime(2026, 10, 15, 12, 30, 0, tzinfo=UTC),
        data=PaymentEventData(payment_id=uuid4()),
    )
    parsed = PaymentCreatedEvent.model_validate_json(event.model_dump_json())
    assert parsed == event
    assert parsed.event_id == event.event_id
    assert parsed.occurred_at == event.occurred_at
    assert parsed.data.payment_id == event.data.payment_id


def test_occurred_at_serializes_as_iso_utc() -> None:
    event = PaymentCreatedEvent(
        occurred_at=datetime(2026, 10, 15, 12, 30, 0, tzinfo=UTC),
        data=PaymentEventData(payment_id=uuid4()),
    )
    dumped = event.model_dump_json()
    assert '"occurred_at":"2026-10-15T12:30:00Z"' in dumped


def test_event_type_is_fixed_literal() -> None:
    event = PaymentCreatedEvent(data=PaymentEventData(payment_id=uuid4()))
    assert event.event_type == "payment.created"


def test_defaults_are_generated() -> None:
    event = PaymentCreatedEvent(data=PaymentEventData(payment_id=uuid4()))
    assert event.event_id is not None
    assert event.occurred_at.tzinfo is not None


def test_default_event_id_is_uuidv7() -> None:
    event = PaymentCreatedEvent(data=PaymentEventData(payment_id=uuid4()))
    assert event.event_id.version == 7
