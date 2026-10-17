import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from payment_service.payments.models import Currency, PaymentStatus
from payment_service.payments.schemas import (
    WEBHOOK_URL_MAX_LENGTH,
    PaymentCreate,
    PaymentRead,
)

pytestmark = pytest.mark.unit

VALID_PAYLOAD = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Test payment",
    "webhook_url": "https://example.com/webhook",
}


def make_read(**overrides: object) -> PaymentRead:
    fields: dict[str, object] = {
        "payment_id": uuid4(),
        "status": PaymentStatus.PENDING,
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Test payment",
        "processed_at": None,
        "created_at": datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC),
    }
    fields.update(overrides)
    return PaymentRead(**fields)


def test_valid_payload_is_accepted() -> None:
    payload = PaymentCreate(**VALID_PAYLOAD)
    assert payload.amount == Decimal("100.00")
    assert payload.currency is Currency.RUB
    assert payload.description == "Test payment"


def test_amount_with_three_decimal_places_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "amount": "0.001"})


def test_amount_zero_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "amount": "0"})


def test_amount_with_thirteen_digits_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "amount": "1000000000000.00"})


def test_unknown_currency_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "currency": "GBP"})


def test_invalid_webhook_url_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "webhook_url": "not-a-url"})


def test_oversized_webhook_url_is_rejected() -> None:
    padded = "https://example.com/" + "a" * WEBHOOK_URL_MAX_LENGTH
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "webhook_url": padded})


def test_boundary_webhook_url_is_accepted() -> None:
    base = "https://example.com/"
    url = base + "a" * (WEBHOOK_URL_MAX_LENGTH - len(base))
    payload = PaymentCreate(**{**VALID_PAYLOAD, "webhook_url": url})
    assert str(payload.webhook_url) == url


def test_empty_description_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID_PAYLOAD, "description": ""})


def test_amount_serializes_as_string_with_two_decimals() -> None:
    read = make_read(amount=Decimal("100"))
    parsed = json.loads(read.model_dump_json())
    assert isinstance(parsed["amount"], str)
    assert parsed["amount"] == "100.00"
    assert parsed["status"] == "pending"
    assert parsed["currency"] == "RUB"


def test_created_at_serializes_as_iso_utc() -> None:
    read = make_read(
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("1.5"),
        currency=Currency.EUR,
    )
    parsed = json.loads(read.model_dump_json())
    assert parsed["created_at"] == "2026-10-15T12:00:00Z"
    assert parsed["amount"] == "1.50"


def test_processed_at_serializes_as_iso_utc_or_none() -> None:
    pending = make_read()
    processed = make_read(processed_at=datetime(2026, 10, 15, 12, 30, 0, tzinfo=UTC))
    assert json.loads(pending.model_dump_json())["processed_at"] is None
    parsed = json.loads(processed.model_dump_json())
    assert parsed["processed_at"] == "2026-10-15T12:30:00Z"


def test_read_exposes_description_and_metadata() -> None:
    read = make_read(
        description="order 42",
        metadata={"order": 42},
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime(2026, 10, 15, 12, 30, 0, tzinfo=UTC),
    )
    parsed = json.loads(read.model_dump_json())
    assert parsed["description"] == "order 42"
    assert parsed["metadata"] == {"order": 42}


def test_metadata_round_trip() -> None:
    metadata = {"key": [1, 2, {"nested": True}]}
    payload = PaymentCreate(**{**VALID_PAYLOAD, "metadata": metadata})
    assert payload.metadata == metadata
    assert payload.model_dump()["metadata"] == metadata


def test_metadata_defaults_to_none() -> None:
    payload = PaymentCreate(**VALID_PAYLOAD)
    assert payload.metadata is None


def test_payment_read_accepts_id_alias_and_field_name() -> None:
    payment_id = uuid4()
    by_alias = PaymentRead.model_validate(
        {
            "id": payment_id,
            "status": "failed",
            "amount": "2",
            "currency": "USD",
            "description": "alias",
            "created_at": "2026-10-15T12:00:00Z",
        }
    )
    by_name = PaymentRead(
        payment_id=payment_id,
        status=PaymentStatus.FAILED,
        amount=Decimal("2"),
        currency=Currency.USD,
        description="name",
        created_at=datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC),
    )
    assert by_alias.payment_id == payment_id
    assert by_name.payment_id == payment_id
    assert by_alias.status is PaymentStatus.FAILED
