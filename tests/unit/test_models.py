from typing import Any

import pytest
from sqlalchemy import CheckConstraint, Numeric

from payment_service.outbox.models import Outbox
from payment_service.payments.models import Currency, Payment, PaymentStatus

pytestmark = pytest.mark.unit


def test_amount_positive_check_constraint_matches_migration() -> None:
    table: Any = Payment.__table__
    checks = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    ]
    assert len(checks) == 1
    assert checks[0].name == "payments_amount_positive_check"
    assert str(checks[0].sqltext) == "amount > 0"


def test_python_attribute_is_meta_and_column_is_metadata() -> None:
    assert hasattr(Payment, "meta")
    assert Payment.__table__.c.metadata.name == "metadata"


def test_request_fingerprint_column_is_char_64() -> None:
    fingerprint_type: Any = Payment.__table__.c.request_fingerprint.type
    assert fingerprint_type.__class__.__name__ == "CHAR"
    assert fingerprint_type.length == 64


def test_amount_column_is_numeric_12_2() -> None:
    amount_type = Payment.__table__.c.amount.type
    assert isinstance(amount_type, Numeric)
    assert amount_type.precision == 12
    assert amount_type.scale == 2


def test_enum_column_types_match_migration_names() -> None:
    currency_type: Any = Payment.__table__.c.currency.type
    status_type: Any = Payment.__table__.c.status.type
    assert currency_type.name == "currency_code"
    assert status_type.name == "payment_status"


def test_enum_values_match_migration_values() -> None:
    currency_type: Any = Payment.__table__.c.currency.type
    status_type: Any = Payment.__table__.c.status.type
    assert list(currency_type.enums) == ["RUB", "USD", "EUR"]
    assert list(status_type.enums) == ["pending", "succeeded", "failed"]
    assert "processing" not in list(status_type.enums)


def test_status_enum_membership() -> None:
    assert {member.value for member in PaymentStatus} == {
        "pending",
        "succeeded",
        "failed",
    }
    assert {member.value for member in Currency} == {"RUB", "USD", "EUR"}


def test_outbox_id_has_bigserial_semantics() -> None:
    id_column = Outbox.__table__.c.id
    assert id_column.autoincrement is True
    assert id_column.primary_key
    assert id_column.type.python_type is int


def test_payment_mapper_has_no_relationships() -> None:
    assert len(Payment.__mapper__.relationships) == 0
