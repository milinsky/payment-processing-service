from datetime import UTC, datetime, timedelta

import pytest

from payment_service.serialization import serialize_dt_utc

pytestmark = pytest.mark.unit


def test_naive_datetime_is_treated_as_utc() -> None:
    assert serialize_dt_utc(datetime(2026, 10, 15, 12, 0, 0)) == (
        "2026-10-15T12:00:00Z"
    )


def test_non_utc_timezone_is_converted() -> None:
    value = datetime(2026, 10, 15, 15, 0, 0, tzinfo=UTC) - timedelta(hours=3)
    assert serialize_dt_utc(value) == "2026-10-15T12:00:00Z"


def test_utc_datetime_keeps_z_suffix() -> None:
    assert (
        serialize_dt_utc(datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC))
        == "2026-10-15T12:00:00Z"
    )
