import httpx
import pytest

from e2e.conftest import (
    find_webhook_event,
    wait_for_status,
)
from e2e.test_payments_flow import CREATE_BODY

pytestmark = pytest.mark.e2e


async def test_payment_failed_flow(
    api_client: httpx.AsyncClient,
    unique_key: str,
) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"Idempotency-Key": unique_key},
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]
    final = await wait_for_status(api_client, payment_id)
    assert final["status"] == "failed"
    event = await find_webhook_event(payment_id)
    assert event["type"] == "payment.failed"
    assert event["status"] == "failed"
