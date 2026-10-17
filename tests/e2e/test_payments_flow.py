from typing import Any

import httpx
import pytest

from e2e.conftest import (
    dlq_message_count,
    find_webhook_event,
    processed_at_of,
    wait_for_attempts,
    wait_for_dlq_delta,
    wait_for_status,
)

pytestmark = pytest.mark.e2e

CREATE_BODY = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Test payment",
    "webhook_url": "http://webhook-mock:8080/webhook",
}


async def create_payment(
    client: httpx.AsyncClient,
    key: str,
    webhook_url: str = "http://webhook-mock:8080/webhook",
) -> dict[str, Any]:
    response = await client.post(
        "/api/v1/payments",
        json={**CREATE_BODY, "webhook_url": webhook_url},
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 202
    body: dict[str, Any] = response.json()
    assert body["status"] == "pending"
    assert body["amount"] == "100.00"
    return body


async def test_payment_succeeded_flow(
    api_client: httpx.AsyncClient,
    unique_key: str,
) -> None:
    created = await create_payment(api_client, unique_key)
    payment_id = created["payment_id"]
    assert created["description"] == "Test payment"
    assert created["processed_at"] is None
    final = await wait_for_status(api_client, payment_id)
    assert final["status"] == "succeeded"
    assert final["description"] == "Test payment"
    assert final["metadata"] is None
    assert final["processed_at"] is not None
    event = await find_webhook_event(payment_id)
    assert event["type"] == "payment.succeeded"
    assert event["status"] == "succeeded"
    assert event["amount"] == "100.00"
    assert str(event["payment_id"]) == str(payment_id)


async def test_idempotent_replay(
    api_client: httpx.AsyncClient,
    unique_key: str,
) -> None:
    first = await create_payment(api_client, unique_key)
    replay = await api_client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"Idempotency-Key": unique_key},
    )
    assert replay.status_code == 200
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["payment_id"] == first["payment_id"]


async def test_missing_api_key_is_unauthorized(
    api_client: httpx.AsyncClient,
    unique_key: str,
) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"Idempotency-Key": unique_key, "X-API-Key": ""},
    )
    assert response.status_code == 401


async def test_dead_webhook_reaches_dlq_flow(
    api_client: httpx.AsyncClient,
    unique_key: str,
) -> None:
    baseline = await dlq_message_count()
    created = await create_payment(
        api_client, unique_key, webhook_url="http://127.0.0.1:9/x"
    )
    payment_id = created["payment_id"]
    final = await wait_for_status(api_client, payment_id)
    assert final["status"] == "succeeded"
    attempts = await wait_for_attempts(payment_id, expected=4)
    assert attempts == 4
    processed_at = await processed_at_of(payment_id)
    assert processed_at is not None
    dlq_count = await wait_for_dlq_delta(baseline)
    assert dlq_count > baseline
