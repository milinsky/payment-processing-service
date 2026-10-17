from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from payment_service.config import WebhookSettings
from payment_service.processing.webhook import (
    WebhookClient,
    WebhookDeliveryError,
    WebhookPayload,
)

pytestmark = pytest.mark.unit

WEBHOOK_URL = "https://example.com/webhook"
EXPECTED_SIGNATURE = "becfe973e32df8d91815314e89b4c0eff9c34426dad15fc80c0ce3bc1f1f8808"

Handler = Callable[[httpx.Request], httpx.Response]


def sample_payload() -> WebhookPayload:
    return WebhookPayload(
        type="payment.succeeded",
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        payment_id=UUID("22222222-2222-2222-2222-222222222222"),
        status="succeeded",
        amount=Decimal("100.00"),
        currency="RUB",
        occurred_at=datetime(2026, 10, 15, 0, 0, 0, tzinfo=UTC),
    )


def make_client(
    handler: Handler,
    secret: str | None = None,
) -> WebhookClient:
    settings = WebhookSettings(timeout_s=5.0, secret=secret)
    return WebhookClient(settings, transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("status_code", [200, 204])
async def test_success_statuses_do_not_raise(status_code: int) -> None:
    client = make_client(lambda request: httpx.Response(status_code, request=request))
    await client.deliver(WEBHOOK_URL, sample_payload())
    await client.aclose()


@pytest.mark.parametrize("status_code", [500, 404])
async def test_error_statuses_raise_with_status_code(status_code: int) -> None:
    client = make_client(lambda request: httpx.Response(status_code, request=request))
    with pytest.raises(WebhookDeliveryError) as excinfo:
        await client.deliver(WEBHOOK_URL, sample_payload())
    assert excinfo.value.status_code == status_code
    assert excinfo.value.url == WEBHOOK_URL
    await client.aclose()


async def test_timeout_raises_without_status_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("connect timeout", request=request)

    client = make_client(handler)
    with pytest.raises(WebhookDeliveryError) as excinfo:
        await client.deliver(WEBHOOK_URL, sample_payload())
    assert excinfo.value.status_code is None
    assert isinstance(excinfo.value.__cause__, httpx.TimeoutException)
    await client.aclose()


async def test_network_error_raises_without_status_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = make_client(handler)
    with pytest.raises(WebhookDeliveryError) as excinfo:
        await client.deliver(WEBHOOK_URL, sample_payload())
    assert excinfo.value.status_code is None
    assert isinstance(excinfo.value.__cause__, httpx.RequestError)
    await client.aclose()


async def test_exactly_one_attempt_on_permanent_failure() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500, request=request)

    client = make_client(handler)
    with pytest.raises(WebhookDeliveryError):
        await client.deliver(WEBHOOK_URL, sample_payload())
    assert len(requests) == 1
    await client.aclose()


async def test_signature_matches_reference_vector() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.headers)
        return httpx.Response(200, request=request)

    client = make_client(handler, secret="test-secret")
    await client.deliver(WEBHOOK_URL, sample_payload())
    assert captured["x-signature"] == EXPECTED_SIGNATURE
    await client.aclose()


async def test_body_is_canonical_json_with_string_amount() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content.decode()
        captured["content_type"] = request.headers["content-type"]
        return httpx.Response(200, request=request)

    client = make_client(handler, secret="test-secret")
    await client.deliver(WEBHOOK_URL, sample_payload())
    assert captured["content_type"] == "application/json"
    assert '"amount":"100.00"' in captured["content"]
    import json

    parsed = json.loads(captured["content"])
    assert isinstance(parsed["amount"], str)
    assert parsed["amount"] == "100.00"
    assert parsed["occurred_at"] == "2026-10-15T00:00:00Z"
    await client.aclose()


async def test_no_signature_header_without_secret() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.headers)
        return httpx.Response(200, request=request)

    client = make_client(handler, secret=None)
    await client.deliver(WEBHOOK_URL, sample_payload())
    assert "x-signature" not in captured
    await client.aclose()


async def test_error_message_contains_no_secret() -> None:
    client = make_client(
        lambda request: httpx.Response(500, request=request),
        secret="top-secret-value",
    )
    with pytest.raises(WebhookDeliveryError) as excinfo:
        await client.deliver(WEBHOOK_URL, sample_payload())
    assert "top-secret-value" not in str(excinfo.value)
    await client.aclose()
