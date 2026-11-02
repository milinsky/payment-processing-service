import asyncio
from collections.abc import AsyncGenerator
from decimal import Decimal
from time import monotonic
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.config import ProcessingSettings, get_settings
from payment_service.main import create_app
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.payments.dependencies import get_session
from payment_service.payments.models import Currency
from payment_service.processing.gateway import GatewayResult, GatewayStatus
from payment_service.processing.service import PaymentProcessor, ProcessingOutcome
from payment_service.processing.webhook import WebhookPayload

pytestmark = pytest.mark.integration

API_KEY = "test-api-key"
CREATE_BODY = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "qa api contract",
    "webhook_url": "https://example.com/webhook",
}
GATEWAY_DELAY_S = 0.6


class SlowGateway:
    def __init__(self, delay_s: float) -> None:
        self.calls: list[tuple[UUID, Decimal, Currency]] = []
        self.entered = asyncio.Event()
        self._delay_s = delay_s

    async def process(
        self, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult:
        self.calls.append((payment_id, amount, currency))
        self.entered.set()
        await asyncio.sleep(self._delay_s)
        return GatewayResult(status=GatewayStatus.SUCCEEDED)


class RecordingWebhook:
    def __init__(self) -> None:
        self.calls: list[WebhookPayload] = []

    async def deliver(self, url: str, payload: WebhookPayload) -> None:
        self.calls.append(payload)


@pytest_asyncio.fixture
async def client(
    monkeypatch: pytest.MonkeyPatch,
    db_session: AsyncSession,
) -> AsyncGenerator[AsyncClient]:
    monkeypatch.setenv("API_KEYS", API_KEY)
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_PASSWORD", "payments")
    monkeypatch.setenv("DB_NAME", "payments")
    get_settings.cache_clear()
    app = create_app(get_settings())

    async def override_get_session() -> AsyncGenerator[AsyncSession]:
        yield db_session

    app.dependency_overrides[get_session] = override_get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


async def test_get_status_returns_pending_while_gateway_in_flight(
    client: AsyncClient,
    db_engine: AsyncEngine,
) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"X-API-Key": API_KEY, "Idempotency-Key": f"qa-pending-{uuid4()}"},
    )
    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])

    gateway = SlowGateway(GATEWAY_DELAY_S)
    webhook = RecordingWebhook()
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    settings = ProcessingSettings(timeout_s=5.0, lease_ttl_s=25.0)
    gateway_any: Any = gateway
    webhook_any: Any = webhook
    processor = PaymentProcessor(maker, gateway_any, webhook_any, settings)
    handle_task = asyncio.create_task(
        processor.handle(
            PaymentCreatedEvent(
                event_id=uuid4(),
                data=PaymentEventData(payment_id=payment_id),
            )
        )
    )
    await gateway.entered.wait()

    during = await client.get(
        f"/api/v1/payments/{payment_id}", headers={"X-API-Key": API_KEY}
    )
    assert during.status_code == 200
    assert during.json()["status"] == "pending"
    assert during.json()["processed_at"] is None

    outcome = await handle_task
    assert outcome is ProcessingOutcome.ACKED_BUSINESS
    assert len(gateway.calls) == 1

    deadline = monotonic() + 5.0
    after_status = ""
    while monotonic() < deadline:
        after = await client.get(
            f"/api/v1/payments/{payment_id}", headers={"X-API-Key": API_KEY}
        )
        assert after.status_code == 200
        after_status = after.json()["status"]
        if after_status == "succeeded":
            break
        await asyncio.sleep(0.1)
    assert after_status == "succeeded"
    assert len(webhook.calls) == 1
