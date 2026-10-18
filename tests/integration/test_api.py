from collections.abc import AsyncGenerator
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import get_settings
from payment_service.main import create_app
from payment_service.payments.dependencies import get_session
from payment_service.payments.models import Currency, Payment
from payment_service.payments.router import IDEMPOTENCY_KEY_MAX_LENGTH
from payment_service.payments.schemas import WEBHOOK_URL_MAX_LENGTH

pytestmark = pytest.mark.integration

API_KEY = "test-api-key"
CREATE_BODY = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Test payment",
    "webhook_url": "https://example.com/webhook",
}


@pytest.fixture
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


async def test_create_payment_returns_202(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json={**CREATE_BODY, "metadata": {"order": 42}},
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "api-k-1"},
    )
    assert response.status_code == 202
    body = response.json()
    assert UUID(body["payment_id"])
    assert body["status"] == "pending"
    assert body["amount"] == "100.00"
    assert body["currency"] == "RUB"
    assert body["description"] == "Test payment"
    assert body["metadata"] == {"order": 42}
    assert body["processed_at"] is None
    assert body["created_at"].endswith("Z")


async def test_replay_returns_200_with_header(client: AsyncClient) -> None:
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": "api-k-2"}
    first = await client.post("/api/v1/payments", json=CREATE_BODY, headers=headers)
    second = await client.post("/api/v1/payments", json=CREATE_BODY, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 200
    assert second.headers["Idempotency-Replayed"] == "true"
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert "Idempotency-Replayed" not in first.headers


async def test_same_key_different_body_returns_409(client: AsyncClient) -> None:
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": "api-k-3"}
    first = await client.post("/api/v1/payments", json=CREATE_BODY, headers=headers)
    conflicting = await client.post(
        "/api/v1/payments",
        json={**CREATE_BODY, "amount": "200.00"},
        headers=headers,
    )
    assert first.status_code == 202
    assert conflicting.status_code == 409
    assert conflicting.json()["detail"]


async def test_missing_idempotency_key_returns_422(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments", json=CREATE_BODY, headers={"X-API-Key": API_KEY}
    )
    assert response.status_code == 422


async def test_empty_idempotency_key_returns_422(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"X-API-Key": API_KEY, "Idempotency-Key": ""},
    )
    assert response.status_code == 422


async def test_oversized_idempotency_key_returns_422(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={
            "X-API-Key": API_KEY,
            "Idempotency-Key": "k" * (IDEMPOTENCY_KEY_MAX_LENGTH + 1),
        },
    )
    assert response.status_code == 422


async def test_boundary_idempotency_key_is_accepted(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={
            "X-API-Key": API_KEY,
            "Idempotency-Key": "k" * IDEMPOTENCY_KEY_MAX_LENGTH,
        },
    )
    assert response.status_code == 202


async def test_oversized_webhook_url_returns_422(client: AsyncClient) -> None:
    padded_url = "https://example.com/" + "a" * WEBHOOK_URL_MAX_LENGTH
    response = await client.post(
        "/api/v1/payments",
        json={**CREATE_BODY, "webhook_url": padded_url},
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "api-k-url"},
    )
    assert response.status_code == 422


async def test_missing_api_key_returns_401(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"Idempotency-Key": "api-k-4"},
    )
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "ApiKey"
    payment_id = uuid4()
    get_response = await client.get(f"/api/v1/payments/{payment_id}")
    assert get_response.status_code == 401
    assert get_response.headers["WWW-Authenticate"] == "ApiKey"


async def test_invalid_api_key_returns_401(client: AsyncClient) -> None:
    headers = {"X-API-Key": "wrong", "Idempotency-Key": "api-k-5"}
    response = await client.post("/api/v1/payments", json=CREATE_BODY, headers=headers)
    assert response.status_code == 401
    payment_id = uuid4()
    get_response = await client.get(
        f"/api/v1/payments/{payment_id}", headers={"X-API-Key": "wrong"}
    )
    assert get_response.status_code == 401


async def test_get_existing_payment_returns_200(client: AsyncClient) -> None:
    created = await client.post(
        "/api/v1/payments",
        json={**CREATE_BODY, "metadata": {"order": 42}},
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "api-k-6"},
    )
    payment_id = created.json()["payment_id"]
    response = await client.get(
        f"/api/v1/payments/{payment_id}", headers={"X-API-Key": API_KEY}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["status"] == "pending"
    assert body["amount"] == "100.00"
    assert body["currency"] == "RUB"
    assert body["description"] == "Test payment"
    assert body["metadata"] == {"order": 42}
    assert body["processed_at"] is None
    assert body["created_at"].endswith("Z")


async def test_create_payment_generates_uuidv7(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=CREATE_BODY,
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "api-k-v7"},
    )
    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])
    assert payment_id.version == 7
    fetched = await client.get(
        f"/api/v1/payments/{payment_id}", headers={"X-API-Key": API_KEY}
    )
    assert fetched.status_code == 200
    assert fetched.json()["payment_id"] == str(payment_id)


async def test_get_legacy_uuidv4_payment_returns_200(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    legacy_id = uuid4()
    db_session.add(
        Payment(
            id=legacy_id,
            amount=Decimal("100.00"),
            currency=Currency.RUB,
            description="legacy payment",
            idempotency_key=f"legacy-{legacy_id}",
            request_fingerprint="0" * 64,
            webhook_url="https://example.com/hook",
        )
    )
    await db_session.commit()
    response = await client.get(
        f"/api/v1/payments/{legacy_id}", headers={"X-API-Key": API_KEY}
    )
    assert response.status_code == 200
    assert response.json()["payment_id"] == str(legacy_id)


async def test_get_unknown_payment_returns_404(client: AsyncClient) -> None:
    response = await client.get(
        f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": API_KEY}
    )
    assert response.status_code == 404


async def test_get_invalid_uuid_returns_422(client: AsyncClient) -> None:
    response = await client.get(
        "/api/v1/payments/not-a-uuid", headers={"X-API-Key": API_KEY}
    )
    assert response.status_code == 422


async def test_responses_carry_request_id(client: AsyncClient) -> None:
    response = await client.get(
        f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": API_KEY}
    )
    assert response.headers["x-request-id"]


async def test_openapi_schema_exposes_both_paths(client: AsyncClient) -> None:
    response = await client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/api/v1/payments" in paths
    assert "/api/v1/payments/{payment_id}" in paths


async def test_openapi_hidden_when_docs_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_KEYS", API_KEY)
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_PASSWORD", "payments")
    monkeypatch.setenv("DB_NAME", "payments")
    monkeypatch.setenv("DOCS_ENABLED", "false")
    get_settings.cache_clear()
    app = create_app(get_settings())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        openapi = await http.get("/openapi.json")
        docs = await http.get("/docs")
        health = await http.get("/health")
    assert openapi.status_code == 404
    assert docs.status_code == 404
    assert health.status_code == 200
