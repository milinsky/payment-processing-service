import asyncio
import os
from collections.abc import AsyncGenerator
from time import monotonic
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

API_URL = os.environ.get("E2E_API_URL", "http://localhost:8000")
WEBHOOK_EVENTS_URL = os.environ.get(
    "E2E_WEBHOOK_EVENTS_URL", "http://localhost:18080/events"
)
RABBIT_MGMT_URL = os.environ.get("E2E_RABBIT_MGMT_URL", "http://localhost:15672")
RABBIT_USER = os.environ.get("RABBITMQ_DEFAULT_USER", "payments")
RABBIT_PASSWORD = os.environ.get("RABBITMQ_DEFAULT_PASS", "payments")

DB_DSN = (
    f"postgresql+asyncpg://"
    f"{os.environ.get('DB_USER', 'payments')}:"
    f"{os.environ.get('DB_PASSWORD', 'payments')}@"
    f"{os.environ.get('DB_HOST', 'localhost')}:"
    f"{os.environ.get('DB_PORT', '55432')}/"
    f"{os.environ.get('DB_NAME', 'payments')}"
)


def _api_key() -> str:
    return os.environ.get("API_KEYS", "changeme").split(",")[0]


@pytest_asyncio.fixture
async def api_client() -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=API_URL,
        headers={"X-API-Key": _api_key()},
        timeout=5.0,
    ) as client:
        yield client


@pytest.fixture
def unique_key() -> str:
    return str(uuid4())


async def wait_for_status(
    client: httpx.AsyncClient,
    payment_id: str,
    terminal: frozenset[str] = frozenset({"succeeded", "failed"}),
    deadline_s: float = 30.0,
) -> dict[str, Any]:
    deadline = monotonic() + deadline_s
    while monotonic() < deadline:
        response = await client.get(f"/api/v1/payments/{payment_id}")
        if response.status_code == 200:
            body: dict[str, Any] = response.json()
            if body["status"] in terminal:
                return body
        await asyncio.sleep(0.5)
    raise TimeoutError(f"payment {payment_id} did not reach {terminal}")


async def find_webhook_event(
    payment_id: str, deadline_s: float = 15.0
) -> dict[str, Any]:
    deadline = monotonic() + deadline_s
    async with httpx.AsyncClient(timeout=5.0) as client:
        while monotonic() < deadline:
            response = await client.get(f"{WEBHOOK_EVENTS_URL}?payment_id={payment_id}")
            if response.status_code == 200:
                items: list[dict[str, Any]] = response.json()["items"]
                for event in items:
                    if str(event.get("payment_id")) == str(payment_id):
                        return event
            await asyncio.sleep(0.5)
    raise TimeoutError(f"webhook event for {payment_id} not delivered")


async def fetch_webhook_attempts(payment_id: str) -> int:
    engine = create_async_engine(DB_DSN)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT webhook_attempts FROM payments"
                    " WHERE id = cast(:payment_id as uuid)"
                ),
                {"payment_id": str(payment_id)},
            )
            return int(result.scalar_one())
    finally:
        await engine.dispose()


async def processed_at_of(payment_id: str) -> Any:
    engine = create_async_engine(DB_DSN)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT processed_at FROM payments"
                    " WHERE id = cast(:payment_id as uuid)"
                ),
                {"payment_id": str(payment_id)},
            )
            return result.scalar_one()
    finally:
        await engine.dispose()


async def wait_for_attempts(
    payment_id: str,
    expected: int,
    deadline_s: float = 25.0,
) -> int:
    deadline = monotonic() + deadline_s
    attempts = await fetch_webhook_attempts(payment_id)
    while monotonic() < deadline and attempts < expected:
        await asyncio.sleep(0.5)
        attempts = await fetch_webhook_attempts(payment_id)
    return attempts


async def dlq_message_count() -> int:
    async with httpx.AsyncClient(
        base_url=RABBIT_MGMT_URL, auth=(RABBIT_USER, RABBIT_PASSWORD), timeout=5.0
    ) as client:
        response = await client.get("/api/queues/%2F/payments.dlq")
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        return int(response.json().get("messages", 0))


async def wait_for_dlq_delta(
    baseline: int,
    deadline_s: float = 20.0,
) -> int:
    deadline = monotonic() + deadline_s
    count = await dlq_message_count()
    while monotonic() < deadline and count <= baseline:
        await asyncio.sleep(0.5)
        count = await dlq_message_count()
    return count
