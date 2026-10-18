import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest

from payment_service.payments.models import Currency
from payment_service.processing.gateway import (
    GatewayStatus,
    RandomGateway,
)

pytestmark = pytest.mark.unit

AMOUNT = Decimal("10.00")


async def process_many(gateway: RandomGateway, count: int) -> list[GatewayStatus]:
    return [
        (await gateway.process(uuid4(), AMOUNT, Currency.RUB)).status
        for _ in range(count)
    ]


def test_success_rate_one_always_succeeds() -> None:
    gateway = RandomGateway(1.0, 0.0, 0.0, seed=1)
    statuses = asyncio.run(process_many(gateway, 20))
    assert statuses == [GatewayStatus.SUCCEEDED] * 20


def test_success_rate_zero_always_fails() -> None:
    gateway = RandomGateway(0.0, 0.0, 0.0, seed=1)
    statuses = asyncio.run(process_many(gateway, 20))
    assert statuses == [GatewayStatus.FAILED] * 20


def test_same_seed_reproduces_outcome_sequence() -> None:
    first = asyncio.run(process_many(RandomGateway(0.9, 0.0, 0.0, seed=42), 50))
    second = asyncio.run(process_many(RandomGateway(0.9, 0.0, 0.0, seed=42), 50))
    assert first == second
    assert len(first) == 50


def test_seeded_success_share_within_bounds() -> None:
    statuses = asyncio.run(process_many(RandomGateway(0.9, 0.0, 0.0, seed=42), 1000))
    share = statuses.count(GatewayStatus.SUCCEEDED) / len(statuses)
    assert 0.8 <= share <= 1.0


async def test_delay_bounds_are_respected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[float] = []

    async def fake_sleep(delay: float) -> None:
        captured.append(delay)

    monkeypatch.setattr("payment_service.processing.gateway.asyncio.sleep", fake_sleep)
    gateway = RandomGateway(1.0, 2.0, 5.0, seed=7)
    statuses = await process_many(gateway, 100)
    assert statuses == [GatewayStatus.SUCCEEDED] * 100
    assert len(captured) == 100
    assert all(2.0 <= delay <= 5.0 for delay in captured)
    assert min(captured) >= 2.0
    assert max(captured) <= 5.0


def test_invalid_success_rate_raises() -> None:
    with pytest.raises(ValueError):
        RandomGateway(1.5, 2.0, 5.0)


def test_invalid_delay_bounds_raise() -> None:
    with pytest.raises(ValueError):
        RandomGateway(0.9, 5.0, 2.0)


async def test_process_does_not_block_event_loop() -> None:
    gateway = RandomGateway(1.0, 0.2, 0.2, seed=5)
    loop = asyncio.get_running_loop()
    start = loop.time()
    ping_end: float | None = None

    async def ping() -> None:
        nonlocal ping_end
        await asyncio.sleep(0.05)
        ping_end = loop.time()

    await asyncio.gather(
        gateway.process(uuid4(), AMOUNT, Currency.USD),
        ping(),
    )
    assert ping_end is not None
    assert ping_end - start < 0.15
