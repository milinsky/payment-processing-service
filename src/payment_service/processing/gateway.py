import asyncio
import enum
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from payment_service.payments.models import Currency


class GatewayStatus(enum.StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class GatewayResult:
    status: GatewayStatus


class PaymentGateway(Protocol):
    async def process(
        self, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult: ...


class RandomGateway:
    def __init__(
        self,
        success_rate: float,
        delay_min_s: float,
        delay_max_s: float,
        seed: int | None = None,
    ) -> None:
        if not 0 <= success_rate <= 1:
            raise ValueError("success_rate out of range")
        if not 0 <= delay_min_s <= delay_max_s:
            raise ValueError("invalid delay bounds")
        self._success_rate = success_rate
        self._delay_min_s = delay_min_s
        self._delay_max_s = delay_max_s
        self._rng = random.Random(seed)

    async def process(
        self,
        payment_id: UUID,
        amount: Decimal,
        currency: Currency,
    ) -> GatewayResult:
        delay = self._rng.uniform(self._delay_min_s, self._delay_max_s)
        await asyncio.sleep(delay)
        succeeded = self._rng.random() < self._success_rate
        status = GatewayStatus.SUCCEEDED if succeeded else GatewayStatus.FAILED
        return GatewayResult(status=status)
