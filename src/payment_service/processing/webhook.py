import hashlib
import hmac
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

import httpx
from pydantic import field_serializer

from payment_service.config import WebhookSettings
from payment_service.payments.models import Currency
from payment_service.payments.schemas import PaymentBaseModel
from payment_service.payments.service import canonical_json
from payment_service.serialization import serialize_dt_utc


class WebhookPayload(PaymentBaseModel):
    type: Literal["payment.succeeded", "payment.failed"]
    event_id: UUID
    payment_id: UUID
    status: Literal["succeeded", "failed"]
    amount: Decimal
    currency: Currency
    occurred_at: datetime

    @field_serializer("occurred_at")
    def serialize_occurred_at(self, value: datetime) -> str:
        return serialize_dt_utc(value)


class WebhookDeliveryError(Exception):
    def __init__(self, url: str, status_code: int | None) -> None:
        super().__init__(
            f"webhook delivery failed: url={url} status_code={status_code}"
        )
        self.url = url
        self.status_code = status_code


class WebhookClient:
    def __init__(
        self,
        settings: WebhookSettings,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            timeout=settings.timeout_s, transport=transport
        )

    async def deliver(self, url: str, payload: WebhookPayload) -> None:
        body = canonical_json(payload)
        headers = {"Content-Type": "application/json"}
        if self._settings.secret is not None:
            headers["X-Signature"] = hmac.new(
                self._settings.secret.encode(), body.encode(), hashlib.sha256
            ).hexdigest()
        try:
            response = await self._client.post(
                url, content=body.encode(), headers=headers
            )
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            raise WebhookDeliveryError(url, status_code=None) from exc
        if not 200 <= response.status_code < 300:
            raise WebhookDeliveryError(url, status_code=response.status_code)

    async def aclose(self) -> None:
        await self._client.aclose()
