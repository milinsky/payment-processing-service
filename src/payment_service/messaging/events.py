from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid7

from pydantic import BaseModel, Field, field_serializer

from payment_service.serialization import serialize_dt_utc


class PaymentEventData(BaseModel):
    payment_id: UUID


class PaymentCreatedEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid7)
    event_type: Literal["payment.created"] = "payment.created"
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    data: PaymentEventData

    @field_serializer("occurred_at")
    def serialize_occurred_at(self, value: datetime) -> str:
        return serialize_dt_utc(value)
