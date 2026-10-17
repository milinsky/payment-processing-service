from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_serializer

from payment_service.payments.models import Currency, PaymentStatus
from payment_service.serialization import serialize_dt_utc

WEBHOOK_URL_MAX_LENGTH = 2048


class PaymentBaseModel(BaseModel):
    @field_serializer("amount", check_fields=False)
    def serialize_amount(self, value: Decimal) -> str:
        return f"{value.quantize(Decimal('0.01'))}"

    @field_serializer("created_at", check_fields=False)
    def serialize_created_at(self, value: datetime) -> str:
        return serialize_dt_utc(value)

    @field_serializer("processed_at", check_fields=False)
    def serialize_processed_at(self, value: datetime | None) -> str | None:
        return None if value is None else serialize_dt_utc(value)


class PaymentCreate(PaymentBaseModel):
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] | None = None
    webhook_url: Annotated[AnyHttpUrl, Field(max_length=WEBHOOK_URL_MAX_LENGTH)]


class PaymentRead(PaymentBaseModel):
    model_config = ConfigDict(populate_by_name=True)

    payment_id: UUID = Field(alias="id", serialization_alias="payment_id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] | None = None
    processed_at: datetime | None = None
    created_at: datetime
