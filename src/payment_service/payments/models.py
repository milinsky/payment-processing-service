import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CHAR, CheckConstraint, DateTime, Numeric, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as PGEnum

from payment_service.db import Base


class PaymentStatus(enum.StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Currency(enum.StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


def _enum_values(cls: type[enum.Enum]) -> list[str]:
    return [member.value for member in cls]


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)

    id: Mapped[uuid.UUID] = mapped_column(
        server_default=text("uuidv7()"), primary_key=True
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    currency: Mapped[Currency] = mapped_column(
        PGEnum(Currency, name="currency_code", values_callable=_enum_values)
    )
    description: Mapped[str] = mapped_column(String(1000))
    meta: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    status: Mapped[PaymentStatus] = mapped_column(
        PGEnum(PaymentStatus, name="payment_status", values_callable=_enum_values),
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_fingerprint: Mapped[str] = mapped_column(CHAR(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    webhook_attempts: Mapped[int] = mapped_column(server_default=text("0"))
