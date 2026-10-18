import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import UUID as PGUUID

from payment_service.db import Base


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "idx_outbox_pending",
            "id",
            postgresql_where=text("dispatched_at IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(), server_default=text("uuidv7()"), unique=True
    )
    aggregate_type: Mapped[str] = mapped_column(String(63))
    aggregate_id: Mapped[uuid.UUID] = mapped_column(PGUUID())
    event_type: Mapped[str] = mapped_column(String(63))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
