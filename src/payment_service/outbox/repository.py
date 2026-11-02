from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.outbox.models import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: Outbox) -> None:
        self._session.add(event)

    async def fetch_pending_batch(self, batch_size: int) -> list[Outbox]:
        rows = await self._session.execute(
            select(Outbox)
            .where(Outbox.dispatched_at.is_(None))
            .order_by(Outbox.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(rows.scalars().all())

    async def fetch_next_pending(self) -> Outbox | None:
        rows = await self._session.execute(
            select(Outbox)
            .where(Outbox.dispatched_at.is_(None))
            .order_by(Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        return rows.scalars().first()

    async def mark_dispatched(self, row_ids: Sequence[int]) -> None:
        await self._session.execute(
            update(Outbox)
            .where(Outbox.id.in_(row_ids))
            .values(dispatched_at=func.now())
        )

    async def delete_expired(self, cutoff: datetime, batch_size: int) -> int:
        result = await self._session.execute(
            delete(Outbox).where(
                Outbox.id.in_(
                    select(Outbox.id)
                    .where(
                        Outbox.dispatched_at.is_not(None),
                        Outbox.dispatched_at < cutoff,
                    )
                    .limit(batch_size)
                )
            )
        )
        return int(getattr(result, "rowcount", 0) or 0)
