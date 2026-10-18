import asyncio
from datetime import UTC, datetime, timedelta

import structlog
from faststream.rabbit import RabbitBroker
from sqlalchemy import Select, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import AppSettings
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.messaging.topology import (
    ROUTING_KEY_NEW,
    payments_exchange,
)
from payment_service.outbox.models import Outbox

logger = structlog.get_logger(__name__)


class OutboxRelay:
    def __init__(
        self,
        broker: RabbitBroker,
        sessionmaker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
    ) -> None:
        self._broker = broker
        self._sessionmaker = sessionmaker
        self._settings = settings

    async def run(self) -> None:
        try:
            while True:
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("outbox_relay_iteration_failed", exc_info=True)
                await asyncio.sleep(self._settings.outbox.poll_interval_s)
        except asyncio.CancelledError:
            return

    async def run_retention(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._settings.outbox.cleanup_interval_s)
                try:
                    removed = await self.cleanup_expired()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("outbox_cleanup_failed", exc_info=True)
                    continue
                if removed:
                    logger.info("outbox_cleanup_removed_rows", rows=removed)
        except asyncio.CancelledError:
            return

    async def fetch_batch(self, session: AsyncSession) -> list[Outbox]:
        rows = await session.execute(
            select(Outbox)
            .where(Outbox.dispatched_at.is_(None))
            .order_by(Outbox.id)
            .limit(self._settings.outbox.batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(rows.scalars().all())

    def _expired_ids_statement(self) -> Select[int]:
        retention = timedelta(seconds=self._settings.outbox.retention_s)
        cutoff = datetime.now(UTC) - retention
        return (
            select(Outbox.id)
            .where(Outbox.dispatched_at.is_not(None), Outbox.dispatched_at < cutoff)
            .limit(self._settings.outbox.batch_size)
        )

    async def cleanup_expired(self) -> int:
        async with self._sessionmaker() as session:
            result = await session.execute(
                delete(Outbox).where(Outbox.id.in_(self._expired_ids_statement()))
            )
            await session.commit()
            return int(getattr(result, "rowcount", 0) or 0)

    async def run_once(self) -> None:
        async with self._sessionmaker() as session, session.begin():
            pending = await self.fetch_batch(session)
            if not pending:
                return
            dispatched: list[int] = []
            for row in pending:
                try:
                    await self._publish(row)
                    dispatched.append(row.id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning(
                        "outbox_publish_failed",
                        event_id=str(row.event_id),
                        exc_info=True,
                    )
            if dispatched:
                await session.execute(
                    update(Outbox)
                    .where(Outbox.id.in_(dispatched))
                    .values(dispatched_at=func.now())
                )
            logger.info("outbox_batch_dispatched", batch_size=len(dispatched))

    async def _publish(self, row: Outbox) -> None:
        event = PaymentCreatedEvent(
            event_id=row.event_id,
            occurred_at=row.created_at,
            data=PaymentEventData(payment_id=row.aggregate_id),
        )
        await self._broker.publish(
            event.model_dump_json(),
            exchange=payments_exchange(),
            routing_key=ROUTING_KEY_NEW,
            message_id=str(row.event_id),
            persist=True,
        )
