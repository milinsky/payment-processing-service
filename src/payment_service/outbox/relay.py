import asyncio
from datetime import UTC, datetime, timedelta

import structlog
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import AppSettings
from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.messaging.topology import (
    ROUTING_KEY_NEW,
    payments_exchange,
)
from payment_service.outbox.models import Outbox
from payment_service.outbox.repository import OutboxRepository

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

    async def cleanup_expired(self) -> int:
        async with self._sessionmaker() as session:
            repo = OutboxRepository(session)
            retention = timedelta(seconds=self._settings.outbox.retention_s)
            cutoff = datetime.now(UTC) - retention
            removed = await repo.delete_expired(
                cutoff, self._settings.outbox.batch_size
            )
            await session.commit()
            return removed

    async def run_once(self) -> None:
        held_sessions: list[AsyncSession] = []
        dispatched = 0
        processed = 0
        try:
            for _ in range(self._settings.outbox.batch_size):
                session = self._sessionmaker()
                publish_failed = False
                try:
                    repo = OutboxRepository(session)
                    row = await repo.fetch_next_pending()
                    if row is None:
                        break
                    processed += 1
                    try:
                        await self._publish(row)
                    except Exception:
                        logger.warning(
                            "outbox_publish_failed",
                            event_id=str(row.event_id),
                            exc_info=True,
                        )
                        held_sessions.append(session)
                        publish_failed = True
                        if (
                            len(held_sessions)
                            >= self._settings.outbox.max_held_sessions
                        ):
                            break
                        continue
                    await repo.mark_dispatched([row.id])
                    await session.commit()
                    dispatched += 1
                finally:
                    if not publish_failed:
                        await session.close()
        finally:
            for held_session in held_sessions:
                await held_session.close()
        if processed:
            logger.info("outbox_batch_dispatched", batch_size=dispatched)

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
