import asyncio
import signal

import structlog

from payment_service.config import RabbitSettings, get_settings
from payment_service.db import create_engine_and_sessionmaker
from payment_service.logging import configure_logging
from payment_service.messaging.broker import create_broker
from payment_service.messaging.topology import declare_full_topology
from payment_service.outbox.relay import OutboxRelay

logger = structlog.get_logger(__name__)


async def main() -> None:
    settings = get_settings()
    engine, sessionmaker = create_engine_and_sessionmaker(settings.db)
    broker = create_broker(RabbitSettings().url)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop_event.set)
    relay_task: asyncio.Task[None] | None = None
    retention_task: asyncio.Task[None] | None = None
    try:
        await broker.connect()
        await declare_full_topology(broker, settings)
        relay = OutboxRelay(broker, sessionmaker, settings)
        relay_task = asyncio.create_task(relay.run())
        retention_task = asyncio.create_task(relay.run_retention())
        logger.info("relay_started")
        await stop_event.wait()
    finally:
        for task in (relay_task, retention_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (relay_task, retention_task) if task is not None),
            return_exceptions=True,
        )
        try:
            await broker.stop()
        except Exception:
            logger.warning("relay_broker_stop_failed", exc_info=True)
        await engine.dispose()
        logger.info("relay_stopped")


if __name__ == "__main__":
    configure_logging(get_settings().log.level)
    asyncio.run(main())
