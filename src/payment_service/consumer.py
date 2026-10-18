import asyncio
from dataclasses import dataclass
from typing import Annotated

import structlog
from faststream import Context, FastStream
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit.message import RabbitMessage
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.config import RabbitSettings, get_settings
from payment_service.db import create_engine_and_sessionmaker
from payment_service.logging import configure_logging
from payment_service.messaging.broker import create_broker
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import (
    declare_full_topology,
    new_queue,
    payments_exchange,
)
from payment_service.processing.gateway import RandomGateway
from payment_service.processing.retry import handle_with_retry
from payment_service.processing.service import PaymentProcessor
from payment_service.processing.webhook import WebhookClient

logger = structlog.get_logger(__name__)

broker = create_broker(RabbitSettings().url)
app = FastStream(broker)


@dataclass
class _Runtime:
    processor: PaymentProcessor | None = None
    engine: AsyncEngine | None = None
    webhook: WebhookClient | None = None


runtime = _Runtime()


@app.on_startup
async def setup() -> None:
    settings = get_settings()
    await broker.connect()
    await declare_full_topology(broker, settings)
    engine, sessionmaker = create_engine_and_sessionmaker(settings.db)
    runtime.engine = engine
    runtime.webhook = WebhookClient(settings.webhook)
    runtime.processor = PaymentProcessor(
        sessionmaker,
        RandomGateway(
            settings.processing.success_rate,
            settings.processing.delay_min_s,
            settings.processing.delay_max_s,
        ),
        runtime.webhook,
    )
    logger.info("consumer_started")


@app.after_shutdown
async def teardown() -> None:
    if runtime.webhook is not None:
        await runtime.webhook.aclose()
    if runtime.engine is not None:
        await runtime.engine.dispose()
    logger.info("consumer_stopped")


@broker.subscriber(queue=new_queue(), exchange=payments_exchange())
async def process_payment(
    message: Annotated[RabbitMessage, Context("message")],
) -> None:
    body = message.body
    headers = dict(message.headers or {})
    try:
        event = PaymentCreatedEvent.model_validate_json(body)
    except ValidationError:
        logger.warning("event_parse_failed", message_id=message.message_id)
        raise RejectMessage() from None
    structlog.contextvars.bind_contextvars(
        event_id=str(event.event_id),
        payment_id=str(event.data.payment_id),
    )
    try:
        processor = runtime.processor
        if processor is None:
            await asyncio.sleep(1.0)
            raise NackMessage()
        max_attempts = get_settings().processing.retry_max_attempts
        try:
            outcome = await handle_with_retry(
                broker,
                processor,
                body,
                headers,
                event,
                max_attempts,
                message_id=message.message_id,
            )
        except RejectMessage:
            logger.warning(
                "message_rejected",
                event_id=str(event.event_id),
                payment_id=str(event.data.payment_id),
            )
            raise
        if outcome == "retry_scheduled":
            logger.info(
                "message_retry_scheduled",
                event_id=str(event.event_id),
                payment_id=str(event.data.payment_id),
            )
        else:
            logger.info(
                "message_processed",
                event_id=str(event.event_id),
                payment_id=str(event.data.payment_id),
            )
    finally:
        structlog.contextvars.unbind_contextvars("event_id", "payment_id")


if __name__ == "__main__":
    configure_logging(get_settings().log.level)
    asyncio.run(app.run())
