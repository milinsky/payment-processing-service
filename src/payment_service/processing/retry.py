from dataclasses import dataclass
from typing import Literal

import structlog
from aio_pika.abc import HeadersType
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitBroker

from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import (
    QUEUE_RETRY_FORMAT,
    payments_exchange,
)
from payment_service.processing.service import (
    PaymentProcessor,
    ProcessingOutcome,
)

logger = structlog.get_logger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"


@dataclass(frozen=True, slots=True)
class RetryNow:
    level: int


@dataclass(frozen=True, slots=True)
class DeadLetter:
    pass


RetryDecision = RetryNow | DeadLetter


def retry_target(attempt: int, max_attempts: int) -> RetryDecision:
    if attempt + 1 <= max_attempts:
        return RetryNow(level=attempt + 1)
    return DeadLetter()


async def handle_with_retry(
    broker: RabbitBroker,
    processor: PaymentProcessor,
    message_body: bytes,
    headers: HeadersType,
    event: PaymentCreatedEvent,
    max_attempts: int,
    message_id: str | None = None,
) -> Literal["acked", "retry_scheduled"]:
    raw_attempt = headers.get(RETRY_COUNT_HEADER, 0)
    attempt = raw_attempt if isinstance(raw_attempt, int) else int(str(raw_attempt))
    try:
        outcome = await processor.handle(event)
    except Exception:
        decision = retry_target(attempt, max_attempts)
        if isinstance(decision, DeadLetter):
            logger.warning(
                "message_dead_lettered",
                event_id=str(event.event_id),
                payment_id=str(event.data.payment_id),
                attempt=attempt,
                exc_info=True,
            )
            raise RejectMessage() from None
        retry_headers: HeadersType = {
            key: value for key, value in headers.items() if key != RETRY_COUNT_HEADER
        }
        retry_headers[RETRY_COUNT_HEADER] = decision.level
        try:
            await broker.publish(
                message_body,
                exchange=payments_exchange(),
                routing_key=QUEUE_RETRY_FORMAT.format(decision.level),
                headers=retry_headers,
                message_id=message_id,
                persist=True,
            )
        except Exception:
            logger.warning(
                "retry_publish_failed",
                event_id=str(event.event_id),
                payment_id=str(event.data.payment_id),
                level=decision.level,
                exc_info=True,
            )
            raise NackMessage() from None
        return "retry_scheduled"
    if outcome is ProcessingOutcome.REJECTED_POISON:
        raise RejectMessage()
    return "acked"
