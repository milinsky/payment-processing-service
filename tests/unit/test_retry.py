from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import structlog.testing
from faststream.exceptions import NackMessage, RejectMessage

from payment_service.messaging.events import PaymentCreatedEvent, PaymentEventData
from payment_service.processing.retry import (
    RETRY_COUNT_HEADER,
    DeadLetter,
    RetryNow,
    handle_with_retry,
    retry_target,
)
from payment_service.processing.service import ProcessingOutcome

pytestmark = pytest.mark.unit


def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        data=PaymentEventData(payment_id=uuid4()),
    )


def make_processor(
    outcome: ProcessingOutcome | BaseException,
) -> Any:
    processor = MagicMock()
    if isinstance(outcome, BaseException):
        processor.handle = AsyncMock(side_effect=outcome)
    else:
        processor.handle = AsyncMock(return_value=outcome)
    return processor


def make_broker(publish_error: BaseException | None = None) -> Any:
    broker = MagicMock()
    if publish_error is not None:
        broker.publish = AsyncMock(side_effect=publish_error)
    else:
        broker.publish = AsyncMock()
    return broker


BODY = b'{"event_id":"x"}'


def published_headers(broker: Any) -> dict[str, Any]:
    headers: dict[str, Any] = broker.publish.call_args.kwargs["headers"]
    return headers


def published_routing_key(broker: Any) -> str:
    routing_key: str = broker.publish.call_args.kwargs["routing_key"]
    return routing_key


def test_retry_target_levels() -> None:
    first = retry_target(0, 3)
    assert isinstance(first, RetryNow)
    assert first.level == 1
    third = retry_target(2, 3)
    assert isinstance(third, RetryNow)
    assert third.level == 3
    assert isinstance(retry_target(3, 3), DeadLetter)


async def test_first_failure_publishes_to_retry_1() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    result = await handle_with_retry(
        broker, processor, BODY, {}, make_event(), max_attempts=3
    )
    assert result == "retry_scheduled"
    assert broker.publish.await_count == 1
    assert published_routing_key(broker) == "payments.retry.1"
    assert published_headers(broker)[RETRY_COUNT_HEADER] == 1
    assert broker.publish.call_args.args[0] == BODY


async def test_third_attempt_failure_publishes_to_retry_3() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    headers: dict[str, Any] = {RETRY_COUNT_HEADER: 2}
    await handle_with_retry(broker, processor, BODY, headers, make_event(), 3)
    assert published_routing_key(broker) == "payments.retry.3"
    assert published_headers(broker)[RETRY_COUNT_HEADER] == 3


async def test_exhausted_attempts_reject() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    headers: dict[str, Any] = {RETRY_COUNT_HEADER: 3}
    with pytest.raises(RejectMessage):
        await handle_with_retry(broker, processor, BODY, headers, make_event(), 3)
    broker.publish.assert_not_awaited()


async def test_publish_failure_raises_nack() -> None:
    broker = make_broker(publish_error=RuntimeError("broker down"))
    processor = make_processor(RuntimeError("tech failure"))
    with pytest.raises(NackMessage):
        await handle_with_retry(broker, processor, BODY, {}, make_event(), 3)
    assert broker.publish.await_count == 1


async def test_ack_outcome_publishes_nothing() -> None:
    broker = make_broker()
    processor = make_processor(ProcessingOutcome.ACKED_BUSINESS)
    result = await handle_with_retry(broker, processor, BODY, {}, make_event(), 3)
    assert result == "acked"
    broker.publish.assert_not_awaited()


async def test_poison_rejects_without_publish() -> None:
    broker = make_broker()
    processor = make_processor(ProcessingOutcome.REJECTED_POISON)
    with pytest.raises(RejectMessage):
        await handle_with_retry(broker, processor, BODY, {}, make_event(), 3)
    broker.publish.assert_not_awaited()


async def test_missing_header_counts_as_first_attempt() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    await handle_with_retry(broker, processor, BODY, {}, make_event(), 3)
    assert published_routing_key(broker) == "payments.retry.1"
    assert published_headers(broker)[RETRY_COUNT_HEADER] == 1


async def test_body_is_forwarded_byte_for_byte() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    body = b'\x00binary{"a":1}\xff'
    await handle_with_retry(broker, processor, body, {}, make_event(), 3)
    assert broker.publish.call_args.args[0] is body


async def test_message_id_is_forwarded() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    await handle_with_retry(
        broker,
        processor,
        BODY,
        {},
        make_event(),
        3,
        message_id="original-message-id",
    )
    assert broker.publish.call_args.kwargs["message_id"] == "original-message-id"


async def test_retry_copy_is_published_persistent() -> None:
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    await handle_with_retry(broker, processor, BODY, {}, make_event(), 3)
    assert broker.publish.call_args.kwargs["persist"] is True


async def test_dead_letter_logs_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr("payment_service.processing.retry.logger", capture)
    broker = make_broker()
    processor = make_processor(RuntimeError("tech failure"))
    headers: dict[str, Any] = {RETRY_COUNT_HEADER: 3}
    with pytest.raises(RejectMessage):
        await handle_with_retry(broker, processor, BODY, headers, make_event(), 3)
    warnings = [call for call in capture.calls if call.method_name == "warning"]
    assert warnings
    assert warnings[0].kwargs["exc_info"] is True
    assert "event_id" in warnings[0].kwargs
    assert "payment_id" in warnings[0].kwargs


async def test_retry_publish_failure_logs_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr("payment_service.processing.retry.logger", capture)
    broker = make_broker(publish_error=RuntimeError("broker down"))
    processor = make_processor(RuntimeError("tech failure"))
    with pytest.raises(NackMessage):
        await handle_with_retry(broker, processor, BODY, {}, make_event(), 3)
    warnings = [call for call in capture.calls if call.method_name == "warning"]
    assert warnings
    assert warnings[0].kwargs["exc_info"] is True
    assert warnings[0].kwargs["level"] == 1
