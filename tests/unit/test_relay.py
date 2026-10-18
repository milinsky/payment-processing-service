import asyncio
import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import structlog.testing

from payment_service.outbox.models import Outbox
from payment_service.outbox.relay import OutboxRelay

pytestmark = pytest.mark.unit


def outbox_row(row_id: int, event_id: UUID | None = None) -> Outbox:
    return Outbox(
        id=row_id,
        event_id=event_id if event_id is not None else uuid4(),
        aggregate_type="payment",
        aggregate_id=uuid4(),
        event_type="payment.created",
        payload={"payment_id": "x"},
        created_at=datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC),
    )


def make_relay_parts(
    pending: list[Outbox],
    publish_side_effect: Callable[..., Any] | None = None,
    poll_interval_s: float = 0.5,
) -> tuple[OutboxRelay, MagicMock, list[str], list[Any]]:
    if publish_side_effect is None:

        def publish_side_effect(*args: Any, **kwargs: Any) -> None:
            return None

    calls: list[str] = []
    update_statements: list[Any] = []
    broker = MagicMock()

    async def publish_wrapper(*args: Any, **kwargs: Any) -> Any:
        calls.append("publish")
        result = publish_side_effect(*args, **kwargs)
        if inspect.isawaitable(result):
            await result
        return result

    broker.publish = AsyncMock(side_effect=publish_wrapper)
    session: Any = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    begin_ctx: Any = MagicMock()
    begin_ctx.__aenter__ = AsyncMock(return_value=None)
    begin_ctx.__aexit__ = AsyncMock(return_value=False)
    session.begin = MagicMock(return_value=begin_ctx)
    select_result = MagicMock()
    select_result.scalars.return_value.all.return_value = pending
    execute_count = 0

    async def execute_side_effect(statement: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal execute_count
        execute_count += 1
        if execute_count == 1:
            calls.append("select")
            return select_result
        calls.append("update")
        update_statements.append(statement)
        return MagicMock()

    session.execute = AsyncMock(side_effect=execute_side_effect)
    maker = MagicMock(return_value=session)
    settings: Any = MagicMock()
    settings.outbox.poll_interval_s = poll_interval_s
    settings.outbox.batch_size = 100
    relay = OutboxRelay(broker, maker, settings)
    return relay, broker, calls, update_statements


async def test_publish_happens_between_select_and_update() -> None:
    relay, broker, calls, _ = make_relay_parts([outbox_row(1)])
    await relay.run_once()
    assert calls == ["select", "publish", "update"]


async def test_failed_publish_marks_only_successful_rows() -> None:
    failing_row = outbox_row(1)
    successful_row = outbox_row(2)
    attempts: list[int] = []

    def publish_side_effect(*args: Any, **kwargs: Any) -> Any:
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise RuntimeError("broker down")
        return None

    relay, broker, calls, update_statements = make_relay_parts(
        [failing_row, successful_row], publish_side_effect=publish_side_effect
    )
    await relay.run_once()
    assert calls == ["select", "publish", "publish", "update"]
    compiled = str(update_statements[0].compile(compile_kwargs={"literal_binds": True}))
    assert "dispatched_at" in compiled
    assert "IN (2)" in compiled
    assert "IN (1, 2)" not in compiled


async def test_empty_batch_never_publishes(monkeypatch: pytest.MonkeyPatch) -> None:
    relay, broker, calls, _ = make_relay_parts([], poll_interval_s=0.25)
    assert calls == []
    await relay.run_once()
    assert calls == ["select"]
    broker.publish.assert_not_awaited()


async def test_run_sleeps_poll_interval_between_iterations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, broker, _, _ = make_relay_parts([], poll_interval_s=0.75)
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        raise asyncio.CancelledError

    monkeypatch.setattr("payment_service.outbox.relay.asyncio.sleep", fake_sleep)
    await relay.run()
    assert sleep_delays == [0.75]
    broker.publish.assert_not_awaited()


async def test_run_returns_cleanly_on_cancel() -> None:
    relay, broker, _, _ = make_relay_parts([], poll_interval_s=0.01)
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0)
    task.cancel()
    await task
    broker.publish.assert_not_awaited()


async def test_run_survives_database_error_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session: Any = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.execute = AsyncMock(side_effect=RuntimeError("database unavailable"))
    maker = MagicMock(return_value=session)
    broker = MagicMock()
    broker.publish = AsyncMock()
    settings: Any = MagicMock()
    settings.outbox.poll_interval_s = 0.3
    relay = OutboxRelay(broker, maker, settings)
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(sleep_delays) >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("payment_service.outbox.relay.asyncio.sleep", fake_sleep)
    await relay.run()
    assert sleep_delays == [0.3, 0.3]
    broker.publish.assert_not_awaited()


async def test_run_logs_iteration_failure_with_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr("payment_service.outbox.relay.logger", capture)
    session: Any = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.execute = AsyncMock(side_effect=RuntimeError("database unavailable"))
    maker = MagicMock(return_value=session)
    settings: Any = MagicMock()
    settings.outbox.poll_interval_s = 0.3
    relay = OutboxRelay(MagicMock(), maker, settings)

    async def fake_sleep(delay: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("payment_service.outbox.relay.asyncio.sleep", fake_sleep)
    await relay.run()
    warnings = [call for call in capture.calls if call.method_name == "warning"]
    assert warnings
    assert warnings[0].args == ("outbox_relay_iteration_failed",)
    assert warnings[0].kwargs["exc_info"] is True


async def test_publish_failure_is_logged_with_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr("payment_service.outbox.relay.logger", capture)
    row = outbox_row(1)

    async def publish_side_effect(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("broker down")

    relay, broker, calls, _ = make_relay_parts(
        [row], publish_side_effect=publish_side_effect
    )
    await relay.run_once()
    warnings = [call for call in capture.calls if call.method_name == "warning"]
    assert warnings
    assert warnings[0].args == ("outbox_publish_failed",)
    assert warnings[0].kwargs["exc_info"] is True
    assert warnings[0].kwargs["event_id"] == str(row.event_id)


async def test_cycle_processes_next_batch_after_publish_failure() -> None:
    journal: list[str] = []
    broker = MagicMock()
    attempts: list[int] = []

    async def publish_wrapper(*args: Any, **kwargs: Any) -> Any:
        journal.append("publish")
        if len(attempts) == 0:
            attempts.append(1)
            raise RuntimeError("broker hiccup")
        attempts.append(2)
        return None

    broker.publish = AsyncMock(side_effect=publish_wrapper)
    settings: Any = MagicMock()
    settings.outbox.batch_size = 100
    sessions: list[Any] = []
    for pending in ([outbox_row(1)], [outbox_row(2)]):
        session: Any = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        begin_ctx: Any = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=None)
        begin_ctx.__aexit__ = AsyncMock(return_value=False)
        session.begin = MagicMock(return_value=begin_ctx)
        select_result = MagicMock()
        select_result.scalars.return_value.all.return_value = pending
        session.execute = AsyncMock(
            side_effect=[select_result, MagicMock(), MagicMock()]
        )
        sessions.append(session)
    maker = MagicMock(side_effect=sessions)
    relay = OutboxRelay(broker, maker, settings)
    await relay.run_once()
    await relay.run_once()
    assert journal == ["publish", "publish"]
    second_update = sessions[1].execute.call_args_list[1]
    compiled = str(
        second_update.args[0].compile(compile_kwargs={"literal_binds": True})
    )
    assert "dispatched_at" in compiled
    assert "IN (2)" in compiled


async def test_cancelled_publish_propagates_out_of_run_once() -> None:
    async def publish_cancel(*args: Any, **kwargs: Any) -> Any:
        raise asyncio.CancelledError

    row = outbox_row(1)
    relay, broker, calls, _ = make_relay_parts(
        [row], publish_side_effect=publish_cancel
    )
    with pytest.raises(asyncio.CancelledError):
        await relay.run_once()
    assert calls == ["select", "publish"]


def make_retention_parts(
    rowcount: int | BaseException,
    cleanup_interval_s: float = 0.5,
    retention_s: float = 86400.0,
) -> tuple[OutboxRelay, MagicMock, list[Any]]:
    execute_result = MagicMock()
    if isinstance(rowcount, BaseException):
        execute_result.rowcount = None
    else:
        execute_result.rowcount = rowcount
    session: Any = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    statements: list[Any] = []

    async def execute_side_effect(statement: Any, *args: Any, **kwargs: Any) -> Any:
        statements.append(statement)
        if isinstance(rowcount, BaseException):
            raise rowcount
        return execute_result

    session.execute = AsyncMock(side_effect=execute_side_effect)
    session.commit = AsyncMock()
    maker = MagicMock(return_value=session)
    settings: Any = MagicMock()
    settings.outbox.cleanup_interval_s = cleanup_interval_s
    settings.outbox.retention_s = retention_s
    settings.outbox.batch_size = 100
    relay = OutboxRelay(MagicMock(), maker, settings)
    return relay, maker, statements


async def test_cleanup_expired_deletes_only_dispatched_rows() -> None:
    relay, maker, statements = make_retention_parts(rowcount=7)
    removed = await relay.cleanup_expired()
    assert removed == 7
    compiled = str(statements[0])
    assert "dispatched_at" in compiled
    assert "outbox" in compiled
    maker.return_value.commit.assert_awaited_once()


async def test_cleanup_expired_returns_zero_when_nothing_to_delete() -> None:
    relay, maker, _ = make_retention_parts(rowcount=0)
    assert await relay.cleanup_expired() == 0


async def test_run_retention_sleeps_interval_and_survives_db_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, maker, _ = make_retention_parts(
        rowcount=RuntimeError("database unavailable"), cleanup_interval_s=0.3
    )
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(sleep_delays) >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("payment_service.outbox.relay.asyncio.sleep", fake_sleep)
    await relay.run_retention()
    assert sleep_delays == [0.3, 0.3]
    assert maker.return_value.execute.await_count == 1
    maker.return_value.commit.assert_not_awaited()


async def test_run_retention_returns_cleanly_on_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, maker, _ = make_retention_parts(rowcount=1, cleanup_interval_s=60.0)
    task = asyncio.create_task(relay.run_retention())
    await asyncio.sleep(0)
    task.cancel()
    await task
    maker.return_value.execute.assert_not_awaited()


async def test_run_retention_logs_removed_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = structlog.testing.CapturingLogger()
    monkeypatch.setattr("payment_service.outbox.relay.logger", capture)
    relay, maker, _ = make_retention_parts(rowcount=3, cleanup_interval_s=0.01)
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(sleep_delays) >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("payment_service.outbox.relay.asyncio.sleep", fake_sleep)
    await relay.run_retention()
    infos = [call for call in capture.calls if call.method_name == "info"]
    assert any(call.kwargs.get("rows") == 3 for call in infos)
