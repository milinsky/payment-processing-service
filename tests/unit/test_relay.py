import asyncio
import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
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


@dataclass
class RelayState:
    journal: list[str] = field(default_factory=list)
    marked: list[list[int]] = field(default_factory=list)
    published: list[str] = field(default_factory=list)
    pending: list[Outbox] = field(default_factory=list)
    dispatched: set[int] = field(default_factory=set)
    holders: dict[int, Any] = field(default_factory=dict)
    closed: set[Any] = field(default_factory=set)
    sessions: list[Any] = field(default_factory=list)
    fetch_error: BaseException | None = None


class RepositoryStub:
    def __init__(self, session: Any, state: RelayState) -> None:
        self._session = session
        self._state = state

    async def fetch_next_pending(self) -> Outbox | None:
        self._state.journal.append("fetch")
        if self._state.fetch_error is not None:
            raise self._state.fetch_error
        for row in self._state.pending:
            if row.id in self._state.dispatched:
                continue
            holder = self._state.holders.get(row.id)
            if holder is not None and holder not in self._state.closed:
                continue
            self._state.holders[row.id] = self._session
            return row
        return None

    async def mark_dispatched(self, row_ids: Sequence[int]) -> None:
        self._state.journal.append("mark")
        self._state.marked.append(list(row_ids))
        self._state.dispatched.update(row_ids)


def make_session_stub(state: RelayState) -> Any:
    session: Any = MagicMock()

    async def commit() -> None:
        state.journal.append("commit")

    async def close() -> None:
        state.journal.append("close")
        state.closed.add(session)

    session.commit = AsyncMock(side_effect=commit)
    session.close = AsyncMock(side_effect=close)
    return session


def make_relay_parts(
    monkeypatch: pytest.MonkeyPatch,
    pending: list[Outbox],
    publish_side_effect: Callable[..., Any] | None = None,
    poll_interval_s: float = 0.5,
    batch_size: int = 100,
    fetch_error: BaseException | None = None,
    max_held_sessions: int = 10,
) -> tuple[OutboxRelay, MagicMock, RelayState]:
    state = RelayState(pending=pending, fetch_error=fetch_error)

    async def publish_wrapper(*args: Any, **kwargs: Any) -> Any:
        state.journal.append("publish")
        state.published.append(str(kwargs["message_id"]))
        result = publish_side_effect(*args, **kwargs) if publish_side_effect else None
        if inspect.isawaitable(result):
            await result
        return result

    broker = MagicMock()
    broker.publish = AsyncMock(side_effect=publish_wrapper)

    def make_session() -> Any:
        session = make_session_stub(state)
        state.sessions.append(session)
        return session

    def make_repo(session: Any) -> RepositoryStub:
        return RepositoryStub(session, state)

    monkeypatch.setattr("payment_service.outbox.relay.OutboxRepository", make_repo)
    maker = MagicMock(side_effect=make_session)
    settings: Any = MagicMock()
    settings.outbox.poll_interval_s = poll_interval_s
    settings.outbox.batch_size = batch_size
    settings.outbox.max_held_sessions = max_held_sessions
    relay = OutboxRelay(broker, maker, settings)
    return relay, broker, state


async def test_fetch_publish_mark_commit_per_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, broker, state = make_relay_parts(monkeypatch, [outbox_row(1)])
    await relay.run_once()
    assert state.journal == [
        "fetch",
        "publish",
        "mark",
        "commit",
        "close",
        "fetch",
        "close",
    ]
    assert state.marked == [[1]]


async def test_failed_publish_marks_only_successful_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failing_row = outbox_row(1)
    successful_row = outbox_row(2)
    attempts: list[int] = []

    def publish_side_effect(*args: Any, **kwargs: Any) -> Any:
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise RuntimeError("broker down")
        return None

    relay, broker, state = make_relay_parts(
        monkeypatch,
        [failing_row, successful_row],
        publish_side_effect=publish_side_effect,
    )
    await relay.run_once()
    assert state.journal == [
        "fetch",
        "publish",
        "fetch",
        "publish",
        "mark",
        "commit",
        "close",
        "fetch",
        "close",
        "close",
    ]
    assert state.published == [
        str(failing_row.event_id),
        str(successful_row.event_id),
    ]
    assert state.marked == [[successful_row.id]]
    failed_session = state.sessions[0]
    failed_session.commit.assert_not_awaited()
    failed_session.close.assert_awaited_once()
    successful_session = state.sessions[1]
    successful_session.commit.assert_awaited_once()


async def test_publish_failures_stop_loop_at_held_session_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [outbox_row(1), outbox_row(2), outbox_row(3), outbox_row(4)]

    def publish_side_effect(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("broker down")

    relay, broker, state = make_relay_parts(
        monkeypatch,
        rows,
        publish_side_effect=publish_side_effect,
        max_held_sessions=2,
    )
    await relay.run_once()
    assert len(state.sessions) == 2
    assert broker.publish.await_count == 2
    assert state.published == [str(rows[0].event_id), str(rows[1].event_id)]
    assert state.marked == []
    assert state.journal == [
        "fetch",
        "publish",
        "fetch",
        "publish",
        "close",
        "close",
    ]


async def test_run_once_processes_at_most_batch_size_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [outbox_row(1), outbox_row(2), outbox_row(3)]
    relay, broker, state = make_relay_parts(monkeypatch, rows, batch_size=2)
    await relay.run_once()
    assert state.published == [str(rows[0].event_id), str(rows[1].event_id)]
    assert state.marked == [[1], [2]]
    assert 3 not in state.dispatched


async def test_empty_batch_never_publishes(monkeypatch: pytest.MonkeyPatch) -> None:
    relay, broker, state = make_relay_parts(monkeypatch, [], poll_interval_s=0.25)
    assert state.journal == []
    await relay.run_once()
    assert state.journal == ["fetch", "close"]
    assert state.marked == []
    broker.publish.assert_not_awaited()


async def test_run_sleeps_poll_interval_between_iterations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, broker, _ = make_relay_parts(monkeypatch, [], poll_interval_s=0.75)
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        raise asyncio.CancelledError

    monkeypatch.setattr("payment_service.outbox.relay.asyncio.sleep", fake_sleep)
    await relay.run()
    assert sleep_delays == [0.75]
    broker.publish.assert_not_awaited()


async def test_run_returns_cleanly_on_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, broker, _ = make_relay_parts(monkeypatch, [], poll_interval_s=0.01)
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0)
    task.cancel()
    await task
    broker.publish.assert_not_awaited()


async def test_run_survives_database_error_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay, broker, _ = make_relay_parts(
        monkeypatch,
        [],
        poll_interval_s=0.3,
        fetch_error=RuntimeError("database unavailable"),
    )
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
    relay, _, _ = make_relay_parts(
        monkeypatch,
        [],
        poll_interval_s=0.3,
        fetch_error=RuntimeError("database unavailable"),
    )

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

    relay, _, state = make_relay_parts(
        monkeypatch, [row], publish_side_effect=publish_side_effect
    )
    await relay.run_once()
    warnings = [call for call in capture.calls if call.method_name == "warning"]
    assert warnings
    assert warnings[0].args == ("outbox_publish_failed",)
    assert warnings[0].kwargs["exc_info"] is True
    assert warnings[0].kwargs["event_id"] == str(row.event_id)
    assert state.marked == []


async def test_failed_row_republished_on_next_run_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = outbox_row(1)
    attempts: list[int] = []

    def publish_side_effect(*args: Any, **kwargs: Any) -> Any:
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise RuntimeError("broker hiccup")
        return None

    relay, _, state = make_relay_parts(
        monkeypatch, [row], publish_side_effect=publish_side_effect
    )
    await relay.run_once()
    assert state.marked == []
    await relay.run_once()
    assert state.journal == [
        "fetch",
        "publish",
        "fetch",
        "close",
        "close",
        "fetch",
        "publish",
        "mark",
        "commit",
        "close",
        "fetch",
        "close",
    ]
    assert state.published == [str(row.event_id), str(row.event_id)]
    assert state.marked == [[row.id]]


async def test_cancelled_publish_propagates_out_of_run_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def publish_cancel(*args: Any, **kwargs: Any) -> Any:
        raise asyncio.CancelledError

    row = outbox_row(1)
    relay, broker, state = make_relay_parts(
        monkeypatch, [row], publish_side_effect=publish_cancel
    )
    with pytest.raises(asyncio.CancelledError):
        await relay.run_once()
    assert state.journal == ["fetch", "publish", "close"]
    assert state.marked == []


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
