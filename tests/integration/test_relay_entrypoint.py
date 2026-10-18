import asyncio
import json
import os
import signal
import subprocess
import sys
from time import monotonic
from time import sleep as sync_sleep
from typing import Any
from urllib.parse import urlsplit

import aio_pika
import pytest

from payment_service.config import (
    ApiSettings,
    AppSettings,
    DbSettings,
    LogSettings,
    OutboxSettings,
    ProcessingSettings,
    WebhookSettings,
    get_settings,
)
from payment_service.messaging.topology import (
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    QUEUE_NEW,
    all_queue_names,
)

pytestmark = pytest.mark.integration

RABBIT_URL = os.environ.get("RABBIT_URL", "amqp://payments:payments@localhost:5672/")
MGMT_HOST = urlsplit(RABBIT_URL).hostname or "localhost"
MGMT_PORT = os.environ.get("MANAGEMENT_PORT", "15672")

REQUIRED_ENV = {
    "API_KEYS": "integration",
    "DB_HOST": os.environ.get("DB_HOST", "localhost"),
    "DB_PORT": os.environ.get("DB_PORT", "55432"),
    "DB_USER": os.environ.get("DB_USER", "payments"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD", "payments"),
    "DB_NAME": os.environ.get("DB_NAME", "payments"),
    "RABBIT_URL": RABBIT_URL,
}


def kill_relay_zombies() -> None:
    if not os.path.isdir("/proc"):
        return
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as handle:
                cmdline = handle.read().decode(errors="replace")
        except OSError:
            continue
        if "payment_service.relay" in cmdline and int(entry) != os.getpid():
            try:
                os.kill(int(entry), signal.SIGKILL)
            except ProcessLookupError:
                continue


@pytest.fixture(autouse=True)
def no_relay_zombies() -> None:
    kill_relay_zombies()


async def test_relay_dispose_runs_even_if_broker_stop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal: list[str] = []
    connected = asyncio.Event()
    import importlib

    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    relay_module = importlib.import_module("payment_service.relay")

    class NoopBroker:
        async def connect(self) -> None:
            journal.append("broker.connect")
            connected.set()

        async def stop(self) -> None:
            journal.append("broker.stop")
            raise RuntimeError("broker already closed")

        async def declare_exchange(self, exchange: object) -> object:
            return self

        async def declare_queue(self, queue: object) -> object:
            return self

        async def bind(self, *args: object, **kwargs: object) -> None:
            return None

        async def publish(self, *args: object, **kwargs: object) -> None:
            return None

    class NoopEngine:
        async def dispose(self) -> None:
            journal.append("engine.dispose")

    class InstantRelayLoop:
        def __init__(self, broker: object, maker: object, settings: object) -> None:
            return None

        async def run(self) -> None:
            return None

        async def run_retention(self) -> None:
            return None

    monkeypatch.setattr(relay_module, "create_broker", lambda url: NoopBroker())
    monkeypatch.setattr(
        relay_module,
        "create_engine_and_sessionmaker",
        lambda db: (NoopEngine(), object()),
    )
    monkeypatch.setattr(relay_module, "OutboxRelay", InstantRelayLoop)
    task = asyncio.create_task(relay_module.main())
    await asyncio.wait_for(connected.wait(), timeout=5.0)
    os.kill(os.getpid(), signal.SIGINT)
    await asyncio.wait_for(task, timeout=5.0)
    assert journal == ["broker.connect", "broker.stop", "engine.dispose"]


async def test_relay_entrypoint_declares_topology_then_shuts_down_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal: list[str] = []
    import importlib

    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    relay_module = importlib.import_module("payment_service.relay")

    class RecordingDeclared:
        async def bind(self, *args: object, **kwargs: object) -> None:
            return None

    class RecordingBroker:
        def __init__(self) -> None:
            self.stop_error: BaseException | None = None

        async def connect(self) -> None:
            journal.append("broker.connect")

        async def stop(self) -> None:
            journal.append("broker.stop")
            if self.stop_error is not None:
                raise self.stop_error

        async def declare_exchange(self, exchange: object) -> RecordingDeclared:
            if "declare_topology" not in journal:
                journal.append("declare_topology")
            return RecordingDeclared()

        async def declare_queue(self, queue: object) -> RecordingDeclared:
            return RecordingDeclared()

        async def publish(self, *args: object, **kwargs: object) -> None:
            return None

    class RecordingEngine:
        async def dispose(self) -> None:
            journal.append("engine.dispose")

    cycle_started_event = asyncio.Event()

    class RecordingRelayLoop:
        def __init__(self, broker: object, maker: object, settings: object) -> None:
            return None

        async def run(self) -> None:
            journal.append("cycle_started")
            cycle_started_event.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                journal.append("cycle_cancelled")

        async def run_retention(self) -> None:
            journal.append("retention_started")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                journal.append("retention_cancelled")

    def engine_factory(db: object) -> tuple[RecordingEngine, object]:
        return RecordingEngine(), object()

    broker = RecordingBroker()
    monkeypatch.setattr(relay_module, "create_broker", lambda url: broker)
    monkeypatch.setattr(relay_module, "create_engine_and_sessionmaker", engine_factory)
    monkeypatch.setattr(relay_module, "OutboxRelay", RecordingRelayLoop)

    task = asyncio.create_task(relay_module.main())
    await asyncio.wait_for(cycle_started_event.wait(), timeout=5.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert journal == [
        "broker.connect",
        "declare_topology",
        "cycle_started",
        "retention_started",
        "cycle_cancelled",
        "retention_cancelled",
        "broker.stop",
        "engine.dispose",
    ]


def _topology_settings() -> AppSettings:
    raw_ttls = os.environ.get("RETRY_TTLS_MS", "5000,10000,20000")
    ttls = [int(value) for value in raw_ttls.split(",")]
    return AppSettings(
        api=ApiSettings(api_keys=["integration"]),
        db=DbSettings(
            host="localhost", user="payments", password="payments", name="payments"
        ),
        processing=ProcessingSettings(retry_ttls_ms=ttls),
        webhook=WebhookSettings(),
        outbox=OutboxSettings(),
        log=LogSettings(),
    )


def _cleanup_broker() -> None:
    async def cleanup() -> None:
        connection = await aio_pika.connect(RABBIT_URL)
        try:
            channel = await connection.channel()
            for name in all_queue_names(_topology_settings()):
                await channel.queue_delete(name)
            await channel.exchange_delete(PAYMENTS_EXCHANGE)
            await channel.exchange_delete(DLX_EXCHANGE)
        finally:
            await connection.close()

    asyncio.run(cleanup())


def _topology_declared() -> bool:
    import urllib.error
    import urllib.request

    request = urllib.request.Request(
        f"http://{MGMT_HOST}:{MGMT_PORT}/api/queues/%2F/{QUEUE_NEW}",
        headers={"Authorization": "Basic cGF5bWVudHM6cGF5bWVudHM="},
    )
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            status: int = response.status
            return status == 200
    except (urllib.error.URLError, urllib.error.HTTPError):
        return False


def _read_output(process: subprocess.Popen[bytes]) -> str:
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    assert process.stdout is not None
    return process.stdout.read().decode()


def test_relay_subprocess_sigterm_smoke() -> None:
    _cleanup_broker()
    env = {**os.environ, **REQUIRED_ENV}
    process: Any = subprocess.Popen(
        [sys.executable, "-m", "payment_service.relay"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=os.getcwd(),
    )
    try:
        deadline = monotonic() + 20.0
        started = False
        while monotonic() < deadline and not started:
            if process.poll() is not None:
                break
            sync_sleep(0.5)
            started = _topology_declared()
        assert started, "relay did not declare topology in time"
        process.send_signal(signal.SIGTERM)
    finally:
        output = _read_output(process)
    assert process.returncode == 0
    assert "relay_started" in output
    assert "relay_stopped" in output
    assert "Traceback" not in output
    started_lines = [
        line
        for line in output.splitlines()
        if line.startswith("{") and "relay_started" in line
    ]
    assert started_lines
    event: dict[str, Any] = json.loads(started_lines[0])
    assert "request_id" not in event
    _cleanup_broker()
