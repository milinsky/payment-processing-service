import os
from collections.abc import AsyncGenerator
from pathlib import Path

import aio_pika
import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from alembic import command
from payment_service.config import AppSettings, DbSettings, RabbitSettings, get_settings
from payment_service.messaging.broker import create_broker
from payment_service.messaging.topology import (
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    all_queue_names,
)

_PROJECT_ROOT = Path(__file__).parents[2]


def _db_settings(pg_env: dict[str, str]) -> DbSettings:
    return DbSettings(
        host=pg_env["DB_HOST"],
        port=int(pg_env["DB_PORT"]),
        user=pg_env["DB_USER"],
        password=pg_env["DB_PASSWORD"],
        name=pg_env["DB_NAME"],
    )


@pytest.fixture(scope="session")
def migrated_schema(compose_pg_env: dict[str, str]) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        for key, value in compose_pg_env.items():
            monkeypatch.setenv(key, value)
        get_settings.cache_clear()
        config = Config(str(_PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(_PROJECT_ROOT / "alembic"))
        command.upgrade(config, "head")
        get_settings.cache_clear()


@pytest_asyncio.fixture(scope="session")
async def db_engine(
    compose_pg_env: dict[str, str],
    migrated_schema: None,
) -> AsyncGenerator[AsyncEngine]:
    engine = create_async_engine(
        _db_settings(compose_pg_env).dsn,
        pool_pre_ping=True,
        poolclass=NullPool,
    )
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(
    db_engine: AsyncEngine,
) -> AsyncGenerator[AsyncSession]:
    maker = async_sessionmaker(db_engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    async with db_engine.begin() as connection:
        await connection.execute(
            sa.text("TRUNCATE payments, outbox RESTART IDENTITY CASCADE")
        )


_RABBIT_PASSTHROUGH = {
    key: os.environ[key]
    for key in ("RABBIT_URL", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
    if key in os.environ
}

_RABBIT_REQUIRED_ENV = {
    "API_KEYS": "integration",
    "DB_HOST": "localhost",
    "DB_PORT": "55432",
    "DB_USER": "payments",
    "DB_PASSWORD": "payments",
    "DB_NAME": "payments",
    "RABBIT_URL": "amqp://payments:payments@localhost:5672/",
}


@pytest.fixture
def rabbit_settings(monkeypatch: pytest.MonkeyPatch) -> AppSettings:
    for key, value in {**_RABBIT_REQUIRED_ENV, **_RABBIT_PASSTHROUGH}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("RETRY_TTLS_MS", raising=False)
    get_settings.cache_clear()
    return get_settings()


@pytest.fixture
def rabbit_url(rabbit_settings: AppSettings) -> str:
    return RabbitSettings().url


async def _delete_payments_entities(settings: AppSettings) -> None:
    connection = await aio_pika.connect(RabbitSettings().url)
    try:
        channel = await connection.channel()
        for name in all_queue_names(settings):
            await channel.queue_delete(name)
        await channel.exchange_delete(PAYMENTS_EXCHANGE)
        await channel.exchange_delete(DLX_EXCHANGE)
    finally:
        await connection.close()


@pytest_asyncio.fixture
async def clean_rabbit(
    rabbit_settings: AppSettings,
) -> AsyncGenerator[RabbitBroker]:
    await _delete_payments_entities(rabbit_settings)
    broker = create_broker(RabbitSettings().url)
    await broker.connect()
    try:
        yield broker
    finally:
        await broker.stop()
        await _delete_payments_entities(rabbit_settings)
