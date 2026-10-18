import asyncio

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, UniqueConstraint

from payment_service.config import DbSettings
from payment_service.db import (
    NAMING_CONVENTION,
    Base,
    create_engine_and_sessionmaker,
)

pytestmark = pytest.mark.unit


def test_base_metadata_uses_naming_convention() -> None:
    assert Base.metadata.naming_convention == NAMING_CONVENTION


def test_unique_constraint_name_is_deterministic() -> None:
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    table = Table(
        "t",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("k", Integer, nullable=False),
        UniqueConstraint("k"),
    )
    unique_names = [
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert unique_names == ["t_k_key"]


def test_create_engine_and_sessionmaker_builds_from_dsn() -> None:
    engine, sessionmaker = create_engine_and_sessionmaker(
        DbSettings(host="dbhost", port=55432, user="u", password="p", name="n")
    )
    assert (
        engine.url.render_as_string(hide_password=False)
        == "postgresql+asyncpg://u:p@dbhost:55432/n"
    )
    assert sessionmaker.kw.get("expire_on_commit") is False
    asyncio.run(engine.dispose())
