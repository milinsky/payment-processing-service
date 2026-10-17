import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

pytestmark = pytest.mark.integration


async def test_select_one(db_engine: AsyncEngine) -> None:
    async with db_engine.connect() as connection:
        scalar = (await connection.execute(text("SELECT 1"))).scalar()
    assert scalar == 1


async def test_payments_and_outbox_tables_exist(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = 'public'"
        )
    )
    tables = {row[0] for row in result}
    assert {"payments", "outbox"} <= tables
