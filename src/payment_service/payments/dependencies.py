import hmac
from collections.abc import AsyncGenerator
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import AppSettings, get_settings
from payment_service.payments.models import Payment
from payment_service.payments.service import get_by_id


async def verify_api_key(
    settings: Annotated[AppSettings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> str:
    if x_api_key is None:
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "ApiKey"})
    for candidate in settings.api.api_keys:
        if hmac.compare_digest(x_api_key, candidate):
            return x_api_key
    raise HTTPException(status_code=401, headers={"WWW-Authenticate": "ApiKey"})


async def get_session(request: Request) -> AsyncGenerator[AsyncSession]:
    maker: async_sessionmaker[AsyncSession] = request.app.state.session_maker
    async with maker() as session:
        yield session


async def valid_payment_id(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Payment:
    payment = await get_by_id(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="payment not found")
    return payment
