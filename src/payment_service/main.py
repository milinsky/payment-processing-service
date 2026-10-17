from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid7

import structlog
from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from payment_service.config import AppSettings, get_settings
from payment_service.db import create_engine_and_sessionmaker
from payment_service.logging import configure_logging
from payment_service.payments.exceptions import IdempotencyKeyConflict
from payment_service.payments.router import (
    idempotency_conflict_handler,
)
from payment_service.payments.router import (
    router as payments_router,
)

health_router = APIRouter(tags=["health"])


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(
            (key.decode().lower(), value.decode())
            for key, value in scope.get("headers", [])
        )
        raw_request_id = headers.get("x-request-id")
        try:
            request_id = str(UUID(raw_request_id)) if raw_request_id else str(uuid7())
        except ValueError:
            request_id = str(uuid7())
        structlog.contextvars.bind_contextvars(request_id=request_id)

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers["x-request-id"] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            structlog.contextvars.unbind_contextvars("request_id")


@health_router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@health_router.get("/health/ready", response_model=None)
async def health_ready(request: Request) -> Response:
    try:
        async with request.app.state.session_maker() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        return JSONResponse(status_code=503, content={"database": "unavailable"})
    return JSONResponse(status_code=200, content={"database": "ok"})


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = app.state.settings
    engine, sessionmaker = create_engine_and_sessionmaker(settings.db)
    app.state.engine = engine
    app.state.session_maker = sessionmaker
    try:
        yield
    finally:
        await engine.dispose()


def create_app(settings: AppSettings) -> FastAPI:
    configure_logging(settings.log.level)
    if not settings.api.docs_enabled:
        app = FastAPI(
            title="Payment Processing Service",
            version="0.1.0",
            lifespan=lifespan,
            openapi_url=None,
            docs_url=None,
            redoc_url=None,
        )
    else:
        app = FastAPI(
            title="Payment Processing Service",
            version="0.1.0",
            lifespan=lifespan,
        )
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    app.include_router(payments_router)
    app.include_router(health_router)
    app.add_exception_handler(IdempotencyKeyConflict, idempotency_conflict_handler)
    return app


app = create_app(get_settings())
