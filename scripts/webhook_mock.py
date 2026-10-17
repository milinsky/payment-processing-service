import hashlib
import hmac
import json
import logging
import os
from typing import Annotated
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("webhook_mock")

app = FastAPI(title="Webhook Mock")

_events: list[dict[str, object]] = []
_failed_count = 0

_FAIL_FIRST_N = int(os.environ.get("FAIL_FIRST_N", "0"))
_SECRET = os.environ.get("WEBHOOK_SECRET", "") or None


@app.post("/webhook")
async def receive_webhook(request: Request) -> JSONResponse:
    global _failed_count
    body = await request.body()
    if _SECRET is not None:
        signature = hmac.new(_SECRET.encode(), body, hashlib.sha256).hexdigest()
        provided = request.headers.get("X-Signature", "")
        if not hmac.compare_digest(signature, provided):
            raise HTTPException(status_code=401, detail="invalid signature")
    if _failed_count < _FAIL_FIRST_N:
        _failed_count += 1
        logger.info(
            json.dumps({"event": "webhook_injected_failure", "attempt": _failed_count})
        )
        raise HTTPException(status_code=500, detail="injected failure")
    try:
        payload = json.loads(body)
    except ValueError:
        payload = {"raw": body.decode(errors="replace")}
    _events.append(payload)
    logger.info(json.dumps({"event": "webhook_received", "payload": payload}))
    return JSONResponse(status_code=200, content={"status": "accepted"})


@app.get("/events")
async def list_events(
    payment_id: Annotated[UUID | None, Query()] = None,
) -> dict[str, object]:
    items = _events
    if payment_id is not None:
        items = [
            event
            for event in _events
            if str(event.get("payment_id")) == str(payment_id)
        ]
    return {"count": len(items), "items": items}


@app.delete("/events")
async def reset_events() -> dict[str, int]:
    dropped = len(_events)
    _events.clear()
    _failed_count = 0
    return {"dropped": dropped}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="warning")
