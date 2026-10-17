from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.payments import service as payments_service
from payment_service.payments.dependencies import (
    get_session,
    valid_payment_id,
    verify_api_key,
)
from payment_service.payments.models import Payment
from payment_service.payments.schemas import PaymentCreate, PaymentRead

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


async def idempotency_conflict_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": "idempotency key already used with different payload"},
    )


IDEMPOTENCY_KEY_MAX_LENGTH = 255


def _to_read(payment: Payment) -> PaymentRead:
    return PaymentRead(
        payment_id=payment.id,
        status=payment.status,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        processed_at=payment.processed_at,
        created_at=payment.created_at,
    )


@router.post(
    "",
    status_code=202,
    response_model=PaymentRead,
    summary="Create payment",
    description=(
        "Creates a payment with pending status and schedules a payment.created"
        " event through the transactional outbox. Repeating the request with the"
        " same Idempotency-Key and the same body returns the existing payment"
        " with 200 and the Idempotency-Replayed header; the same key with a"
        " different body is rejected with 409."
    ),
    responses={
        200: {
            "description": "Idempotent replay of an existing payment",
        },
        409: {
            "description": "Idempotency key already used with a different payload",
        },
        401: {"description": "Missing or invalid X-API-Key"},
        422: {
            "description": "Invalid body or missing Idempotency-Key header",
        },
    },
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=IDEMPOTENCY_KEY_MAX_LENGTH,
        ),
    ],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentRead | JSONResponse:
    result = await payments_service.create_idempotent(session, payload, idempotency_key)
    if result.replayed:
        return JSONResponse(
            content=_to_read(result.payment).model_dump(mode="json"),
            status_code=200,
            headers={"Idempotency-Replayed": "true"},
        )
    return _to_read(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get payment",
    description=(
        "Returns the payment by its identifier with the current status,"
        " amount as a string with two decimal places, description, metadata,"
        " processing timestamp and creation timestamp in UTC."
    ),
    responses={
        404: {"description": "Payment not found"},
        401: {"description": "Missing or invalid X-API-Key"},
        422: {"description": "Invalid payment identifier"},
    },
)
async def get_payment(
    payment: Annotated[Payment, Depends(valid_payment_id)],
) -> PaymentRead:
    return _to_read(payment)
