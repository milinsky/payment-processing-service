from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from payment_service.config import AppSettings

PAYMENTS_EXCHANGE = "payments"
ROUTING_KEY_NEW = "new"
QUEUE_NEW = "payments.new"
QUEUE_RETRY_FORMAT = "payments.retry.{}"
DLX_EXCHANGE = "payments.dlx"
DEAD_ROUTING_KEY = "payments.dead"
QUEUE_DLQ = "payments.dlq"


def retry_queue_names(settings: AppSettings) -> list[str]:
    return [
        QUEUE_RETRY_FORMAT.format(index)
        for index in range(1, len(settings.processing.retry_ttls_ms) + 1)
    ]


def all_queue_names(settings: AppSettings) -> list[str]:
    return [QUEUE_NEW, *retry_queue_names(settings), QUEUE_DLQ]


def payments_exchange() -> RabbitExchange:
    return RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)


def dlx_exchange() -> RabbitExchange:
    return RabbitExchange(DLX_EXCHANGE, type=ExchangeType.DIRECT, durable=True)


def new_queue() -> RabbitQueue:
    return RabbitQueue(
        QUEUE_NEW,
        durable=True,
        routing_key=ROUTING_KEY_NEW,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
        },
    )


def dlq_queue() -> RabbitQueue:
    return RabbitQueue(QUEUE_DLQ, durable=True)


def retry_queue(name: str, ttl_ms: int) -> RabbitQueue:
    return RabbitQueue(
        name,
        durable=True,
        arguments={
            "x-message-ttl": ttl_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY_NEW,
        },
    )


async def declare_full_topology(broker: RabbitBroker, settings: AppSettings) -> None:
    exchange = await broker.declare_exchange(payments_exchange())
    dlx = await broker.declare_exchange(dlx_exchange())
    dlq = await broker.declare_queue(dlq_queue())
    await dlq.bind(dlx, routing_key=DEAD_ROUTING_KEY)
    queue_new = await broker.declare_queue(new_queue())
    await queue_new.bind(exchange, routing_key=ROUTING_KEY_NEW)
    retry_names = retry_queue_names(settings)
    for name, ttl_ms in zip(
        retry_names, settings.processing.retry_ttls_ms, strict=True
    ):
        queue_obj = await broker.declare_queue(retry_queue(name, ttl_ms))
        await queue_obj.bind(exchange, routing_key=name)
