from faststream.rabbit import RabbitBroker

GRACEFUL_TIMEOUT_S = 15.0
RECONNECT_INTERVAL_S = 5.0


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        graceful_timeout=GRACEFUL_TIMEOUT_S,
        reconnect_interval=RECONNECT_INTERVAL_S,
    )
