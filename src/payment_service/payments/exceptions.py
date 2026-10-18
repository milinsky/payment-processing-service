class IdempotencyKeyConflict(Exception):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"idempotency key already used with different body: {idempotency_key}"
        )
        self.idempotency_key = idempotency_key
