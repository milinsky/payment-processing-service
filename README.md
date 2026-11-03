# Payment Processing Service

An asynchronous payment processing service: FastAPI + PostgreSQL + RabbitMQ (FastStream), transactional outbox, broker retries with exponential backoff (TTL queues), DLQ, idempotent payment creation. Processing uses lease-based claiming instead of row locks: no database transaction or lock is held while the payment gateway or webhooks are being called.

The whole stack runs via Docker: `make up` — no Python required on the host.

## Overview

- **api** — FastAPI: `POST /api/v1/payments` (creation, idempotency by `Idempotency-Key`), `GET /api/v1/payments/{id}` (payment details); depends only on PostgreSQL (never touches the broker, does not read `RABBIT_URL`).
- **relay** — standalone process (`python -m payment_service.relay`): background transactional-outbox loop (fetch → publish → mark), one row per short transaction.
- **consumer** — FastStream: a single `payments.new` handler (gateway emulation 90/10 with a 2–5s delay, status finalization, webhook delivery).
- **postgres** — source of truth: `payments` (including the internal `processing_claimed_until` lease column), `outbox`; Alembic migrations are applied by the api entrypoint.
- **rabbitmq** — `payments` exchange + TTL retry topology + DLQ; management UI on `:15672`.
- Guarantees: creation is atomic with the event (single TX), delivery is at-least-once, re-processing is idempotent, messages that exhaust their retries end up in `payments.dlq` with an `x-death` history.
- Code layout: one package per domain (`payments`, `processing`, `outbox`, `messaging`); all SQL lives in thin repositories (`PaymentRepository`, `OutboxRepository`) that accept an injected session, while services own transaction boundaries.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|POST /api/v1/payments| API[api: FastAPI, DB only]
    API -->|TX: payments + outbox| PG[(PostgreSQL)]
    RLY[relay: standalone process] -->|row-wise SKIP LOCKED, publish rk=new, confirms| EX[RabbitMQ exchange payments]
    PG -->|outbox rows| RLY
    EX -->|binding rk=new| Q[payments.new]
    Q --> CON[consumer: claim lease, gateway 90/10 2-5s with no open TX, finalize, webhook]
    CON -->|claim / finalize status / delivered_at in short TXs| PG
    CON -->|POST webhook| WH[client webhook receiver]
    CON -->|technical failure, attempt<3| RT[payments.retry.1/2/3 TTL 5/10/20s]
    RT -->|TTL expired, DLX rk=new| EX
    CON -->|RejectMessage at attempt>=3| DLX[payments.dlx]
    DLX --> DLQ[payments.dlq]
```

RabbitMQ topology (all entities are durable, declared idempotently at relay and consumer startup — `messaging/topology.py`):

| Entity | Type | Parameters |
|---|---|---|
| `payments` | direct exchange | relay publishes here (rk `new`) as do retry copies (rk `payments.retry.N`) |
| `payments.new` | classic queue | bind `payments` rk `new`; `x-dead-letter-exchange=payments.dlx`, `x-dead-letter-routing-key=payments.dead` |
| `payments.retry.1/2/3` | classic queues | `x-message-ttl=5000/10000/20000`, DLX → `payments` rk `new` |
| `payments.dlx` | direct exchange | — |
| `payments.dlq` | classic queue | bind `payments.dlx` rk `payments.dead`; messages carry `x-death` |

## Quick start

```bash
make up
```

Brings up 6 services: postgres, rabbitmq, api (migrations are applied automatically, then uvicorn starts; depends only on postgres), relay (waits for a healthy api — the schema is guaranteed), consumer and an auxiliary webhook-mock. Healthchecks (`/health/ready` for api) make the stack ready in ~15–30 seconds. `make ps` shows the status.

Webhook receiver events (the bundled webhook-mock helper service): `http://127.0.0.1:18080/events` — count + items, filter `?payment_id=<uuid>`, reset with `DELETE /events`.

Create a payment:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" \
  -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "demo", "webhook_url": "http://webhook-mock:8080/webhook"}'
```

Response `202 Accepted`:

```json
{"payment_id": "…uuid…", "status": "pending", "amount": "100.00", "currency": "RUB", "description": "demo", "metadata": null, "processed_at": null, "created_at": "2026-10-15T12:00:00Z"}
```

Fetch the status (after the gateway processes it, the status becomes `succeeded` or `failed`):

```bash
curl -s -H "X-API-Key: changeme" http://localhost:8000/api/v1/payments/<payment_id>
```

The webhook-mock runs as part of the stack — the notification shows up at `http://127.0.0.1:18080/events` (body: `type`, `event_id`, `payment_id`, `status`, `amount` as a string, `currency`, `occurred_at`).

## API

`POST /api/v1/payments` — headers: `X-API-Key` (required), `Idempotency-Key` (required, 1–255 chars); body: `amount` (string, `>0`, 2 decimal places, up to 12 digits), `currency` (`RUB|USD|EUR`), `description` (1–1000), `metadata` (arbitrary JSON, optional), `webhook_url` (http/https, up to 2048 chars).

| Code | Case |
|---|---|
| 202 | payment created, status `pending` |
| 200 | replay: same `Idempotency-Key` and same body; response header `Idempotency-Replayed: true`, same `payment_id` |
| 409 | same `Idempotency-Key`, different body (SHA-256 fingerprint of canonical JSON) |
| 401 | missing/invalid `X-API-Key` (`WWW-Authenticate: ApiKey` header) |
| 422 | invalid body, `Idempotency-Key`/`webhook_url` too long, or `Idempotency-Key` missing |

`GET /api/v1/payments/{payment_id}` — details: `payment_id`, `status`, `amount` (string, 2 decimal places), `currency`, `description`, `metadata`, `processed_at` (UTC, `null` until processed), `created_at`. Codes: 200, 401, 404, 422 (non-UUID).

Replay:

```bash
curl -si -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "demo", "webhook_url": "http://webhook-mock:8080/webhook"}'
```

→ `HTTP/1.1 200 OK` with `Idempotency-Replayed: true` and the same `payment_id`.

OpenAPI/Swagger: `http://localhost:8000/docs`.

## Environment variables

Configured via `.env` next to the compose file (template: `.env.example`; defaults are dev-mode values). Only docker compose reads the `.env` file (variable interpolation); the application itself (`AppSettings`) reads only container environment variables.

| Variable | Purpose | Default |
|---|---|---|
| `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME` | PostgreSQL DSN: api (DB only), relay and consumer (DB+Rabbit) | inside compose: `postgres`/5432/`payments`/`payments`/`payments`; local: `localhost:55432` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | postgres container init | `payments`/`payments`/`payments` |
| `RABBIT_URL` | AMQP DSN for relay/consumer; api does not use the broker and does not read this variable | `amqp://payments:payments@rabbitmq:5672/` |
| `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` | rabbitmq container init | `payments`/`payments` |
| `API_KEYS` | comma-separated list of valid `X-API-Key` values | `changeme` (dev value) |
| `DOCS_ENABLED` | api: expose OpenAPI/`/docs`/`/redoc` (`false` hides the docs) | `true` |
| `LOG_LEVEL` | log level for all processes (validated at startup) | `INFO` |
| `GATEWAY_SUCCESS_RATE` | gateway emulation success probability | `0.9` |
| `GATEWAY_DELAY_MIN_S` / `GATEWAY_DELAY_MAX_S` | gateway delay bounds | `2.0`/`5.0` |
| `GATEWAY_TIMEOUT_S` | upper bound for a single gateway call (`asyncio.timeout`); must be < `GATEWAY_LEASE_TTL_S` (validated at startup) | `15.0` |
| `GATEWAY_LEASE_TTL_S` | processing lease TTL; a crashed consumer's claim expires after this and redelivery re-claims the payment | `25.0` |
| `RETRY_TTLS_MS` | retry queue TTLs in ms (number of values = number of queues); the consumer logs a startup warning if their sum ≤ `GATEWAY_LEASE_TTL_S` | `5000,10000,20000` |
| `RETRY_MAX_ATTEMPTS` | retry attempts after the original delivery (total deliveries before DLQ = N+1 = 4) | `3` (must be ≤ number of TTLs; validated at startup) |
| `WEBHOOK_TIMEOUT_S` | webhook delivery timeout | `10.0` |
| `WEBHOOK_SECRET` | if set — webhooks are signed with `X-Signature` (HMAC-SHA256 of the body) | empty (no signature sent) |
| `WEBHOOK_FAIL_FIRST_N` | webhook-mock: the first N requests answer 500 (retry demo) | `0` |
| `OUTBOX_POLL_INTERVAL_S` / `OUTBOX_BATCH_SIZE` | relay: poll period and max rows per cycle | `0.5`/`100` |
| `OUTBOX_MAX_HELD_SESSIONS` | relay: stop the dispatch cycle after this many consecutive publish failures (rows stay locked until the cycle ends; the next cycle retries) | `10` |
| `OUTBOX_RETENTION_S` / `OUTBOX_CLEANUP_INTERVAL_S` | relay: how long dispatched outbox rows live and how often cleanup runs (in `OUTBOX_BATCH_SIZE` batches) | `86400`/`3600` |

## How it works

**Creation (TX#1).** `POST` validates the body, computes `request_fingerprint` = SHA-256 of canonical JSON, and inserts `payments` + `outbox` **in one transaction** (insert-first: races between parallel duplicates are prevented by the UNIQUE constraint on `idempotency_key`, not by a preliminary SELECT).

**Relay (standalone service).** The `python -m payment_service.relay` process: connect → topology declaration (before the loops start) → two background loops. Dispatch is **row-wise**: fetch the next pending row `FOR UPDATE SKIP LOCKED` → publish to exchange `payments` rk `new` (`persist=True`, publisher confirms, `message_id=event_id`) → mark `dispatched_at` → commit — one short transaction per row, up to `OUTBOX_BATCH_SIZE` rows per cycle. A row whose publish failed keeps its row lock held until the end of the cycle (`SKIP LOCKED` hands the next rows to other relay instances) and stays undispatched for the next cycle; a burst of failures stops the cycle at `OUTBOX_MAX_HELD_SESSIONS` held sessions so the connection pool is never exhausted. Retention: every `OUTBOX_CLEANUP_INTERVAL_S`, batch-delete rows with `dispatched_at` older than `OUTBOX_RETENTION_S` (the table does not grow forever). Rows are marked only after a confirmed publish; a publish failure leaves the row for the next cycle (a duplicate publish is safe — the consumer is idempotent). Thanks to `SKIP LOCKED`, multiple relay instances are safe; SIGTERM stops both loops, then `broker.stop()` → `engine.dispose()`.

**Consumer (processing pipeline).** For each message: **claim** — an atomic conditional `UPDATE payments SET processing_claimed_until = now() + lease WHERE id = … AND status = 'pending' AND (processing_claimed_until IS NULL OR processing_claimed_until < now())` in a short committed transaction (exactly one competing handler wins; the lease is computed and compared with the database `now()` only — the application clock is never used) → **gateway emulation** (90% success / 10% failure, 2–5s delay) runs with **no open session** under `asyncio.timeout(GATEWAY_TIMEOUT_S)` → **finalize** — conditional `UPDATE … SET status = <terminal>, processed_at = now(), webhook_attempts + 1 WHERE status = 'pending'`; a late finalize that finds zero rows (the lease was taken over after expiry) degrades to a duplicate-ack without a second webhook → **webhook delivery** (a single attempt, outside any transaction) → a short transaction marks `webhook_delivered_at`. If the row is `pending` with someone else's unexpired lease, the handler raises `PaymentBusyError`, which the retry wrapper treats as a technical failure — the message goes through the TTL retry queues instead of waiting on a lock. While a payment is being processed its public status stays `pending`: the lease column is internal and is never exposed through the API.

**Crash recovery.** A consumer crash at any point leaves either an expired lease (redelivery re-claims the payment once `GATEWAY_LEASE_TTL_S` passes) or a terminal status without delivery (the webhook-only redelivery path). The defaults keep the timing chain consistent: gateway timeout 15s < lease 25s < retry budget 35s (5+10+20).

**Retry.** A technical failure (DB, broker, undelivered webhook, busy payment) raises from the wrapper: with `x-retry-count < 3` a copy is published to `payments.retry.{N+1}` (body byte-for-byte, header `x-retry-count+1`, persistent), the original is acked; a failure of the copy publish itself → `nack(requeue)` — a quick retry, not DLQ. When the TTL expires, the message is dead-lettered back to `payments` rk `new` — a fresh attempt after a delay. After 3 retries — `RejectMessage` → `payments.dlq` with `x-death`. At most 4 deliveries total (original + 3); `webhook_attempts` is incremented before each attempt.

**Why three TTL queues instead of per-message expiration:** per-message TTL in a shared queue blocks head-of-line — a message with a large TTL at the head delays all shorter ones; a dedicated queue per retry level gives independent flows and a visible counter in the management UI. The alternatives (quorum + delivery-limit — no delay; the delayed-message plugin — requires a custom image) were rejected.

**Business failure ≠ technical failure.** A gateway rejection (10%) is a normal outcome: status `failed`, webhook `payment.failed`, ack, DLQ untouched. Exceptions are raised only for technical failures (see Retry).

## Idempotency

| Layer | Mechanism |
|---|---|
| Creation | UNIQUE(`idempotency_key`) + insert-first; same key + same body → 200 + `Idempotency-Replayed: true`; different body → 409 |
| Consumer | status gate: terminal status + webhook delivered → ack without re-running the gateway; terminal without delivery → webhook-only resend; `pending` with a foreign live lease → busy → retry |
| At-least-once delivery | relay confirms + retry circuit; duplicate publishes/deliveries are possible |
| Webhook receiver | **must deduplicate by `payment_id`/`event_id`** — with at-least-once a duplicate webhook is possible: a crash between delivery and the `webhook_delivered_at` commit, as well as concurrent delivery of two copies of the same event before delivery is recorded (with multiple consumer/relay instances) |

## Demo scenarios

### 1. DLQ live (management UI)

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" -H "Idempotency-Key: dlq-demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "dlq demo", "webhook_url": "http://127.0.0.1:9/hook"}'
```

A host with a closed port → the payment status becomes `succeeded` (the gateway succeeded), but delivery keeps failing: 3 deliveries through `payments.retry.1/2/3` (TTL 5/10/20s — the path to DLQ takes ≈35–40s; to speed it up, start the consumer with `RETRY_TTLS_MS=1000,1000,1000`). Open `http://localhost:15672` (login/password `payments`/`payments`): the `payments.retry.*` queues show message spikes, then a message appears in `payments.dlq` (Get Messages shows the `x-death` header and `x-retry-count: 3`). A repeat GET shows `succeeded`; `webhook_attempts` = 4 (visible in the DB: `docker compose exec postgres psql -U payments -d payments -c "select status, webhook_attempts, webhook_delivered_at from payments where id='<uuid>'"` — `delivered_at` is empty, the status is terminal, the message can be replayed manually from the UI).

### 2. Broker outage resilience (outbox accumulates)

```bash
docker compose stop rabbitmq
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" -H "Idempotency-Key: outage-demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "50.00", "currency": "RUB", "description": "outage demo", "webhook_url": "http://webhook-mock:8080/webhook"}'
docker compose start rabbitmq
```

While the broker is down, the relay does not mark rows; once it starts (reconnect ~5s), the event is published and the payment completes — GET shows a terminal status.

### 3. Guaranteed business failure

```bash
make consumer-at E2E_RATE=0.0
```

Restarts the consumer and relay with a consistent env (RATE=0.0, zero gateway delays; the TTL topology is unchanged — no PRECONDITION conflicts). A new payment → status `failed`, webhook of type `payment.failed`, `payments.dlq` stays empty. To revert: `make consumer-at` (RATE=1.0); a full restore of the production gateway defaults 90/10 — `make down && make up`.

### 4. Replay

Repeat the POST from Quick start with the same `Idempotency-Key: demo-1` and the same body → `200` + `Idempotency-Replayed: true`, same `payment_id`. Change `amount` → `409`.

## Guarantees → mechanisms

| Guarantee | Mechanism |
|---|---|
| 202 ⇔ both rows exist | single TX: `payments` + `outbox` |
| At-least-once delivery | relay publish confirms + broker TTL retries |
| No DB locks during external calls | lease-based claiming: the gateway call and webhook delivery run with no open transaction; claim/finalize/delivery-mark are separate short transactions |
| No duplicate gateway emulation | atomic conditional claim + terminal-status gate (+ finalize guarded by `WHERE status='pending'`) |
| No second webhook after success | `webhook_delivered_at` in the gate |
| Lost messages are observable | `payments.dlq` + `x-death`/`x-retry-count` headers |
| Duplicate requests have no effect | UNIQUE constraint + body fingerprint |
| Outbox does not grow forever | retention of `dispatched_at` older than `OUTBOX_RETENTION_S` |

## Observability

Application logs in all processes are JSON (structlog, UTC); uvicorn's own output (access log) and FastStream's (Received/Processed) remain in the standard text format. Correlation: the api binds a `request_id` (from the `X-Request-ID` header or generated) to the request context and returns it in the response — `payment_created`/`payment_replayed` events carry `request_id`; consumer/relay bind an `event_id`/`payment_id` context per message/batch.

## Development

All tooling runs inside docker (no host Python needed):

| Command | Action |
|---|---|
| `make up` / `make down` / `make restart` / `make build` | stack lifecycle (6 services: postgres, rabbitmq, api, relay, consumer, webhook-mock) |
| `make ps` / `make logs` / `make logs-f` | status / logs (tail 100) / follow |
| `make test` | unit+integration inside docker + coverage report (gate ≥85%, actual ~99%); stops api relay consumer for the run (does not bring them back — after the run do `docker compose start api relay consumer` or `make up`) |
| `make e2e` | e2e scenarios against the live stack: rebuilds the api/relay/consumer/webhook-mock images, runs the scenarios in two phases (RATE=1.0 and RATE=0.0); afterwards the stack is recreated and started automatically (`down -v` → `up -d --wait` — the default 6 services are back up) |
| `make e2e-failed` | only the failed phase (consumer with `GATEWAY_SUCCESS_RATE=0.0`) |
| `make consumer-at` | restart the consumer and relay with the invocation's env variables (GATEWAY_*, TTLs unchanged); `consumer-at-env E2E_TTLS=1` — variant with test TTLs (only on fresh volumes — after `make down -v`/`make e2e`; on a live default stack it causes a TTL PRECONDITION conflict) |
| `make lint` / `make fmt` / `make typecheck` | ruff check+format / formatting / mypy strict (src and tests) — in the tests image |
| `make migrate` | `alembic upgrade head` in the api container |
| `make pause-live` | stop api, relay and consumer (postgres/rabbitmq stay up) |

Debugging from the host: PostgreSQL `localhost:55432` (`payments`/`payments`/`payments`), RabbitMQ management `http://localhost:15672`.

## Deliberately not done

- SSRF filtering for webhook URLs (private ranges, DNS rebinding), egress policies.
- API rate limiting, metrics, tracing, TLS termination.
- Auth mechanisms beyond the static key (JWT/OAuth), key rotation without a restart.
- Alembic offline mode (`alembic upgrade --sql`) — env.py has no offline branch; migrations run only against a live database.
- Automatic replay from the DLQ (manual via the management UI only).
- Multiple relay/consumer instances are supported: relay — row-wise `FOR UPDATE SKIP LOCKED`; consumer — atomic lease claiming: a concurrent duplicate gets busy and flows through TTL retries instead of waiting; a duplicate webhook is still possible — the at-least-once contract applies. Compose runs one instance of each (the spec describes a single consumer; scaling is an orchestration concern).
