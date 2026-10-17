# Payment Processing Service

[![Quality gate status](https://sonarcloud.io/api/project_badges/measure?project=milinsky_payment-processing-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=milinsky_payment-processing-service)

Асинхронный сервис процессинга платежей: FastAPI + PostgreSQL + RabbitMQ (FastStream), transactional outbox, брокерные ретраи с экспоненциальной задержкой (TTL-очереди), DLQ, идемпотентное создание платежей.

Стек поднимается целиком через Docker: `make up` — Python на машине не требуется.

## Обзор

- **api** — FastAPI: `POST /api/v1/payments` (создание, идемпотентность по `Idempotency-Key`), `GET /api/v1/payments/{id}` (детальная информация); зависит только от PostgreSQL (брокера не касается и `RABBIT_URL` не читает).
- **relay** — отдельный процесс (`python -m payment_service.relay`): фоновый цикл транзакционного outbox (poll → publish → mark).
- **consumer** — FastStream: один обработчик `payments.new` (эмуляция шлюза 90/10 с задержкой 2–5с, фиксация статуса, доставка webhook).
- **postgres** — источник истины: `payments`, `outbox`; миграции Alembic применяются entrypoint'ом api.
- **rabbitmq** — exchange `payments` + TTL-retry-топология + DLQ; management UI на `:15672`.
- Гарантии: создание атомарно с событием (одна TX), доставка at-least-once, повторная обработка идемпотентна, исчерпавшие ретраи сообщения — в `payments.dlq` с историей `x-death`.

## Архитектура

```mermaid
flowchart LR
    C[Client] -->|POST /api/v1/payments| API[api: FastAPI, только БД]
    API -->|TX: payments + outbox| PG[(PostgreSQL)]
    RLY[relay: отдельный процесс] -->|poll SKIP LOCKED, publish rk=new, confirms| EX[RabbitMQ exchange payments]
    PG -->|outbox строки| RLY
    EX -->|binding rk=new| Q[payments.new]
    Q --> CON[consumer: шлюз 90/10 2-5s, статус, webhook]
    CON -->|update status / delivered_at| PG
    CON -->|POST webhook| WH[webhook-получатель клиента]
    CON -->|техсбой, attempt<3| RT[payments.retry.1/2/3 TTL 5/10/20s]
    RT -->|TTL истёк, DLX rk=new| EX
    CON -->|RejectMessage при attempt>=3| DLX[payments.dlx]
    DLX --> DLQ[payments.dlq]
```

Топология RabbitMQ (все сущности durable, объявляются идемпотентно на старте relay и consumer — `messaging/topology.py`):

| Сущность | Тип | Параметры |
|---|---|---|
| `payments` | direct exchange | публикует relay (rk `new`) и retry-копии (rk `payments.retry.N`) |
| `payments.new` | classic queue | bind `payments` rk `new`; `x-dead-letter-exchange=payments.dlx`, `x-dead-letter-routing-key=payments.dead` |
| `payments.retry.1/2/3` | classic queue | `x-message-ttl=5000/10000/20000`, DLX → `payments` rk `new` |
| `payments.dlx` | direct exchange | — |
| `payments.dlq` | classic queue | bind `payments.dlx` rk `payments.dead`; сообщения несут `x-death` |

## Быстрый старт

```bash
make up
```

Поднимает 6 сервисов: postgres, rabbitmq, api (миграции применяются автоматически, затем uvicorn; зависит только от postgres), relay (ждёт healthy api — схема гарантирована), consumer и вспомогательный webhook-mock. healthchecks (`/health/ready` для api) дают готовность за ~15–30 секунд. `make ps` показывает статус.

События webhook-получателя (вспомогательный сервис webhook-mock из коробки): `http://127.0.0.1:18080/events` — count + items, фильтр `?payment_id=<uuid>`, сброс `DELETE /events`.

Создание платежа:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" \
  -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "demo", "webhook_url": "http://webhook-mock:8080/webhook"}'
```

Ответ `202 Accepted`:

```json
{"payment_id": "…uuid…", "status": "pending", "amount": "100.00", "currency": "RUB", "description": "demo", "metadata": null, "processed_at": null, "created_at": "2026-10-15T12:00:00Z"}
```

Получение статуса (после обработки шлюзом статус становится `succeeded` или `failed`):

```bash
curl -s -H "X-API-Key: changeme" http://localhost:8000/api/v1/payments/<payment_id>
```

Webhook-mock поднят вместе со стеком — уведомление появится в `http://127.0.0.1:18080/events` (тело: `type`, `event_id`, `payment_id`, `status`, `amount` строкой, `currency`, `occurred_at`).

## API

`POST /api/v1/payments` — заголовки `X-API-Key` (обязателен), `Idempotency-Key` (обязателен, 1–255 символов); тело: `amount` (строка, `>0`, 2 знака, до 12 цифр), `currency` (`RUB|USD|EUR`), `description` (1–1000), `metadata` (произвольный JSON, опционально), `webhook_url` (http/https, до 2048 символов).

| Код | Случай |
|---|---|
| 202 | платёж создан, статус `pending` |
| 200 | реплей: тот же `Idempotency-Key` и то же тело; заголовок ответа `Idempotency-Replayed: true`, тот же `payment_id` |
| 409 | тот же `Idempotency-Key`, другое тело (fingerprint SHA-256 канонического JSON) |
| 401 | нет/неверен `X-API-Key` (заголовок `WWW-Authenticate: ApiKey`) |
| 422 | невалидное тело, слишком длинный `Idempotency-Key`/`webhook_url` либо отсутствует `Idempotency-Key` |

`GET /api/v1/payments/{payment_id}` — детальная информация: `payment_id`, `status`, `amount` (строкой, 2 знака), `currency`, `description`, `metadata`, `processed_at` (UTC, `null` до обработки), `created_at`. Коды: 200, 401, 404, 422 (не-UUID).

Реплей:

```bash
curl -si -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "demo", "webhook_url": "http://webhook-mock:8080/webhook"}'
```

→ `HTTP/1.1 200 OK` c `Idempotency-Replayed: true` и тем же `payment_id`.

OpenAPI/Swagger: `http://localhost:8000/docs`.

## Переменные окружения

Задаются через `.env` рядом с compose (шаблон — `.env.example`; значения по умолчанию — dev-режим). Файл `.env` читает только docker compose (интерполяция переменных); само приложение (`AppSettings`) читает исключительно переменные окружения контейнера.

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME` | DSN PostgreSQL: api (только DB), relay и consumer (DB+Rabbit) | внутри compose: `postgres`/5432/`payments`/`payments`/`payments`; локально: `localhost:55432` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | инициализация контейнера postgres | `payments`/`payments`/`payments` |
| `RABBIT_URL` | AMQP DSN для relay/consumer; api брокер не использует и переменную не читает | `amqp://payments:payments@rabbitmq:5672/` |
| `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` | инициализация контейнера rabbitmq | `payments`/`payments` |
| `API_KEYS` | список допустимых `X-API-Key` через запятую | `changeme` (dev-значение) |
| `DOCS_ENABLED` | api: выставлять OpenAPI/`/docs`/`/redoc` (`false` прячет документацию) | `true` |
| `LOG_LEVEL` | уровень логирования всех процессов (валидируется при старте) | `INFO` |
| `GATEWAY_SUCCESS_RATE` | вероятность успеха эмуляции шлюза | `0.9` |
| `GATEWAY_DELAY_MIN_S` / `GATEWAY_DELAY_MAX_S` | границы задержки шлюза | `2.0`/`5.0` |
| `RETRY_TTLS_MS` | TTL retry-очередей, мс (число значений = число очередей) | `5000,10000,20000` |
| `RETRY_MAX_ATTEMPTS` | число retry-попыток после исходной доставки (всего доставок до DLQ = N+1 = 4) | `3` (обязателен ≤ числа TTL; валидация при старте) |
| `WEBHOOK_TIMEOUT_S` | таймаут доставки webhook | `10.0` |
| `WEBHOOK_SECRET` | если задан — webhook подписывается `X-Signature` (HMAC-SHA256 от тела) | пусто (подпись не отправляется) |
| `WEBHOOK_FAIL_FIRST_N` | webhook-mock: N первых запросов отвечают 500 (демо retry) | `0` |
| `OUTBOX_POLL_INTERVAL_S` / `OUTBOX_BATCH_SIZE` | relay: период опроса и размер батча | `0.5`/`100` |
| `OUTBOX_RETENTION_S` / `OUTBOX_CLEANUP_INTERVAL_S` | relay: сколько жить отправленным строкам outbox и как часто запускать очистку (батчами по `OUTBOX_BATCH_SIZE`) | `86400`/`3600` |

## Как это работает

**Создание (TX#1).** `POST` валидирует тело, вычисляет `request_fingerprint` = SHA-256 канонического JSON, вставляет `payments` + `outbox` **в одной транзакции** (INSERT-first: гонки параллельных дублей исключает UNIQUE-констрейнт по `idempotency_key`, а не предварительный SELECT).

**Relay (отдельный сервис).** Процесс `python -m payment_service.relay`: connect → объявление топологии (до старта цикла) → два фоновых цикла: диспетчеризация — `SELECT … WHERE dispatched_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED` батчем → publish в exchange `payments` rk `new` (`persist=True`, publisher confirms, `message_id=event_id`) → пометка `dispatched_at` в той же транзакции; ретенция — каждые `OUTBOX_CLEANUP_INTERVAL_S` батч-удаление строк с `dispatched_at` старше `OUTBOX_RETENTION_S` (таблица не растёт бесконечно). Пометка — только после подтверждённой публикации; сбой публикации оставляет строку для повторного цикла (дубль публикации безопасен — consumer идемпотентен). Благодаря `SKIP LOCKED` безопасно запускать несколько инстансов relay; SIGTERM гасит оба цикла, затем `broker.stop()` → `engine.dispose()`.

**Consumer (порядок операций).** Для каждого сообщения: перечитать платёж из БД по `payment_id` **с блокировкой строки** (`SELECT … FOR UPDATE` — БД источник истины, не payload; параллельная обработка дублей сериализуется, шлюз не выполняется дважды) → идемпотентный гейт: терминальный статус **и** `webhook_delivered_at` заполнен → просто ack → эмуляция шлюза (90% успех / 10% отказ, задержка 2–5с) → commit статуса с `processed_at = now()` из БД (единый источник времени) и инкремент `webhook_attempts` (TX#3, до отправки) → отправка webhook (ровно одна попытка, вне транзакции) → commit `webhook_delivered_at` (TX#4) → ack.

**Retry.** Технический сбой (БД, брокер, недоставленный webhook) — исключение обёртки: при `x-retry-count < 3` публикуется копия в `payments.retry.{N+1}` (тело байт-в-байт, заголовок `x-retry-count+1`, persist), оригинал ack; сбой самой публикации копии → `nack(requeue)` — быстрый повтор, не DLQ. TTL истёк → dead-letter обратно в `payments` rk `new` — новая попытка с задержкой. После 3 ретраев — `RejectMessage` → `payments.dlq` с `x-death`. Итого максимум 4 доставки (исходная + 3), `webhook_attempts` инкрементируется до каждой попытки.

**Почему три TTL-очереди, а не per-message expiration:** per-message TTL в общей очереди блокирует head-of-line — сообщение с большим TTL в голове задерживает все младшие; отдельная очередь на уровень ретрая даёт независимые потоки и видимость счётчика в management UI. Альтернативы (quorum + delivery-limit — без задержки; плагин delayed-message — требует кастомизации образа) отвергнуты.

**Бизнес-отказ ≠ техсбой.** Отказ шлюза (10%) — штатный исход: статус `failed`, webhook `payment.failed`, ack, DLQ не используется. Исключения — только технические сбои (см. retry).

## Идемпотентность

| Уровень | Механизм |
|---|---|
| Создание | UNIQUE(`idempotency_key`) + INSERT-first; тот же ключ + то же тело → 200 + `Idempotency-Replayed: true`; другое тело → 409 |
| Consumer | статус-гейт: терминальный статус + доставленный webhook → ack без повторной эмуляции; терминальный без доставки → доотправка только webhook |
| Доставка ≥1 раз | relay confirms + retry-контур; дубль публикации/доставки возможен |
| Получатель webhook | **обязан дедуплицировать по `payment_id`/`event_id`** — при at-least-once дубль webhook возможен: крэш между отправкой и commit `webhook_delivered_at`, а также конкурентная доставка двух копий одного события до фиксации доставки (при нескольких инстансах consumer/relay) |

## Демо-сценарии

### 1. DLQ вживую (management UI)

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" -H "Idempotency-Key: dlq-demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "dlq demo", "webhook_url": "http://127.0.0.1:9/hook"}'
```

Вебхост с закрытым портом → статус платежа станет `succeeded` (шлюз отработал), но доставка будет падать: 3 доставки из `payments.retry.1/2/3` (TTL 5/10/20с — путь до DLQ ≈ 35–40с; для ускорения поднимите consumer с `RETRY_TTLS_MS=1000,1000,1000`). Откройте `http://localhost:15672` (логин/пароль `payments`/`payments`): очереди `payments.retry.*` покажут всплески сообщений, затем в `payments.dlq` появится сообщение (Get Messages — виден заголовок `x-death` и `x-retry-count: 3`). Повторный GET платежа покажет `succeeded`; `webhook_attempts` = 4 (виден в БД: `docker compose exec postgres psql -U payments -d payments -c "select status, webhook_attempts, webhook_delivered_at from payments where id='<uuid>'"` — `delivered_at` пуст, статус терминальный, сообщение можно replay-нуть вручную из UI).

### 2. Отказоустойчивость брокера (outbox копит)

```bash
docker compose stop rabbitmq
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: changeme" -H "Idempotency-Key: outage-demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "50.00", "currency": "RUB", "description": "outage demo", "webhook_url": "http://webhook-mock:8080/webhook"}'
docker compose start rabbitmq
```

Пока брокер недоступен, relay не помечает строки; после старта (reconnect ~5с) событие доиздаётся и платёж завершается — GET показывает терминальный статус.

### 3. Гарантированный бизнес-отказ

```bash
make consumer-at E2E_RATE=0.0
```

Перезапускает consumer и relay с согласованным env (RATE=0.0, нулевые задержки шлюза; TTL-топология не меняется — PRECONDITION-конфликтов нет). Новый платёж → статус `failed`, webhook типа `payment.failed`, `payments.dlq` пуста. Вернуть: `make consumer-at` (RATE=1.0); полное восстановление прод-дефолтов шлюза 90/10 — `make down && make up`.

### 4. Реплей

Повторите POST из Quickstart с тем же `Idempotency-Key: demo-1` и тем же телом → `200` + `Idempotency-Replayed: true`, тот же `payment_id`. Измените `amount` → `409`.

## Гарантии → механизмы

| Гарантия | Механизм |
|---|---|
| 202 ⇔ существуют обе строки | одна TX: `payments` + `outbox` |
| Доставка ≥1 раз | relay publish confirms + broker TTL-retry |
| Нет повторной эмуляции шлюза | статус-гейт consumer'а + `FOR UPDATE` на строке платежа (в т.ч. при параллельной доставке) |
| Нет второго webhook после успеха | `webhook_delivered_at` в гейте |
| Потерянные сообщения наблюдаемы | `payments.dlq` + заголовки `x-death`/`x-retry-count` |
| Дубликаты запросов без эффекта | UNIQUE-констрейнт + fingerprint тела |
| Outbox не растёт бесконечно | ретенция `dispatched_at` старше `OUTBOX_RETENTION_S` |

## Наблюдаемость

Логи приложения во всех процессах — JSON (structlog, UTC); служебный вывод uvicorn (access-лог) и FastStream (Received/Processed) остаётся стандартным текстовым форматом. Корреляция: api привязывает `request_id` (из заголовка `X-Request-ID` или генерирует) к контексту запроса и возвращает его в ответе — события `payment_created`/`payment_replayed` несут `request_id`; consumer/relay — контекст `event_id`/`payment_id` каждого сообщения/батча.

## Разработка

Все инструменты — в docker (Python на хосте не нужен):

| Команда | Действие |
|---|---|
| `make up` / `make down` / `make restart` / `make build` | жизненный цикл стека (6 сервисов: postgres, rabbitmq, api, relay, consumer, webhook-mock) |
| `make ps` / `make logs` / `make logs-f` | статус / логи (tail 100) / follow |
| `make test` | unit+integration внутри docker + отчёт покрытия (гейт ≥85%, фактическое ~99%); останавливает api relay consumer на время прогона (обратно не поднимает — после прогона выполните `docker compose start api relay consumer` или `make up`) |
| `make e2e` | e2e-сценарии против живого стека: пересобирает образы api/relay/consumer/webhook-mock, двухфазно (RATE=1.0 и RATE=0.0) прогоняет сценарии; по завершении стек пересоздаётся и поднимается автоматически (`down -v` → `up -d --wait` — дефолтные 6 сервисов снова в строю) |
| `make e2e-failed` | только failed-фаза (consumer с `GATEWAY_SUCCESS_RATE=0.0`) |
| `make consumer-at` | перезапуск consumer и relay с env-переменными вызова (GATEWAY_*, без смены TTL); `consumer-at-env E2E_TTLS=1` — вариант с тестовыми TTL (только на свежих volumes — после `make down -v`/`make e2e`; на живом дефолт-стеке вызовет PRECONDITION-конфликт TTL) |
| `make lint` / `make fmt` / `make typecheck` | ruff check+format / форматирование / mypy strict (src и tests) — в tests-образе |
| `make migrate` | `alembic upgrade head` в контейнере api |
| `make pause-live` | остановить api, relay и consumer (postgres/rabbitmq остаются) |

Отладка с хоста: PostgreSQL `localhost:55432` (`payments`/`payments`/`payments`), RabbitMQ management `http://localhost:15672`.

## Что сознательно не сделано

- Фильтрация SSRF-поверхности webhook (приватные диапазоны, DNS-rebinding), egress-политики.
- Rate-limiting API, метрики, tracing, TLS-терминация
- Auth-механизмы сложнее статического ключа (JWT/OAuth), ротация ключей без рестарта.
- Offline-режим Alembic (`alembic upgrade --sql`) — env.py не содержит offline-ветки, миграции выполняются только на живой БД.
- Автоматический replay из DLQ (только ручной через management UI).
- Несколько инстансов relay/consumer поддерживаются: relay — `FOR UPDATE SKIP LOCKED`, consumer — блокировка строки платежа на время обработки (второй конкурент просто ждёт и уходит в статус-гейт; дубль webhook при этом всё же возможен — действует контракт at-least-once). В compose по одному экземпляру (ТЗ описывает одного consumer, масштабирование — вопрос оркестрации).
