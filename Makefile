COMPOSE := docker compose
TEST_TARGET := api-test
PYTEST_MARKERS := unit or integration

.PHONY: up down restart build logs logs-f ps test e2e e2e-failed consumer-at lint fmt typecheck migrate pause-live

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs --tail=100

logs-f:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

pause-live:
	$(COMPOSE) stop api relay consumer

test: pause-live
	$(COMPOSE) --profile test up -d --wait postgres rabbitmq
	$(COMPOSE) --profile test build $(TEST_TARGET)
	$(COMPOSE) --profile test run --rm $(TEST_TARGET) pytest -m "$(PYTEST_MARKERS)" --cov=payment_service --cov-report=term-missing

E2E_API_BASE := http://api:8000
E2E_EVENTS_URL := http://webhook-mock:8080/events
E2E_MGMT_URL := http://rabbitmq:15672
E2E_CONSUMER_ENV = GATEWAY_SUCCESS_RATE=$(E2E_RATE) GATEWAY_DELAY_MIN_S=0 GATEWAY_DELAY_MAX_S=0 WEBHOOK_TIMEOUT_S=2
E2E_TEST_TTL_ENV = RETRY_TTLS_MS=1000,1000,1000 RETRY_MAX_ATTEMPTS=3
E2E_RATE ?= 1.0
E2E_RUN = $(COMPOSE) --profile test run --rm -e E2E_API_URL=$(E2E_API_BASE) -e E2E_WEBHOOK_EVENTS_URL=$(E2E_EVENTS_URL) -e E2E_RABBIT_MGMT_URL=$(E2E_MGMT_URL) $(TEST_TARGET)

consumer-at-env:
	$(COMPOSE) stop consumer relay
	$(E2E_CONSUMER_ENV) $(if $(E2E_TTLS),$(E2E_TEST_TTL_ENV),) $(COMPOSE) up -d consumer relay

consumer-at:
	$(COMPOSE) stop consumer relay
	$(E2E_CONSUMER_ENV) $(COMPOSE) up -d consumer relay

e2e:
	$(COMPOSE) --profile test down -v --remove-orphans
	$(COMPOSE) up -d --wait postgres rabbitmq
	$(COMPOSE) --profile test build $(TEST_TARGET)
	$(COMPOSE) build api relay consumer webhook-mock
	$(E2E_CONSUMER_ENV) $(E2E_TEST_TTL_ENV) $(COMPOSE) up -d --wait api relay consumer webhook-mock
	$(E2E_RUN) sh -c 'pytest -m e2e -k "not failed_flow" || test $$? -eq 5'
	$(MAKE) consumer-at-env E2E_RATE=0.0 E2E_TTLS=1
	$(E2E_RUN) sh -c 'pytest -m e2e -k failed_flow || test $$? -eq 5'
	$(COMPOSE) --profile test down -v --remove-orphans
	$(COMPOSE) up -d --wait

e2e-failed:
	$(COMPOSE) --profile test down -v --remove-orphans
	$(COMPOSE) up -d --wait postgres rabbitmq
	$(COMPOSE) build api relay consumer webhook-mock
	$(E2E_CONSUMER_ENV) $(E2E_TEST_TTL_ENV) $(COMPOSE) up -d --wait api relay webhook-mock
	$(MAKE) consumer-at-env E2E_RATE=0.0 E2E_TTLS=1
	$(E2E_RUN) sh -c 'pytest -m e2e -k failed_flow || test $$? -eq 5'
	$(COMPOSE) --profile test down -v --remove-orphans
	$(COMPOSE) up -d --wait

lint: pause-live
	$(COMPOSE) --profile test run --rm -v $(CURDIR):/app -v /app/.venv $(TEST_TARGET) ruff check .
	$(COMPOSE) --profile test run --rm -v $(CURDIR):/app -v /app/.venv $(TEST_TARGET) ruff format --check .

fmt:
	$(COMPOSE) --profile test run --rm --user root -v $(CURDIR):/app -v /app/.venv $(TEST_TARGET) ruff format .
	$(COMPOSE) --profile test run --rm --user root -v $(CURDIR):/app -v /app/.venv $(TEST_TARGET) ruff check --fix .

typecheck: pause-live
	$(COMPOSE) --profile test run --rm -v $(CURDIR):/app -v /app/.venv $(TEST_TARGET) mypy

migrate:
	$(COMPOSE) up -d --wait api
	$(COMPOSE) exec api alembic upgrade head
