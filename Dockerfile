FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /usr/local/bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ ./src/
RUN uv sync --locked --no-dev --no-editable

FROM builder AS tests
ENV PATH="/app/.venv/bin:$PATH"
RUN uv sync --locked --no-editable
COPY tests/ /app/tests/
COPY scripts/ /app/scripts/
COPY alembic.ini ./
COPY alembic/ /app/alembic/

FROM python:3.14-slim
RUN useradd --create-home appuser
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY scripts/ ./scripts/
COPY alembic.ini ./
COPY alembic/ /app/alembic/
ENV PATH="/app/.venv/bin:$PATH"
USER appuser
CMD ["python", "-c", "print('stub')"]
