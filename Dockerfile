FROM python:3.12-slim AS base

FROM base AS deps
COPY --from=ghcr.io/astral-sh/uv:0.9.21 /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --no-editable --no-dev


FROM base AS runner
COPY --from=deps --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY dagster.yaml ./app/dagster.yaml
COPY src ./app/src

WORKDIR /app/

EXPOSE 80
