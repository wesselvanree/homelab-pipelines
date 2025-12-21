# Install uv
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim as builder

# Change the working directory to the `app` directory
WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable --no-dev


FROM python:3.12-slim

# Copy the environment, but not the source code
COPY --from=builder --chown=app:app /app/.venv /app/.venv

# Copy your Dagster project. You may need to replace the filepath depending on your project structure
COPY src ./app/src
COPY dagster.yaml ./app/dagster.yaml
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app/

# Expose the port that your Dagster instance will run on
EXPOSE 80

# Start Dagster gRPC server
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "80", "-m", "src"]
