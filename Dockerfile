# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency spec first (layer cache)
COPY pyproject.toml ./
COPY requirements.txt ./

# Install all deps into a prefix we can copy into the runtime stage
RUN pip install --prefix=/install --no-cache-dir \
    sqlalchemy pymysql "pydantic[email]" pydantic-settings python-dotenv structlog \
    prometheus-client tenacity pandas requests typer rich fastapi uvicorn \
    apscheduler alembic httpx opentelemetry-api opentelemetry-sdk


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="data-pipeline"
LABEL version="1.0.0"

# Non-root user for security
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY pipeline/ ./pipeline/
COPY cli.py main.py alembic.ini* ./
COPY sql/ ./sql/
COPY data/ ./data/

# Writable directories for dead-letter files and SQLite DB
RUN mkdir -p dead_letter && chown -R pipeline:pipeline /app

USER pipeline

# Health check — confirms Python + settings load correctly
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from pipeline.settings import settings; print('OK')" || exit 1

# Default: show help
ENTRYPOINT ["python", "cli.py"]
CMD ["--help"]
