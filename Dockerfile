# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build tools needed for native extensions (e.g. grpcio, pyarrow)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip + setuptools first (required for PEP 660 editable / build backends)
RUN pip install --upgrade pip "setuptools>=64" wheel

# Copy only the files pip needs to resolve dependencies (better layer caching)
COPY pyproject.toml ./
COPY pipeline/ ./pipeline/
COPY cli.py ./

# Install the package and ALL its declared dependencies into /install
# This reads pyproject.toml so it never gets out of sync with the dep list
RUN pip install --prefix=/install --no-cache-dir .


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="data-pipeline"
LABEL version="1.0.0"

# Non-root user for security
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline

WORKDIR /app

# Copy installed packages (includes pipeline package + all deps) from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY pipeline/ ./pipeline/
COPY cli.py main.py ./
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
