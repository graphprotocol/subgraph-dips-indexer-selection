FROM python:3.12-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install exactly the versions in uv.lock, the ones CI tests and the audits check, into
# /app/.venv with the image's own Python. uv is mounted for this step only, and dev tools
# stay out.
ENV UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1 PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
RUN --mount=from=ghcr.io/astral-sh/uv:0.12.24@sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a,source=/uv,target=/bin/uv \
    uv sync --locked --no-install-project --no-dev --no-cache

# Copy source code
COPY src/ ./src/

# Set Python path to include src directory
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the service
CMD ["uvicorn", "iisa.iisa_http_endpoints:app", "--host", "0.0.0.0", "--port", "8080"]
