FROM python:3.12-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy package configuration and install dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir .

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
