# Build stage - install dependencies with gcc and generate protobuf code
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies (gcc for C extensions, protobuf-compiler for proto codegen)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Generate protobuf Python code from .proto schema
COPY proto/ proto/
RUN protoc -I proto --python_out=. proto/gateway_queries.proto

# Runtime stage - clean image without gcc
FROM python:3.12-slim

WORKDIR /app

# Install curl for database download
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download MaxMind GeoLite2 databases.
# Attribution: This product includes GeoLite2 data created by MaxMind, available from https://www.maxmind.com
# The license key is consumed via a BuildKit secret mount so the value never lands
# in image-layer metadata (which `docker history` would otherwise expose). Mount
# the secret with: `docker build --secret id=maxmind,env=MAXMIND_LICENSE_KEY ...`
# (env=) or `--secret id=maxmind,src=<keyfile>` (src=). Write the literal string
# "skip" to opt into degraded mode and skip GeoIP entirely.
RUN --mount=type=secret,id=maxmind,required=true \
    if [ ! -s /run/secrets/maxmind ]; then \
        echo "ERROR: maxmind secret is empty. Pass a valid MaxMind license key" && \
        echo "       (mount with --secret id=maxmind,env=VAR or src=keyfile), or" && \
        echo "       write 'skip' to opt into degraded scoring mode (no GeoIP)." && \
        exit 1; \
    elif [ "$(cat /run/secrets/maxmind)" = "skip" ]; then \
        echo "WARNING: maxmind secret is 'skip', GeoIP databases skipped (degraded scoring mode)"; \
    else \
        LICENSE_KEY=$(cat /run/secrets/maxmind) && \
        curl -fsSL "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=${LICENSE_KEY}&suffix=tar.gz" | \
        tar -xzf - --strip-components=1 -C /app --wildcards "*/GeoLite2-City.mmdb" && \
        echo "Downloaded GeoLite2-City database" && \
        curl -fsSL "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key=${LICENSE_KEY}&suffix=tar.gz" | \
        tar -xzf - --strip-components=1 -C /app --wildcards "*/GeoLite2-ASN.mmdb" && \
        echo "Downloaded GeoLite2-ASN database"; \
    fi

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code, then overlay with builder-generated protobuf
COPY *.py .
COPY --from=builder /app/gateway_queries_pb2.py .

# Run as non-root user
RUN useradd -m appuser
USER appuser

# Entry point
CMD ["python", "main.py"]
