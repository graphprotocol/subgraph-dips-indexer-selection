# Display available commands and their descriptions (default target)
default:
    @just --list

# Build the service image locally, tagging it
# `ghcr.io/graphprotocol/subgraph-dips-indexer-selection:${IISA_TAG:-local}`.
# The default `:local` tag is what local-network consumes when pointed at a
# locally-built image; override with `IISA_TAG=foo just build-image`.
build-image:
    docker compose build
