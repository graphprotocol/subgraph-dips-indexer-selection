"""Sync status fetcher service.

Long-running service that periodically polls each indexer's /status
endpoint to discover which deployments they have synced and healthy.
Pushes the resulting snapshot directly to the iisa HTTP service via
iisa_client.post_sync_status; no shared filesystem is involved.

HTTP endpoints:
  GET /health  -- healthcheck (503 until first write)
"""

import asyncio
import json
import logging
import os
import signal
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    import aiohttp

from iisa_client import IISAPushError, get_push_token, post_sync_status
from processing import discover_indexers_from_network_subgraph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
FETCH_INTERVAL = int(os.environ.get("SYNC_STATUS_FETCH_INTERVAL", "3600"))
FETCH_TIMEOUT = int(os.environ.get("SYNC_STATUS_FETCH_TIMEOUT", "10"))
MAX_CONCURRENCY = int(os.environ.get("SYNC_STATUS_MAX_CONCURRENCY", "50"))
MAX_RETRIES = int(os.environ.get("SYNC_STATUS_MAX_RETRIES", "5"))
RETRY_BACKOFF_MULTIPLIER = int(os.environ.get("SYNC_STATUS_RETRY_BACKOFF_MULTIPLIER", "1"))
RETRY_BACKOFF_MAX = int(os.environ.get("SYNC_STATUS_RETRY_BACKOFF_MAX", "5"))
HTTP_PORT = int(os.environ.get("SYNC_STATUS_HTTP_PORT", "9091"))
GRAPH_NETWORK_SUBGRAPH_URL = os.environ.get("GRAPH_NETWORK_SUBGRAPH_URL", "")
IISA_API_URL = os.environ.get("IISA_API_URL", "")
# Read the bearer token once at module load so rotation requires a restart —
# consistent with RedpandaProvider's init-time read and the IISA_API_URL
# pattern above. Rotation tooling is out of scope for this service.
IISA_PUSH_TOKEN: Optional[str] = get_push_token()

STATUS_QUERY = "{ indexingStatuses { subgraph synced health } }"

# Service state
_last_write_time: Optional[str] = None
_stop_event = threading.Event()


async def _fetch_single_status(
    session: "aiohttp.ClientSession",
    indexer: str,
    url: str,
    semaphore: asyncio.Semaphore,
) -> Optional[dict]:
    """Fetch /status from a single indexer with retry logic."""
    import aiohttp

    status_url = url.rstrip("/") + "/status"
    payload = json.dumps({"query": STATUS_QUERY}).encode()

    async def do_fetch() -> list:
        async with semaphore:
            async with session.post(
                status_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            ) as resp:
                if resp.status >= 500:
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=f"Server error {resp.status}",
                    )
                resp.raise_for_status()
                data = await resp.json()
                statuses: list = data.get("data", {}).get("indexingStatuses", [])
                return statuses

    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            statuses = await do_fetch()
            synced_deployments = [
                s["subgraph"]
                for s in statuses
                if s.get("synced") is True and s.get("health") == "healthy"
            ]
            return {
                "indexer": indexer,
                "deployments": synced_deployments,
            }
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                delay = min(
                    RETRY_BACKOFF_MAX,
                    RETRY_BACKOFF_MULTIPLIER * (2**attempt),
                )
                await asyncio.sleep(delay)

    logger.warning(
        "Failed to fetch /status from %s (%s): %s",
        indexer[:10],
        url,
        last_error,
    )
    return None


async def _fetch_all_statuses(
    indexer_urls: Dict[str, str],
) -> dict[str, dict]:
    """Fetch /status from all indexers concurrently."""
    import aiohttp

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    now = datetime.now(timezone.utc).isoformat()

    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_single_status(session, indexer, url, semaphore)
            for indexer, url in indexer_urls.items()
        ]
        results = await asyncio.gather(*tasks)

    output: dict[str, dict] = {}
    for result in results:
        if result is not None and result["deployments"]:
            output[result["indexer"]] = {
                "deployments": result["deployments"],
                "fetched_at": now,
            }

    return output


def _push_sync_status(data: dict) -> None:
    """Push sync status to iisa. Raises IISAPushError on failure after retries."""
    post_sync_status(IISA_API_URL, IISA_PUSH_TOKEN, data)


def run_fetch_cycle() -> bool:
    """Run one fetch cycle. Returns True on success."""
    global _last_write_time

    indexer_urls = discover_indexers_from_network_subgraph(GRAPH_NETWORK_SUBGRAPH_URL)
    if not indexer_urls:
        logger.warning("No indexers discovered, skipping cycle")
        return False

    logger.info("Fetching /status from %d indexers...", len(indexer_urls))
    data = asyncio.run(_fetch_all_statuses(indexer_urls))

    total_deployments = sum(len(entry["deployments"]) for entry in data.values())
    logger.info(
        "Fetched sync status: %d indexers responded, %d total synced deployments",
        len(data),
        total_deployments,
    )

    try:
        _push_sync_status(data)
    except IISAPushError as e:
        logger.error("Failed to push sync status to iisa: %s", e)
        return False

    _last_write_time = datetime.now(timezone.utc).isoformat()
    logger.info("Pushed sync status for %d indexers to iisa", len(data))
    return True


# ---------------------------------------------------------------------------
# HTTP healthcheck
# ---------------------------------------------------------------------------


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            if _last_write_time is not None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {
                            "status": "healthy",
                            "last_write": _last_write_time,
                        }
                    ).encode()
                )
            else:
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "not_ready"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default request logging


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main():
    def handle_signal(signum, _frame):
        logger.info("Received signal %d, shutting down", signum)
        _stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Sync status fetcher starting: interval=%ds, timeout=%ds, concurrency=%d, retries=%d",
        FETCH_INTERVAL,
        FETCH_TIMEOUT,
        MAX_CONCURRENCY,
        MAX_RETRIES,
    )

    # Start HTTP healthcheck server
    server = HTTPServer(("0.0.0.0", HTTP_PORT), HealthHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    logger.info("Healthcheck server on port %d", HTTP_PORT)

    # Run first cycle immediately
    try:
        run_fetch_cycle()
    except Exception:
        logger.exception("First fetch cycle failed")

    # Main loop — wait for interval or shutdown signal
    while not _stop_event.is_set():
        _stop_event.wait(timeout=FETCH_INTERVAL)
        if _stop_event.is_set():
            break
        try:
            run_fetch_cycle()
        except Exception:
            logger.exception("Fetch cycle failed")

    server.shutdown()
    logger.info("Sync status fetcher stopped")


if __name__ == "__main__":
    main()
