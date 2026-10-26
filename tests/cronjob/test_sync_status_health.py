"""Tests for the sync-status fetcher's /health endpoint."""

import json
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import sync_status_fetcher  # noqa: E402
from sync_status_fetcher import HealthHandler  # noqa: E402


@pytest.fixture
def health_url():
    """Serve HealthHandler on a free local port and yield its /health URL."""
    server = HTTPServer(("127.0.0.1", 0), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/health"
    server.shutdown()


def _get(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def test_not_ready_before_the_first_push(health_url, monkeypatch):
    monkeypatch.setattr(sync_status_fetcher, "_last_write_time", None)

    status, body = _get(health_url)

    assert status == 503
    assert body["status"] == "not_ready"


def test_healthy_while_the_last_push_is_recent(health_url, monkeypatch):
    monkeypatch.setattr(sync_status_fetcher, "MAX_PUSH_AGE", 9000)
    monkeypatch.setattr(
        sync_status_fetcher,
        "_last_write_time",
        datetime.now(timezone.utc) - timedelta(seconds=60),
    )

    status, body = _get(health_url)

    assert status == 200
    assert body["status"] == "healthy"


def test_unhealthy_once_the_last_push_is_too_old(health_url, monkeypatch):
    """A push that worked once must not keep the pod healthy after later pushes fail."""
    monkeypatch.setattr(sync_status_fetcher, "MAX_PUSH_AGE", 9000)
    monkeypatch.setattr(
        sync_status_fetcher,
        "_last_write_time",
        datetime.now(timezone.utc) - timedelta(seconds=9001),
    )

    status, body = _get(health_url)

    assert status == 503
    assert body["status"] == "stale"
    assert body["last_write_age_seconds"] >= 9001


def test_default_allows_one_failed_cycle(monkeypatch):
    """By default the limit sits between 2 and 3 fetch intervals."""
    interval = sync_status_fetcher.FETCH_INTERVAL

    assert 2 * interval < sync_status_fetcher.MAX_PUSH_AGE < 3 * interval


def test_a_successful_cycle_records_the_push_time(monkeypatch):
    monkeypatch.setattr(sync_status_fetcher, "_last_write_time", None)
    monkeypatch.setattr(
        sync_status_fetcher,
        "discover_indexers_from_network_subgraph",
        lambda _url: {"0xAAA": "https://indexer.example.com"},
    )

    async def fake_fetch(_urls):
        return {"0xAAA": {"deployments": ["QmDeploy1"], "fetched_at": "2026-03-24T00:00:00+00:00"}}

    monkeypatch.setattr(sync_status_fetcher, "_fetch_all_statuses", fake_fetch)
    monkeypatch.setattr(sync_status_fetcher, "_push_sync_status", lambda _data: None)

    before = datetime.now(timezone.utc)
    assert sync_status_fetcher.run_fetch_cycle() is True

    assert sync_status_fetcher._last_write_time is not None
    assert sync_status_fetcher._last_write_time >= before
