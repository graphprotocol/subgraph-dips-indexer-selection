"""Tests for how the sync-status fetcher runs the freshness prober: not at all unless switched
on, and when on, started with the fetcher and reported in /health without changing its code."""

import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

JOBS_PATH = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(JOBS_PATH))

import freshness_prober  # noqa: E402
import sync_status_fetcher  # noqa: E402

ENABLED = {
    "FRESHNESS_PROBE_ENABLED": "true",
    "FRESHNESS_GATEWAY_URL": "https://gateway.example.com/api",
    "FRESHNESS_GATEWAY_API_KEY": "fake-freshness-key-0000",
}


class _FakeProber:
    def health(self, now):
        return {"running": True, "last_push": "2026-11-10T22:32:30+00:00"}


@pytest.fixture
def one_cycle(monkeypatch):
    """Run main() for exactly 1 sync-status cycle, on a free port, without touching signals."""
    stop = threading.Event()
    cycles = []

    def fake_cycle():
        cycles.append(1)
        stop.set()
        return True

    monkeypatch.setattr(sync_status_fetcher, "_stop_event", stop)
    monkeypatch.setattr(sync_status_fetcher, "_freshness_prober", None)
    monkeypatch.setattr(sync_status_fetcher, "run_fetch_cycle", fake_cycle)
    monkeypatch.setattr(sync_status_fetcher, "HTTP_PORT", 0)
    monkeypatch.setattr(sync_status_fetcher.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(
        sync_status_fetcher, "GRAPH_NETWORK_SUBGRAPH_URL", "https://network.example.com/sg"
    )
    for name in ENABLED:
        monkeypatch.delenv(name, raising=False)
    return cycles


def test_with_the_prober_off_the_fetcher_runs_as_before(one_cycle, monkeypatch):
    monkeypatch.setattr(
        sync_status_fetcher,
        "_start_freshness_prober",
        lambda _config: pytest.fail("the prober must not start when switched off"),
    )

    assert sync_status_fetcher.main() == 0

    assert one_cycle == [1]
    assert sync_status_fetcher._freshness_prober is None
    _, body = sync_status_fetcher._health(datetime.now(timezone.utc))
    assert "freshness" not in body


def test_with_the_prober_off_the_fetcher_loads_none_of_its_modules():
    script = (
        "import sys; "
        f"sys.path.insert(0, {str(JOBS_PATH)!r}); "
        "import sync_status_fetcher; "
        "print(','.join(m for m in ('freshness_prober', 'freshness_gateway', "
        "'freshness_sources') if m in sys.modules))"
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == ""


def test_with_the_prober_on_the_fetcher_starts_it_with_its_stop_event(one_cycle, monkeypatch):
    for name, value in ENABLED.items():
        monkeypatch.setenv(name, value)
    started = {}

    def fake_start(config, stop, **kwargs):
        started.update(config=config, stop=stop, **kwargs)
        thread = threading.Thread(target=stop.wait)
        thread.start()
        return _FakeProber(), thread

    monkeypatch.setattr(freshness_prober, "start_freshness_prober", fake_start)

    assert sync_status_fetcher.main() == 0

    assert started["stop"] is sync_status_fetcher._stop_event
    assert started["config"].gateway_url == "https://gateway.example.com/api"
    assert started["subgraph_url"] == "https://network.example.com/sg"
    assert isinstance(sync_status_fetcher._freshness_prober, _FakeProber)


@pytest.mark.parametrize("pushed_ago", [None, 60, 100_000])
def test_health_reports_the_prober_without_changing_the_status_code(monkeypatch, pushed_ago):
    now = datetime.now(timezone.utc)
    last_write = None if pushed_ago is None else now - timedelta(seconds=pushed_ago)
    monkeypatch.setattr(sync_status_fetcher, "_last_write_time", last_write)
    monkeypatch.setattr(sync_status_fetcher, "_freshness_prober", None)
    code_without, body_without = sync_status_fetcher._health(now)

    monkeypatch.setattr(sync_status_fetcher, "_freshness_prober", _FakeProber())
    code_with, body_with = sync_status_fetcher._health(now)

    assert code_with == code_without
    assert body_with == {**body_without, "freshness": _FakeProber().health(now)}
