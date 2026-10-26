"""Tests for the sync status fetcher service."""

import sys
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Make the cronjob package importable.
jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import sync_status_fetcher  # noqa: E402
from sync_status_fetcher import (  # noqa: E402
    _fetch_all_statuses,
    _fetch_single_status,
    _push_sync_status,
    run_fetch_cycle,
)


class TestFetchSingleStatus:
    """Tests for fetching and filtering a single indexer's /status."""

    @pytest.fixture
    def mock_response(self):
        """Build a mock aiohttp response with configurable JSON."""

        def _make(json_data, status=200):
            resp = AsyncMock()
            resp.status = status
            resp.json = AsyncMock(return_value=json_data)
            resp.raise_for_status = MagicMock()
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        return _make

    @pytest.mark.asyncio
    async def test_filters_to_synced_healthy_only(self, mock_response):
        """Only deployments with synced=true AND health=healthy pass."""
        json_data = {
            "data": {
                "indexingStatuses": [
                    {"subgraph": "QmSynced", "synced": True, "health": "healthy"},
                    {"subgraph": "QmUnhealthy", "synced": True, "health": "unhealthy"},
                    {"subgraph": "QmNotSynced", "synced": False, "health": "healthy"},
                    {"subgraph": "QmFailed", "synced": False, "health": "failed"},
                ]
            }
        }
        resp = mock_response(json_data)
        session = MagicMock()
        session.post = MagicMock(return_value=resp)
        semaphore = MagicMock()
        semaphore.__aenter__ = AsyncMock()
        semaphore.__aexit__ = AsyncMock()

        result = await _fetch_single_status(
            session, "0xAAA", "https://indexer.example.com", semaphore
        )

        assert result is not None
        assert result["indexer"] == "0xAAA"
        assert result["deployments"] == ["QmSynced"]

    @pytest.mark.asyncio
    async def test_returns_none_on_persistent_failure(self, mock_response):
        """Returns None after all retries exhausted."""
        import asyncio

        # The error must come from inside the async context manager.
        # __aexit__ must return False or the exception gets suppressed.
        failing_cm = AsyncMock()
        failing_cm.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError("timeout"))
        failing_cm.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.post = MagicMock(return_value=failing_cm)
        semaphore = MagicMock()
        semaphore.__aenter__ = AsyncMock()
        semaphore.__aexit__ = AsyncMock(return_value=False)

        with patch("sync_status_fetcher.MAX_RETRIES", 1):
            result = await _fetch_single_status(
                session, "0xBBB", "https://dead.example.com", semaphore
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_statuses_returns_empty_deployments(self, mock_response):
        """Indexer with no deployments returns empty list."""
        json_data = {"data": {"indexingStatuses": []}}
        resp = mock_response(json_data)
        session = MagicMock()
        session.post = MagicMock(return_value=resp)
        semaphore = MagicMock()
        semaphore.__aenter__ = AsyncMock()
        semaphore.__aexit__ = AsyncMock()

        result = await _fetch_single_status(
            session, "0xCCC", "https://empty.example.com", semaphore
        )

        assert result is not None
        assert result["deployments"] == []


class TestFetchAllStatuses:
    """Tests for concurrent fetching across multiple indexers."""

    @pytest.mark.asyncio
    async def test_excludes_indexers_with_no_synced_deployments(self):
        """Indexers with empty deployment lists are excluded from output."""

        async def mock_fetch(session, indexer, url, sem):
            if indexer == "0xGood":
                return {"indexer": "0xGood", "deployments": ["QmA"]}
            return {"indexer": "0xEmpty", "deployments": []}

        with patch(
            "sync_status_fetcher._fetch_single_status",
            side_effect=mock_fetch,
        ):
            result = await _fetch_all_statuses(
                {
                    "0xGood": "https://good.com",
                    "0xEmpty": "https://empty.com",
                }
            )

        assert "0xGood" in result
        assert "0xEmpty" not in result
        assert "fetched_at" in result["0xGood"]

    @pytest.mark.asyncio
    async def test_excludes_failed_fetches(self):
        """Indexers that returned None are excluded."""

        async def mock_fetch(session, indexer, url, sem):
            if indexer == "0xOK":
                return {"indexer": "0xOK", "deployments": ["QmA"]}
            return None

        with patch(
            "sync_status_fetcher._fetch_single_status",
            side_effect=mock_fetch,
        ):
            result = await _fetch_all_statuses(
                {
                    "0xOK": "https://ok.com",
                    "0xDead": "https://dead.com",
                }
            )

        assert "0xOK" in result
        assert "0xDead" not in result


class TestPushSyncStatus:
    """_push_sync_status forwards the payload to iisa_client.post_sync_status."""

    def test_pushes_via_iisa_client(self):
        data = {"0xAAA": {"deployments": ["QmA"], "fetched_at": "now"}}

        with (
            patch("sync_status_fetcher.IISA_API_URL", "http://iisa:8080"),
            patch("sync_status_fetcher.IISA_PUSH_TOKEN", "test-token"),
            patch("sync_status_fetcher.post_sync_status") as mock_post,
        ):
            _push_sync_status(data)

        mock_post.assert_called_once_with("http://iisa:8080", "test-token", data)


class TestRunFetchCycle:
    """Tests for the full fetch-filter-push pipeline."""

    def test_pushes_to_iisa_on_success(self):
        indexer_urls = {"0xAAA": "https://indexer.example.com"}
        fetch_result = {
            "0xAAA": {
                "deployments": ["QmDeploy1"],
                "fetched_at": "2026-03-24T00:00:00+00:00",
            }
        }

        with (
            patch(
                "sync_status_fetcher.discover_indexers_from_network_subgraph",
                return_value=indexer_urls,
            ),
            patch(
                "sync_status_fetcher._fetch_all_statuses",
                new_callable=AsyncMock,
                return_value=fetch_result,
            ),
            patch("sync_status_fetcher._push_sync_status") as mock_push,
        ):
            result = run_fetch_cycle()

        assert result is True
        mock_push.assert_called_once_with(fetch_result)

    def test_returns_false_on_push_failure(self):
        """A push that exhausts all retries should fail the cycle (returns False)."""
        from iisa_client import IISAPushError

        indexer_urls = {"0xAAA": "https://indexer.example.com"}
        fetch_result = {
            "0xAAA": {
                "deployments": ["QmDeploy1"],
                "fetched_at": "2026-03-24T00:00:00+00:00",
            }
        }

        with (
            patch(
                "sync_status_fetcher.discover_indexers_from_network_subgraph",
                return_value=indexer_urls,
            ),
            patch(
                "sync_status_fetcher._fetch_all_statuses",
                new_callable=AsyncMock,
                return_value=fetch_result,
            ),
            patch(
                "sync_status_fetcher._push_sync_status",
                side_effect=IISAPushError("retries exhausted"),
            ),
        ):
            result = run_fetch_cycle()

        assert result is False

    def test_raises_when_indexer_urls_cannot_be_read(self):
        """The main loop logs the error with its traceback and skips the cycle."""
        with (
            patch(
                "sync_status_fetcher.discover_indexers_from_network_subgraph",
                side_effect=ValueError("GRAPH_NETWORK_SUBGRAPH_URL is not set"),
            ),
            patch("sync_status_fetcher._push_sync_status") as mock_push,
        ):
            with pytest.raises(ValueError):
                run_fetch_cycle()

        mock_push.assert_not_called()


class TestMain:
    """main() refuses to start without a usable network subgraph URL: every cycle would fail."""

    @pytest.mark.parametrize(
        "url",
        ["", "graph-node:8000/subgraphs/name/graph-network"],
        ids=["unset", "no_scheme"],
    )
    def test_refuses_to_start_without_a_usable_subgraph_url(self, monkeypatch, caplog, url):
        monkeypatch.setattr(sync_status_fetcher, "GRAPH_NETWORK_SUBGRAPH_URL", url)

        with (
            patch("sync_status_fetcher.HTTPServer") as mock_server,
            patch("sync_status_fetcher.run_fetch_cycle") as mock_cycle,
        ):
            assert sync_status_fetcher.main() == 1

        mock_server.assert_not_called()
        mock_cycle.assert_not_called()
        assert "GRAPH_NETWORK_SUBGRAPH_URL" in caplog.text

    def test_starts_with_a_usable_subgraph_url(self, monkeypatch):
        monkeypatch.setattr(
            sync_status_fetcher, "GRAPH_NETWORK_SUBGRAPH_URL", "http://graph-node:8000/x"
        )
        # Already told to stop, so main() runs 1 cycle and returns.
        stop = threading.Event()
        stop.set()
        monkeypatch.setattr(sync_status_fetcher, "_stop_event", stop)

        with (
            patch("sync_status_fetcher.signal.signal"),
            patch("sync_status_fetcher.HTTPServer"),
            patch("sync_status_fetcher.run_fetch_cycle") as mock_cycle,
        ):
            assert sync_status_fetcher.main() == 0

        mock_cycle.assert_called_once()
