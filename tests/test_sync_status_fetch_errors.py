"""How the sync-status fetcher copes with indexers that answer badly or not at all."""

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "cronjobs" / "compute_scores"))

import sync_status_fetcher  # noqa: E402
from sync_status_fetcher import (  # noqa: E402
    NoIndexerRespondedError,
    _fetch_all_statuses,
    _fetch_single_status,
    run_fetch_cycle,
)


def _session_returning(json_side_effect):
    """A session whose POST answers 200 and whose body parsing returns or raises the given value."""
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(side_effect=[json_side_effect])
    resp.raise_for_status = MagicMock()
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.post = MagicMock(return_value=resp)
    return session


@pytest.mark.parametrize(
    "body",
    [
        {"data": None},
        {"data": {"indexingStatuses": None}},
        ["not", "an", "object"],
        {"data": {"indexingStatuses": ["not an object"]}},
        {"data": {"indexingStatuses": [{"synced": True, "health": "healthy"}]}},
        json.JSONDecodeError("Expecting value", "<html>", 0),
    ],
    ids=["null-data", "null-statuses", "list-body", "non-object-status", "no-subgraph", "bad-json"],
)
async def test_a_malformed_reply_counts_as_no_answer(body):
    """One indexer's bad reply must not raise out of the fetch and sink every other indexer."""
    session = _session_returning(body)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await _fetch_single_status(
            session, "0xBad", "https://bad.example", sync_status_fetcher.asyncio.Semaphore(1)
        )

    assert result is None


async def test_no_answers_at_all_raises():
    """If every indexer failed, the empty result says nothing about what is synced."""

    async def no_answer(session, indexer, url, sem):
        return None

    with patch("sync_status_fetcher._fetch_single_status", side_effect=no_answer):
        with pytest.raises(NoIndexerRespondedError):
            await _fetch_all_statuses({"0xA": "https://a.example", "0xB": "https://b.example"})


async def test_answers_with_nothing_synced_are_still_a_result():
    async def nothing_synced(session, indexer, url, sem):
        return {"indexer": indexer, "deployments": []}

    with patch("sync_status_fetcher._fetch_single_status", side_effect=nothing_synced):
        result = await _fetch_all_statuses({"0xA": "https://a.example"})

    assert result == {}


def test_cycle_pushes_nothing_when_no_indexer_answered(monkeypatch):
    """Pushing an empty snapshot would wipe IISA's sync status, so the cycle fails instead."""
    monkeypatch.setattr(sync_status_fetcher, "_last_write_time", None)
    monkeypatch.setattr(
        sync_status_fetcher,
        "discover_indexers_from_network_subgraph",
        lambda _url: {"0xA": "https://a.example"},
    )
    push = MagicMock()
    monkeypatch.setattr(sync_status_fetcher, "_push_sync_status", push)

    async def none_answered(_urls):
        raise NoIndexerRespondedError("none of 1 indexers answered /status")

    monkeypatch.setattr(sync_status_fetcher, "_fetch_all_statuses", none_answered)

    assert run_fetch_cycle() is False
    push.assert_not_called()
    assert sync_status_fetcher._last_write_time is None
