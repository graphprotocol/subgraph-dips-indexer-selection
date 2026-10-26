"""How the sync-status fetcher copes with indexers that answer badly or not at all."""

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "cronjobs" / "compute_scores"))

import sync_status_fetcher  # noqa: E402
from sync_status_fetcher import (  # noqa: E402
    _fetch_single_status,
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
