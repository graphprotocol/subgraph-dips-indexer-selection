"""A claim to be synced counts only where the indexer has an active allocation on-chain.

Selection prefers an indexer listed as synced on a deployment: the first gets in at any score.
The list comes from each indexer's own /status reply, so any indexer could claim every
deployment. An allocation is on-chain, so the fetcher pushes only the claims it backs.
"""

import logging
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import subgraph  # noqa: E402
import sync_status_fetcher  # noqa: E402
from subgraph import SubgraphResponseError  # noqa: E402
from sync_status_fetcher import run_fetch_cycle  # noqa: E402

FETCHED_AT = "2026-11-10T00:00:00+00:00"


def _claims(**deployments_by_indexer):
    return {
        indexer: {"deployments": deployments, "fetched_at": FETCHED_AT}
        for indexer, deployments in deployments_by_indexer.items()
    }


def _run_cycle(claims, allocations):
    """Run 1 fetch cycle on the given claims and allocations; return the pushed snapshot."""
    with (
        patch(
            "sync_status_fetcher.discover_indexers_from_network_subgraph",
            return_value={indexer: f"https://{indexer}.example" for indexer in claims},
        ),
        patch(
            "sync_status_fetcher._fetch_all_statuses", new_callable=AsyncMock, return_value=claims
        ),
        # create=True so that, before the check existed, this ran and failed on what it checks.
        patch("sync_status_fetcher.read_active_allocations", return_value=allocations, create=True),
        patch("sync_status_fetcher._push_sync_status") as push,
    ):
        assert run_fetch_cycle() is True
    push.assert_called_once()
    return push.call_args.args[0]


def test_a_claim_without_an_allocation_is_dropped():
    pushed = _run_cycle(
        _claims(**{"0xa": ["QmAllocated", "QmClaimedOnly"]}), {"0xa": {"QmAllocated"}}
    )

    assert pushed == _claims(**{"0xa": ["QmAllocated"]})


def test_claims_with_allocations_are_kept():
    claims = _claims(**{"0xa": ["QmOne", "QmTwo"], "0xb": ["QmOne"]})

    pushed = _run_cycle(claims, {"0xa": {"QmOne", "QmTwo", "QmThree"}, "0xb": {"QmOne"}})

    assert pushed == claims


def test_an_indexer_with_no_allocated_claim_is_left_out():
    pushed = _run_cycle(_claims(**{"0xa": ["QmOne"], "0xb": ["QmOne"]}), {"0xa": {"QmOne"}})

    assert pushed == _claims(**{"0xa": ["QmOne"]})


def test_a_failed_allocation_read_pushes_nothing(monkeypatch):
    # Pushing the claims unchecked would hand the head start back to anyone who claims it, so
    # the cycle fails instead and the service keeps its last snapshot.
    monkeypatch.setattr(sync_status_fetcher, "_last_write_time", None)
    with (
        patch(
            "sync_status_fetcher.discover_indexers_from_network_subgraph",
            return_value={"0xa": "https://0xa.example"},
        ),
        patch(
            "sync_status_fetcher._fetch_all_statuses",
            new_callable=AsyncMock,
            return_value=_claims(**{"0xa": ["QmOne"]}),
        ),
        patch(
            "sync_status_fetcher.read_active_allocations",
            side_effect=SubgraphResponseError("network subgraph returned no active allocations"),
            create=True,
        ),
        patch("sync_status_fetcher._push_sync_status") as push,
    ):
        with pytest.raises(SubgraphResponseError):
            run_fetch_cycle()

    push.assert_not_called()
    assert sync_status_fetcher._last_write_time is None


def test_dropped_claims_are_counted_on_1_line_that_names_no_claim(caplog):
    # A claimed deployment name is text the indexer wrote, so it never reaches the log. The
    # indexers come from the network subgraph and are quoted, and only the first 10 are named.
    claims = _claims(
        **{f"0x{i:02d}": ["QmFake\nINFO - Pushed sync status", "QmTwo"] for i in range(12)}
    )

    with caplog.at_level(logging.INFO, logger="sync_status_fetcher"):
        _run_cycle(claims, {})

    lines = [r.getMessage() for r in caplog.records if "allocation" in r.getMessage()]
    assert len(lines) == 1, lines
    assert "\n" not in lines[0] and "QmFake" not in lines[0]
    assert "24 sync claims" in lines[0] and "12 indexers" in lines[0]
    assert "'0x00': 2" in lines[0] and "and 2 more" in lines[0]


# --- Reading the active allocations ---------------------------------------------------------


@pytest.fixture
def no_retry_wait():
    """Skip the retry backoff, which otherwise sleeps for about 30s."""
    with patch.object(subgraph.read_active_allocations.retry, "sleep", lambda _: None):
        yield


def _page(allocations):
    response = MagicMock()
    response.json.return_value = {"data": {"allocations": allocations}}
    return response


def _allocation(allocation_id, indexer, deployment):
    return {
        "id": allocation_id,
        "indexer": {"id": indexer},
        "subgraphDeployment": {"ipfsHash": deployment},
    }


def test_active_allocations_are_grouped_by_indexer():
    page = _page(
        [
            _allocation("0x01", "0xaaa", "QmOne"),
            _allocation("0x02", "0xaaa", "QmTwo"),
            _allocation("0x03", "0xbbb", "QmOne"),
        ]
    )

    with patch("subgraph.requests.post", return_value=page) as post:
        allocated = subgraph.read_active_allocations("https://subgraph.example.com")

    assert allocated == {"0xaaa": {"QmOne", "QmTwo"}, "0xbbb": {"QmOne"}}
    query = post.call_args.kwargs["json"]["query"]
    assert "allocations(" in query and "status: Active" in query


def test_an_allocation_missing_its_indexer_or_deployment_is_skipped_and_logged(caplog):
    page = _page(
        [
            _allocation("0x01", "0xaaa", "QmOne"),
            {"id": "0x02", "indexer": None, "subgraphDeployment": {"ipfsHash": "QmTwo"}},
            {"id": "0x03", "indexer": {"id": "0xbbb"}},
        ]
    )

    with patch("subgraph.requests.post", return_value=page):
        with caplog.at_level(logging.WARNING, logger="subgraph"):
            allocated = subgraph.read_active_allocations("https://subgraph.example.com")

    assert allocated == {"0xaaa": {"QmOne"}}
    assert any("2 active allocations" in r.getMessage() for r in caplog.records)


def test_no_active_allocations_at_all_is_an_error(no_retry_wait):
    # The network always has some, so none means the subgraph answered from a broken copy.
    with patch("subgraph.requests.post", return_value=_page([])) as post:
        with pytest.raises(SubgraphResponseError):
            subgraph.read_active_allocations("https://subgraph.example.com")

    assert post.call_count == 5
