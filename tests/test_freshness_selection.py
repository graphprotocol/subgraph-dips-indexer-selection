"""Tests for leaving stale indexers out of selection on chains whose freshness is enforced."""

import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.freshness import Check, minute_index
from iisa.iisa_http_endpoints import (
    IISAState,
    SelectionRequest,
    Settings,
    _select_with_processor,
    app,
)
from iisa.indexer_selection import IndexerSelector

CHAIN = "arbitrum-one"
OTHER_CHAIN = "mainnet"
DEPLOYMENT = "QmFakeFreshnessDeployment"
# BEST outscores GOOD, which outscores FAIR, on every metric.
BEST, GOOD, FAIR = ("0x" + pair * 20 for pair in ("a1", "b2", "c3"))


def _record(indexer: str, rank: int) -> dict[str, Any]:
    """Scores for 1 indexer priced on both chains; a lower rank scores better on every metric."""
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_coefficient_upper_bound": 0.01 * (rank + 1),
        "lat_normalized_score": 0.9 - 0.3 * rank,
        "uptime_score": 0.99 - 0.1 * rank,
        "success_rate": 0.99 - 0.1 * rank,
        "stake_to_fees": 3_000.0 / (rank + 1),
        "total_query_fees": 2_000.0,
        "last_known_slashable_stake": 3_000_000.0,
        "org": f"Org {rank}",
        "dst_lat": 10.0 * rank,
        "dst_lon": 10.0 * rank,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "{100 + 100 * rank}", '
        f'"{OTHER_CHAIN}": "{100 + 100 * rank}"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "100",
        "dips_supported_networks": f'["{CHAIN}", "{OTHER_CHAIN}"]',
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def state(tmp_path, monkeypatch) -> Iterator[IISAState]:
    """A service state with scores for BEST, GOOD and FAIR, and no chain enforced."""
    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_FRESHNESS_ENFORCED_CHAINS", raising=False)
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(tmp_path / "freshness.json"))
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "scores.json"))
    iisa_http_endpoints.get_settings.cache_clear()
    state = IISAState()
    state.initialize(Settings())
    state.load_scores_from_records([_record(BEST, 0), _record(GOOD, 1), _record(FAIR, 2)])
    iisa_http_endpoints._state = state
    yield state
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _enforce(monkeypatch, chains: str) -> None:
    monkeypatch.setenv("IISA_FRESHNESS_ENFORCED_CHAINS", chains)
    iisa_http_endpoints.get_settings.cache_clear()


def _judge(state: IISAState, outcomes: dict[tuple[str, str], str]) -> None:
    """Push the same outcome for each (indexer, chain) for 5 minutes, enough to judge it."""
    now = minute_index(datetime.now(timezone.utc))
    for minute in range(now - 10, now - 5):
        checks = [Check(ix, chain, outcome) for (ix, chain), outcome in outcomes.items()]
        state.freshness.apply(minute, checks, lambda _: None)


def _request(chain: str, size: int, existing: list[str] | None) -> SelectionRequest:
    return SelectionRequest(
        deployment_id=DEPLOYMENT, chain_id=chain, num_candidates=size, existing_indexers=existing
    )


def _select(chain: str = CHAIN, size: int = 1, existing: list[str] | None = None) -> list[str]:
    return [
        indexer.id for indexer in _select_with_processor(_request(chain, size, existing)).indexers
    ]


def _scores(size: int, existing: list[str]) -> dict[str, float]:
    """Each indexer's weighted score in a selection on CHAIN."""
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    with patch.object(iisa_http_endpoints, "IndexerSelector", side_effect=build):
        _select_with_processor(_request(CHAIN, size, existing))
    return dict(zip(built[0].data["indexer"], built[0].data["weighted_score"]))


def test_marked_indexer_is_still_picked_when_no_chain_is_enforced(state):
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})

    assert _select() == [BEST]


def test_marked_candidate_is_left_out_on_an_enforced_chain(state, monkeypatch, caplog):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})

    with caplog.at_level(logging.INFO, logger="iisa-service"):
        assert _select() == [GOOD]

    assert any("left out 1 of 3 candidates as stale" in r.getMessage() for r in caplog.records)


def test_indexer_with_no_record_is_left_out_on_an_enforced_chain(state, monkeypatch):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})

    assert _select() == [GOOD]


def test_marked_group_member_stays_in_the_group(state, monkeypatch):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "fail", (FAIR, CHAIN): "pass"})

    assert _select(size=2, existing=[GOOD]) == [GOOD, FAIR]
    assert _select(size=1, existing=[GOOD]) == [GOOD]


def test_stale_group_member_is_scored_as_if_nothing_were_enforced(state, monkeypatch):
    """BEST is in the group and sets the scale. Were it treated as declined it would stop
    setting it, and GOOD and FAIR would score higher than they do with nothing enforced.
    """
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})
    plain = _scores(size=2, existing=[BEST])
    _enforce(monkeypatch, CHAIN)

    assert _scores(size=2, existing=[BEST]) == plain


def test_every_candidate_marked_picks_nobody_new(state, monkeypatch):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "fail", (FAIR, CHAIN): "no_evidence"})

    assert _select(size=3) == []
    assert _select(size=3, existing=[FAIR]) == [FAIR]


def test_enforced_chain_with_no_push_picks_nobody_and_warns(state, monkeypatch, caplog):
    _enforce(monkeypatch, CHAIN)

    with caplog.at_level(logging.WARNING, logger="iisa-service"):
        assert _select(size=2) == []

    assert any("no freshness push" in r.getMessage() for r in caplog.records)


def test_other_chains_are_unaffected(state, monkeypatch):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (BEST, OTHER_CHAIN): "fail"})

    assert _select(chain=OTHER_CHAIN) == [BEST]


def test_select_indexers_over_http_leaves_out_the_marked_candidate(state, monkeypatch):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})

    response = TestClient(app).post(
        "/select-indexers",
        json={"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1},
    )

    assert response.status_code == 200, response.text
    assert [indexer["id"] for indexer in response.json()["indexers"]] == [GOOD]


def test_selection_does_not_wait_for_a_push_being_applied(state, monkeypatch):
    """A push holds the freshness lock while it records and saves, which can take seconds.
    Selection runs on the event loop, so it must read the marks without that lock.
    """
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})
    pool = ThreadPoolExecutor(max_workers=1)

    with state.freshness._lock:
        selection = pool.submit(_select)
        finished, _ = wait([selection], timeout=5)
    pool.shutdown()

    assert finished, "selection waited for the lock a push holds"
    assert selection.result() == [GOOD]


def test_dips_indexers_still_lists_marked_indexers(state, monkeypatch):
    _enforce(monkeypatch, CHAIN)
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})

    response = TestClient(app).get("/dips-indexers", params={"chain": CHAIN})

    assert sorted(response.json()["indexers"]) == sorted([BEST, GOOD, FAIR])


class TestEnforcedChainsSetting:
    def test_parses_a_comma_separated_value_with_spaces(self, monkeypatch):
        monkeypatch.setenv("IISA_FRESHNESS_ENFORCED_CHAINS", " amoy, arbitrum-sepolia ,,matic ")

        assert Settings().freshness_enforced_chains == {"amoy", "arbitrum-sepolia", "matic"}

    @pytest.mark.parametrize("value", ["", " ", ","])
    def test_blank_value_enforces_nothing(self, monkeypatch, value):
        monkeypatch.setenv("IISA_FRESHNESS_ENFORCED_CHAINS", value)

        assert Settings().freshness_enforced_chains == frozenset()

    def test_enforces_nothing_by_default(self, monkeypatch):
        monkeypatch.delenv("IISA_FRESHNESS_ENFORCED_CHAINS", raising=False)

        assert Settings().freshness_enforced_chains == frozenset()


class TestReportedEnforcement:
    def test_each_chain_says_whether_it_is_enforced(self, state, monkeypatch):
        _enforce(monkeypatch, f"{CHAIN}, amoy")
        _judge(state, {(BEST, CHAIN): "pass", (BEST, OTHER_CHAIN): "pass"})

        chains = TestClient(app).get("/freshness").json()["chains"]

        assert {chain: body["enforced"] for chain, body in chains.items()} == {
            CHAIN: True,
            OTHER_CHAIN: False,
            "amoy": True,
        }
        assert chains["amoy"]["indexers"] == {}

    def test_enforced_chain_is_listed_before_the_first_push(self, state, monkeypatch):
        _enforce(monkeypatch, "amoy")

        body = TestClient(app).get("/freshness").json()

        assert body == {"last_minute": None, "chains": {"amoy": {"enforced": True, "indexers": {}}}}
