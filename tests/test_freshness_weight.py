"""Tests for weighing freshness when picking indexers: each indexer's share of checks passed on
the request's chain over 28 days, shaped and weighed beside the other metrics."""

import math
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.freshness import (
    MIN_SCORED_MINUTES,
    Check,
    DayCount,
    FreshnessState,
    FreshnessStore,
    LastCheck,
    PairState,
    freshness_score,
    minute_index,
)
from iisa.iisa_http_endpoints import IISAState, SelectionRequest, Settings, _select_with_processor
from iisa.indexer_selection import DEFAULT_WEIGHTS, IndexerSelector, _calculate_weighted_scores
from iisa.share_score import SHARE_EXPONENT, shaped_share

CHAIN = "arbitrum-one"
OTHER_CHAIN = "mainnet"
DEPLOYMENT = "QmFakeFreshnessWeightDeployment"
TWIN, TWIN_TOO, NEWCOMER = ("0x" + pair * 20 for pair in ("a1", "b2", "c3"))
DAY = 1_440
# 2026-11-10T22:00:00Z, the service's last minute in these tests.
LAST = minute_index(datetime(2026, 11, 10, 22, 0, tzinfo=timezone.utc))


def _pair(passed: int, counted: int, first_minute: int) -> PairState:
    return PairState(
        marked=False,
        first_minute=first_minute,
        counting=True,
        days={LAST // DAY: DayCount(passed=passed, counted=counted)},
        last=LastCheck(minute=LAST, outcome="pass"),
    )


def _store(
    pairs: dict[tuple[str, str], tuple[int, int]], days_of_checks: float = 7
) -> FreshnessStore:
    """A store whose last minute is LAST, holding (passed, counted) over 28 days for each
    (indexer, chain), every pair first seen ``days_of_checks`` days before LAST."""
    first_minute = LAST - round(days_of_checks * DAY)
    chains: dict[str, dict[str, PairState]] = {}
    for (indexer, chain), (passed, counted) in pairs.items():
        chains.setdefault(chain, {})[indexer] = _pair(passed, counted, first_minute)
    state = FreshnessState(
        last_minute=LAST,
        received=set(range(LAST - 9, LAST + 1)),
        chains=chains,
        last_counted=dict.fromkeys(chains, LAST),
    )
    return FreshnessStore(state)


class TestShape:
    def test_a_share_is_raised_to_the_6th_power(self):
        assert SHARE_EXPONENT == 6
        assert shaped_share(1.0) == 1.0
        assert shaped_share(0.9) == pytest.approx(0.531441)
        assert shaped_share(0.8) == pytest.approx(0.262144)
        assert shaped_share(0.5) == pytest.approx(0.015625)
        assert shaped_share(0.0) == 0.0

    def test_the_score_is_the_shaped_share_of_checks_passed(self):
        assert freshness_score(18_000, 20_000) == pytest.approx(0.9**6)
        assert freshness_score(20_000, 20_000) == 1.0

    def test_the_share_is_taken_over_at_least_7_days_of_minutes(self):
        """A newcomer that passed every check in its first hour can't score 1."""
        assert MIN_SCORED_MINUTES == 10_080
        assert freshness_score(60, 60) == pytest.approx((60 / 10_080) ** 6)
        assert freshness_score(60, 60) < 1e-12
        assert freshness_score(10_080, 10_080) == 1.0
        assert freshness_score(9_072, 10_000) == pytest.approx((9_072 / 10_080) ** 6)


class TestScoresOnAChain:
    def test_a_chain_with_7_days_of_checks_is_scored(self):
        store = _store({(TWIN, CHAIN): (18_000, 20_000), (TWIN_TOO, CHAIN): (0, 20_000)})

        assert store.scores_on(CHAIN) == pytest.approx({TWIN: 0.9**6, TWIN_TOO: 0.0})

    def test_a_chain_with_less_than_7_days_of_checks_is_not_scored(self):
        store = _store({(TWIN, CHAIN): (100, 100)}, days_of_checks=7 - 1 / DAY)

        assert store.scores_on(CHAIN) is None

    def test_a_chain_with_no_records_is_not_scored(self):
        assert _store({(TWIN, CHAIN): (100, 100)}).scores_on(OTHER_CHAIN) is None
        assert FreshnessStore().scores_on(CHAIN) is None

    def test_the_chains_earliest_pair_decides_and_a_newcomer_starts_near_0(self):
        store = _store({(TWIN, CHAIN): (20_000, 20_000)})
        store.apply(LAST + 1, [Check(NEWCOMER, CHAIN, "pass")], lambda _: None)

        scores = store.scores_on(CHAIN)

        assert scores is not None
        assert scores[TWIN] == 1.0
        assert scores[NEWCOMER] == pytest.approx((1 / 10_080) ** 6)

    def test_scores_are_read_without_waiting_for_the_lock(self):
        store = _store({(TWIN, CHAIN): (20_000, 20_000)})

        with store._lock:
            assert store.scores_on(CHAIN) == {TWIN: 1.0}


def _record(indexer: str) -> dict[str, Any]:
    """Scores for 1 indexer priced on both chains, the same for every indexer but its org and
    location."""
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_coefficient_upper_bound": 0.01,
        "lat_normalized_score": 0.9,
        "uptime_score": 0.99,
        "success_rate": 0.99,
        "stake_to_fees": 3_000.0,
        "total_query_fees": 2_000.0,
        "last_known_slashable_stake": 3_000_000.0,
        "org": f"Org {indexer[2:4]}",
        "dst_lat": 10.0,
        "dst_lon": 10.0,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "100", "{OTHER_CHAIN}": "100"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "100",
        "dips_supported_networks": f'["{CHAIN}", "{OTHER_CHAIN}"]',
        # The job never pushes freshness; one in the scores must not be weighed.
        "freshness": 1.0,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def service(tmp_path, monkeypatch) -> Iterator[IISAState]:
    """A service holding identical scores for TWIN and TWIN_TOO, with its clock at LAST."""
    for name in ("IISA_PUSH_TOKEN", "IISA_REQUIRE_PUSH_TOKEN", "IISA_FRESHNESS_EXEMPT_CHAINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(tmp_path / "freshness.json"))
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "scores.json"))
    monkeypatch.setattr(iisa_http_endpoints, "_current_minute", lambda: LAST)
    iisa_http_endpoints.get_settings.cache_clear()
    state = IISAState()
    state.initialize(Settings())
    state.load_scores_from_records([_record(TWIN), _record(TWIN_TOO)])
    iisa_http_endpoints._state = state
    yield state
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _selector(chain: str = CHAIN) -> IndexerSelector:
    """The selector a selection of 1 indexer on ``chain`` builds."""
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    request = SelectionRequest(deployment_id=DEPLOYMENT, chain_id=chain, num_candidates=1)
    with patch.object(iisa_http_endpoints, "IndexerSelector", side_effect=build):
        _select_with_processor(request)
    return built[0]


def _picked(chain: str = CHAIN) -> list[str]:
    return list(_selector(chain).current_group)


class TestSelection:
    @pytest.mark.parametrize(("fresher", "staler"), [(TWIN, TWIN_TOO), (TWIN_TOO, TWIN)])
    def test_a_fresher_indexer_outranks_an_otherwise_equal_one(self, service, fresher, staler):
        service.freshness = _store(
            {(fresher, CHAIN): (19_800, 20_000), (staler, CHAIN): (18_000, 20_000)}
        )

        assert _picked() == [fresher]

    def test_the_score_on_the_requests_chain_is_weighed(self, service):
        service.freshness = _store(
            {(TWIN, CHAIN): (18_000, 20_000), (TWIN_TOO, OTHER_CHAIN): (20_000, 20_000)}
        )
        service.freshness.apply(LAST + 1, [Check(TWIN_TOO, CHAIN, "pass")], lambda _: None)

        data = _selector().data.set_index("indexer")

        assert data.loc[TWIN, "norm_freshness"] == pytest.approx(0.9**6)
        assert data.loc[TWIN_TOO, "norm_freshness"] == pytest.approx((1 / 10_080) ** 6)

    def test_an_indexer_with_no_record_on_a_scored_chain_scores_0(self, service, monkeypatch):
        """The chain is exempt, so TWIN_TOO isn't left out as stale for having no record."""
        monkeypatch.setenv("IISA_FRESHNESS_EXEMPT_CHAINS", CHAIN)
        iisa_http_endpoints.get_settings.cache_clear()
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})

        data = _selector().data.set_index("indexer")

        assert data.loc[TWIN_TOO, "norm_freshness"] == 0.0
        assert data.loc[TWIN, "norm_freshness"] == pytest.approx(0.9**6)

    @pytest.mark.parametrize("records", ["under-7-days", "none"])
    def test_freshness_is_left_out_rather_than_scored_0(self, service, records):
        """With less than 7 days of checks on the chain, or none at all, the weighted score is
        the average of the other 6 metrics."""
        if records == "under-7-days":
            service.freshness = _store(
                {(TWIN, CHAIN): (100, 100), (TWIN_TOO, CHAIN): (0, 100)}, days_of_checks=6
            )

        data = _selector().data
        others = {k: v for k, v in DEFAULT_WEIGHTS.items() if k != "freshness"}

        assert data["norm_freshness"].isna().all()
        assert not math.isnan(data["weighted_score"].iloc[0])
        assert list(data["weighted_score"]) == pytest.approx(
            list(_calculate_weighted_scores(data.drop(columns="norm_freshness"), others))
        )


class TestChecksStopCounting:
    """Freshness is weighed only while a check on the chain counted within
    IISA_FRESHNESS_MAX_AGE_MINUTES, 60 by default, of the service's clock, as enforcement
    judges it: the scores are worked out at each push, so they stop changing with the checks.
    """

    @pytest.mark.parametrize(("age", "weighed"), [(0, True), (60, True), (61, False)])
    def test_freshness_is_left_out_once_the_prober_stops(self, service, monkeypatch, age, weighed):
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})
        monkeypatch.setattr(iisa_http_endpoints, "_current_minute", lambda: LAST + age)

        freshness = _selector().data.set_index("indexer")["norm_freshness"]

        assert (freshness.notna().all(), freshness.isna().all()) == (weighed, not weighed)

    def test_pushes_holding_only_voids_leave_freshness_out_after_the_limit(
        self, service, monkeypatch
    ):
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})
        for minute in range(LAST + 1, LAST + 62):
            service.freshness.apply(minute, [Check(TWIN, CHAIN, "void")], lambda _: None)
        monkeypatch.setattr(iisa_http_endpoints, "_current_minute", lambda: LAST + 61)

        assert _selector().data["norm_freshness"].isna().all()

    def test_freshness_comes_back_once_checks_count_again(self, service, monkeypatch):
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})
        monkeypatch.setattr(iisa_http_endpoints, "_current_minute", lambda: LAST + 120)
        assert _selector().data["norm_freshness"].isna().all()

        service.freshness.apply(LAST + 119, [Check(TWIN, CHAIN, "pass")], lambda _: None)

        data = _selector().data.set_index("indexer")
        assert data.loc[TWIN, "norm_freshness"] == pytest.approx((18_001 / 20_001) ** 6)
        assert data.loc[TWIN_TOO, "norm_freshness"] == 0.0

    def test_each_chains_reported_score_leaves_freshness_out_too(self, service, monkeypatch):
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})
        monkeypatch.setattr(iisa_http_endpoints, "_current_minute", lambda: LAST + 61)

        entries = TestClient(iisa_http_endpoints.app).get("/scores/weighted").json()["scores"]
        twin = next(entry for entry in entries if entry["indexer"] == TWIN)

        assert "freshness" not in twin["chains"][CHAIN]["components"]


class TestReportedScores:
    def test_network_wide_scores_leave_freshness_out_and_each_chain_lists_it(self, service):
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})

        entries = TestClient(iisa_http_endpoints.app).get("/scores/weighted").json()["scores"]
        twin = next(entry for entry in entries if entry["indexer"] == TWIN)

        assert "freshness" not in twin["components"]
        assert twin["chains"][CHAIN]["components"]["freshness"] == pytest.approx(0.9**6)
        assert "freshness" not in twin["chains"][OTHER_CHAIN]["components"]

    def test_get_score_lists_freshness_in_each_chains_breakdown(self, service):
        service.freshness = _store({(TWIN, CHAIN): (18_000, 20_000)})

        body = (
            TestClient(iisa_http_endpoints.app).post("/get-score", json={"indexer_id": TWIN}).json()
        )

        assert "freshness" not in body["components"]
        assert body["chains"][CHAIN]["components"]["freshness"] == pytest.approx(0.9**6)
