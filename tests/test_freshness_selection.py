"""Tests for leaving stale indexers out of selection on every chain not exempt from freshness."""

import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from iisa import iisa_http_endpoints, score_loader
from iisa.freshness import Check, minute_index, minute_text
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
    """A service state with scores for BEST, GOOD and FAIR, and no chain exempt."""
    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_FRESHNESS_EXEMPT_CHAINS", raising=False)
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


def _exempt(monkeypatch, chains: str) -> None:
    monkeypatch.setenv("IISA_FRESHNESS_EXEMPT_CHAINS", chains)
    iisa_http_endpoints.get_settings.cache_clear()


def _judge(state: IISAState, outcomes: dict[tuple[str, str], str], last: int | None = None) -> None:
    """Push the same outcome for each (indexer, chain) for 5 minutes, enough to judge it,
    ending at the minute ``last``, by default 6 minutes ago.
    """
    if last is None:
        last = minute_index(datetime.now(timezone.utc)) - 6
    for minute in range(last - 4, last + 1):
        checks = [Check(ix, chain, outcome) for (ix, chain), outcome in outcomes.items()]
        state.freshness.apply(minute, checks, lambda _: None)


def _push_voids(state: IISAState, chain: str, minutes: range) -> None:
    """Push a void outcome for every indexer on ``chain`` in each of ``minutes``."""
    for minute in minutes:
        checks = [Check(ix, chain, "void", reason="key_out_of_funds") for ix in (BEST, GOOD, FAIR)]
        state.freshness.apply(minute, checks, lambda _: None)


def _clock(monkeypatch, minute: int) -> None:
    """Set the service's clock to ``minute``."""
    monkeypatch.setattr(iisa_http_endpoints, "_current_minute", lambda: minute)


def _not_enforced(caplog, level: int = logging.WARNING) -> list[str]:
    """The messages logged at ``level`` saying freshness isn't enforced on a chain."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == level and "isn't enforced" in r.getMessage()
    ]


# A fixed minute the tests that set the clock count from.
LAST = minute_index(datetime(2026, 11, 10, 22, 0, tzinfo=timezone.utc))
ALL_PASS_BUT_BEST = {(BEST, CHAIN): "fail", (GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"}


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


def test_marked_candidate_is_left_out_by_default(state, caplog):
    _judge(state, ALL_PASS_BUT_BEST)

    with caplog.at_level(logging.INFO, logger="iisa-service"):
        assert _select() == [GOOD]

    assert any("left out 1 of 3 candidates as stale" in r.getMessage() for r in caplog.records)


def test_marked_indexer_is_still_picked_on_an_exempt_chain(state, monkeypatch):
    _exempt(monkeypatch, f" amoy, {CHAIN} ")
    _judge(state, ALL_PASS_BUT_BEST)

    assert _select() == [BEST]


def test_exempting_one_chain_leaves_the_others_enforced(state, monkeypatch):
    _exempt(monkeypatch, OTHER_CHAIN)
    outcomes = {**ALL_PASS_BUT_BEST}
    outcomes.update({(ix, OTHER_CHAIN): o for (ix, _), o in ALL_PASS_BUT_BEST.items()})
    _judge(state, outcomes)

    assert _select(chain=OTHER_CHAIN) == [BEST]
    assert _select(chain=CHAIN) == [GOOD]


def test_indexer_with_no_record_is_left_out_once_the_chain_has_checks(state):
    _judge(state, {(GOOD, CHAIN): "pass", (FAIR, CHAIN): "pass"})

    assert _select() == [GOOD]


def test_marked_group_member_stays_in_the_group(state):
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "fail", (FAIR, CHAIN): "pass"})

    assert _select(size=2, existing=[GOOD]) == [GOOD, FAIR]
    assert _select(size=1, existing=[GOOD]) == [GOOD]


def test_stale_group_member_is_scored_as_if_the_chain_were_exempt(state, monkeypatch):
    """BEST is in the group and sets the scale. Were it treated as declined it would stop
    setting it, and GOOD and FAIR would score higher than they do on an exempt chain.
    """
    _judge(state, ALL_PASS_BUT_BEST)
    _exempt(monkeypatch, CHAIN)
    plain = _scores(size=2, existing=[BEST])
    _exempt(monkeypatch, "")

    assert _scores(size=2, existing=[BEST]) == plain


def test_every_candidate_marked_picks_nobody_new(state):
    _judge(state, {(BEST, CHAIN): "fail", (GOOD, CHAIN): "fail", (FAIR, CHAIN): "no_evidence"})

    assert _select(size=3) == []
    assert _select(size=3, existing=[FAIR]) == [FAIR]


class TestChecksTooOld:
    """On a chain that isn't exempt the marks decide selection only while a check there
    counted within IISA_FRESHNESS_MAX_AGE_MINUTES, 60 by default, of the service's clock.
    """

    @pytest.mark.parametrize(("age", "picked"), [(0, GOOD), (60, GOOD), (61, BEST)])
    def test_marks_apply_up_to_and_including_the_limit(self, state, monkeypatch, age, picked):
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + age)

        assert _select() == [picked]

    def test_marked_candidate_is_picked_with_a_warning_once_checks_are_too_old(
        self, state, monkeypatch, caplog
    ):
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + 90)

        with caplog.at_level(logging.INFO, logger="iisa-service"):
            assert _select() == [BEST]

        (warning,) = _not_enforced(caplog)
        assert f"chain={CHAIN}" in warning
        assert "last counted check was 90 minutes ago" in warning
        assert "60" in warning
        assert not any("as stale" in r.getMessage() for r in caplog.records)

    def test_each_selection_warns(self, state, monkeypatch, caplog):
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + 90)

        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            _select()
            _select()

        assert len(_not_enforced(caplog)) == 2

    def test_pushes_holding_only_voids_stop_enforcement_after_the_limit(
        self, state, monkeypatch, caplog
    ):
        """The prober keeps pushing, but its gateway key is out of funds, so every check is
        void and the marks stop changing.
        """
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _push_voids(state, CHAIN, range(LAST + 1, LAST + 61))
        _clock(monkeypatch, LAST + 60)
        assert _select() == [GOOD]

        _push_voids(state, CHAIN, range(LAST + 61, LAST + 62))
        _clock(monkeypatch, LAST + 61)
        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            assert _select() == [BEST]

        assert state.freshness.last_minute == LAST + 61
        assert "61 minutes ago" in _not_enforced(caplog)[0]

    def test_counted_checks_arriving_again_bring_enforcement_back(self, state, monkeypatch):
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + 120)
        assert _select() == [BEST]

        _judge(state, ALL_PASS_BUT_BEST, last=LAST + 119)

        assert _select() == [GOOD]

    @pytest.mark.parametrize("pushes", ["none", "only-voids"])
    def test_chain_that_never_had_a_counted_check_is_not_enforced_and_does_not_warn(
        self, state, monkeypatch, caplog, pushes
    ):
        """A chain the prober never checks, such as every chain while it is off, would
        otherwise warn on every selection.
        """
        if pushes == "only-voids":
            _push_voids(state, CHAIN, range(LAST - 9, LAST + 1))
        _clock(monkeypatch, LAST)

        with caplog.at_level(logging.DEBUG, logger="iisa-service"):
            assert _select(size=2) == [BEST, GOOD]

        assert _not_enforced(caplog) == []
        (note,) = _not_enforced(caplog, logging.DEBUG)
        assert f"chain={CHAIN}" in note
        assert "no freshness check on it has counted in the last 28 days" in note

    def test_checks_that_counted_over_28_days_ago_do_not_warn(self, state, monkeypatch, caplog):
        """Pushes holding only voids keep a chain's last counted minute on record, but a check
        that counted that long ago is no longer in the 28 days kept.
        """
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + 28 * 1_440 + 1)

        with caplog.at_level(logging.DEBUG, logger="iisa-service"):
            assert _select() == [BEST]

        assert _not_enforced(caplog) == []
        assert len(_not_enforced(caplog, logging.DEBUG)) == 1

    def test_second_chain_with_current_checks_stays_enforced(self, state, monkeypatch, caplog):
        outcomes = {**ALL_PASS_BUT_BEST}
        outcomes.update({(ix, OTHER_CHAIN): o for (ix, _), o in ALL_PASS_BUT_BEST.items()})
        _judge(state, outcomes, last=LAST)
        state.freshness.apply(
            LAST + 1,
            [Check(GOOD, OTHER_CHAIN, "pass"), Check(GOOD, CHAIN, "void")],
            lambda _: None,
        )
        _clock(monkeypatch, LAST + 61)

        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            assert _select(chain=CHAIN) == [BEST]
            assert _select(chain=OTHER_CHAIN) == [GOOD]

        (warning,) = _not_enforced(caplog)
        assert f"chain={CHAIN} " in warning

    def test_limit_comes_from_the_setting(self, state, monkeypatch, caplog):
        monkeypatch.setenv("IISA_FRESHNESS_MAX_AGE_MINUTES", "5")
        iisa_http_endpoints.get_settings.cache_clear()
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + 5)
        assert _select() == [GOOD]

        _clock(monkeypatch, LAST + 6)
        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            assert _select() == [BEST]

        assert "6 minutes ago" in _not_enforced(caplog)[0]

    def test_exempt_chain_never_warns(self, state, monkeypatch, caplog):
        _exempt(monkeypatch, CHAIN)
        _judge(state, ALL_PASS_BUT_BEST, last=LAST)
        _clock(monkeypatch, LAST + 90)

        with caplog.at_level(logging.DEBUG, logger="iisa-service"):
            assert _select() == [BEST]

        assert _not_enforced(caplog) == []
        assert _not_enforced(caplog, logging.DEBUG) == []


def test_select_indexers_over_http_leaves_out_the_marked_candidate(state):
    _judge(state, ALL_PASS_BUT_BEST)

    response = TestClient(app).post(
        "/select-indexers",
        json={"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1},
    )

    assert response.status_code == 200, response.text
    assert [indexer["id"] for indexer in response.json()["indexers"]] == [GOOD]


def test_selection_does_not_wait_for_a_push_being_applied(state):
    """A push holds the freshness lock while it records and saves, which can take seconds.
    Selection runs on the event loop, so it must read the marks without that lock.
    """
    _judge(state, ALL_PASS_BUT_BEST)
    pool = ThreadPoolExecutor(max_workers=1)

    with state.freshness._lock:
        selection = pool.submit(_select)
        finished, _ = wait([selection], timeout=5)
    pool.shutdown()

    assert finished, "selection waited for the lock a push holds"
    assert selection.result() == [GOOD]


def test_dips_indexers_still_lists_marked_indexers(state):
    _judge(state, ALL_PASS_BUT_BEST)

    response = TestClient(app).get("/dips-indexers", params={"chain": CHAIN})

    assert sorted(response.json()["indexers"]) == sorted([BEST, GOOD, FAIR])


class TestExemptChainsSetting:
    def test_parses_a_comma_separated_value_with_spaces(self, monkeypatch):
        monkeypatch.setenv("IISA_FRESHNESS_EXEMPT_CHAINS", " amoy, arbitrum-sepolia ,,matic ")

        assert Settings().freshness_exempt_chains == {"amoy", "arbitrum-sepolia", "matic"}

    @pytest.mark.parametrize("value", ["", " ", ","])
    def test_blank_value_exempts_nothing(self, monkeypatch, value):
        monkeypatch.setenv("IISA_FRESHNESS_EXEMPT_CHAINS", value)

        assert Settings().freshness_exempt_chains == frozenset()

    def test_exempts_nothing_by_default(self, monkeypatch):
        monkeypatch.delenv("IISA_FRESHNESS_EXEMPT_CHAINS", raising=False)

        assert Settings().freshness_exempt_chains == frozenset()

    def test_the_old_opt_in_setting_is_gone(self, monkeypatch):
        monkeypatch.setenv("IISA_FRESHNESS_ENFORCED_CHAINS", "amoy")

        assert not hasattr(Settings(), "freshness_enforced_chains")


class TestMaxAgeSetting:
    def test_defaults_to_60_minutes(self, monkeypatch):
        monkeypatch.delenv("IISA_FRESHNESS_MAX_AGE_MINUTES", raising=False)

        assert Settings().freshness_max_age_minutes == 60

    def test_reads_the_environment(self, monkeypatch):
        monkeypatch.setenv("IISA_FRESHNESS_MAX_AGE_MINUTES", "15")

        assert Settings().freshness_max_age_minutes == 15

    @pytest.mark.parametrize("value", ["0", "-5", "1.5", "an hour"])
    def test_refuses_anything_but_a_whole_number_of_at_least_1(self, monkeypatch, value):
        monkeypatch.setenv("IISA_FRESHNESS_MAX_AGE_MINUTES", value)

        with pytest.raises(ValidationError):
            Settings()


class TestReportedEnforcement:
    def test_each_chain_says_whether_it_is_enforced(self, state, monkeypatch):
        _exempt(monkeypatch, f"{OTHER_CHAIN}, amoy")
        _judge(state, {(BEST, CHAIN): "pass", (BEST, OTHER_CHAIN): "pass"})

        chains = TestClient(app).get("/freshness").json()["chains"]

        assert {chain: body["enforced"] for chain, body in chains.items()} == {
            CHAIN: True,
            OTHER_CHAIN: False,
            "amoy": False,
        }
        assert chains["amoy"]["indexers"] == {}

    def test_only_exempt_chains_are_listed_before_the_first_push(self, state, monkeypatch):
        assert TestClient(app).get("/freshness").json() == {"last_minute": None, "chains": {}}

        _exempt(monkeypatch, "amoy")
        body = TestClient(app).get("/freshness").json()

        assert body == {
            "last_minute": None,
            "chains": {
                "amoy": {
                    "enforced": False,
                    "enforcing": False,
                    "last_counted_minute": None,
                    "indexers": {},
                }
            },
        }

    def test_each_chain_says_whether_it_is_enforcing_and_when_a_check_last_counted(
        self, state, monkeypatch
    ):
        _exempt(monkeypatch, "amoy")
        _judge(state, {(BEST, CHAIN): "pass", (BEST, OTHER_CHAIN): "pass"}, last=LAST)
        state.freshness.apply(LAST + 1, [Check(BEST, CHAIN, "pass")], lambda _: None)
        _clock(monkeypatch, LAST + 61)

        chains = TestClient(app).get("/freshness").json()["chains"]

        assert {
            chain: (body["enforced"], body["enforcing"], body["last_counted_minute"])
            for chain, body in chains.items()
        } == {
            CHAIN: (True, True, minute_text(LAST + 1)),
            OTHER_CHAIN: (True, False, minute_text(LAST)),
            "amoy": (False, False, None),
        }
