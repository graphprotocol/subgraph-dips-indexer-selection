"""Each selection leaves 1 record of who was considered and why the reply names who it does.

The reply to dipper carries only the picked indexers and their prices. The record holds the
rest: the request, every candidate with the score it was judged on and the parts of that score,
who was left out and why, each change made to the group, and the final group. It is logged as 1
JSON line and the most recent ones are served by GET /selections. Nothing here may change the
reply itself.
"""

import json
import logging
import sys
import threading
import tracemalloc
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints as endpoints
from iisa import score_loader
from iisa.freshness import Check, minute_index
from iisa.iisa_http_endpoints import IISAState, Settings, app
from iisa.indexer_selection import IndexerSelector
from iisa.selection_record import (
    LOG_PREFIX,
    MAX_RECORD_BYTES,
    MAX_RECORDS_PER_DEPLOYMENT,
    MAX_STORE_BYTES,
    RECORD_OVERHEAD_BYTES,
    SelectionRecord,
    SelectionRecordStore,
    record_json,
)

CHAIN = "arbitrum-one"
DEPLOYMENT = "QmSelectionRecord"
OTHER_DEPLOYMENT = "QmAnotherDeployment"
A, B, C, D, E, T, X = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4", "e5", "07", "f6"))
UNKNOWN = "0x" + "99" * 20

# The names a candidate's parts are recorded under, by the column selection keeps each in.
COMPONENT_COLUMNS = {
    "stake_to_fees": "norm_stake_to_fees",
    "base_price": "norm_base_price_per_epoch",
    "latency": "norm_lat_lin_reg_coefficient",
    "uptime": "norm_uptime_score",
    "success_rate": "norm_success_rate",
    "price_per_entity": "norm_price_per_entity",
    "freshness": "norm_freshness",
}
FACTORS = ("uptime", "freshness", "quality", "value")


def _scores(indexer: str, success_rate: float, **changes: Any) -> dict[str, Any]:
    """1 indexer's pushed scores, with an operator and a place of its own, so any 2 make a group
    selection accepts; a higher success rate gives a higher total, all else equal.
    """
    record = {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "success_rate": success_rate,
        "stake_to_fees": 100.0,
        "total_query_fees": 10.0,
        "last_known_slashable_stake": 1_000.0,
        "org": f"org-{indexer[2:4]}",
        "dst_lat": int(indexer[2:4], 16) / 4,
        "dst_lon": 4.9,
        "computed_at": "2026-11-12T09:00:00Z",
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "450"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": f'["{CHAIN}"]',
    }
    return {**record, **changes}


# By total alone D is the best pick, then C, then B, then A.
RANKED = [_scores(A, 0.970), _scores(B, 0.975), _scores(C, 0.980), _scores(D, 0.990)]
# T totals just under D and shares D's operator and place, so the 2 alone are not a spread group.
TWIN = _scores(T, 0.985, org=f"org-{D[2:4]}", dst_lat=int(D[2:4], 16) / 4)
# X totals about 0.03, low enough for selection to replace it when it is a member.
FAILING = _scores(
    X,
    0.5,
    uptime_score=0.5,
    stake_to_fees=0.0001,
    lat_normalized_score=0.0,
    dips_min_grt_per_30_days=f'{{"{CHAIN}": "100000"}}',
)


@pytest.fixture
def load(tmp_path, monkeypatch) -> Iterator[Callable[..., IISAState]]:
    """Load the given indexers' scores into a fresh service with no token and no exempt chain."""
    for name in ("IISA_PUSH_TOKEN", "IISA_READ_TOKEN", "IISA_REQUIRE_PUSH_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("IISA_FRESHNESS_EXEMPT_CHAINS", raising=False)
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(tmp_path / "freshness.json"))
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "scores.json"))
    endpoints.get_settings.cache_clear()

    def load(*records: dict[str, Any]) -> IISAState:
        state = IISAState()
        state.initialize(Settings())
        state.load_scores_from_records(list(records))
        endpoints._state = state
        return state

    yield load
    endpoints._state = IISAState()
    endpoints.get_settings.cache_clear()


class _RecordLines(logging.Handler):
    """Collects the record in each line the service logs for a selection."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        line = record.getMessage()
        if line.startswith(LOG_PREFIX):
            assert "\n" not in line
            self.records.append(json.loads(line.removeprefix(LOG_PREFIX)))


@pytest.fixture
def logged() -> Iterator[list[dict[str, Any]]]:
    """Every record the service logs during the test, oldest first."""
    lines, service_log = _RecordLines(), logging.getLogger("iisa-service")
    level = service_log.level
    service_log.addHandler(lines)
    service_log.setLevel(logging.INFO)
    yield lines.records
    service_log.removeHandler(lines)
    service_log.setLevel(level)


def _post(**request: Any) -> Any:
    body = {"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1, **request}
    return TestClient(app, raise_server_exceptions=False).post("/select-indexers", json=body)


@pytest.fixture
def select(logged) -> Callable[..., dict[str, Any]]:
    """Make 1 selection and return the record it logged, which names the reply's group."""

    def select(**request: Any) -> dict[str, Any]:
        before = len(logged)
        reply = _post(**request)
        assert reply.status_code == 200, reply.text
        (record,) = logged[before:]
        assert record["final_group"] == [indexer["id"] for indexer in reply.json()["indexers"]]
        return record

    return select


def _kept(deployment: str = DEPLOYMENT) -> list[dict[str, Any]]:
    """The records GET /selections answers with for the deployment."""
    answer = TestClient(app).get("/selections", params={"deployment": deployment})
    assert answer.status_code == 200, answer.text
    return answer.json()["records"]


def _candidate(record: dict[str, Any], indexer: str) -> dict[str, Any]:
    (found,) = [entry for entry in record["candidates"] if entry["indexer"] == indexer]
    return found


def _claims_synced(state: IISAState, *indexers: str) -> None:
    """Load a sync status in which each of the indexers says it has synced DEPLOYMENT."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    state.load_sync_status_from_dict(
        {indexer: {"deployments": [DEPLOYMENT], "fetched_at": fetched_at} for indexer in indexers}
    )


class FakeChecker:
    """Stands in for the gateway: the claims in `fail` are not confirmed."""

    def __init__(self, fail: tuple[str, ...] = ()) -> None:
        self.fail = set(fail)

    async def unconfirmed(self, deployment_id: str, indexers: list[str]) -> dict[str, str]:
        return {
            indexer: "the latest block is 900s old" for indexer in indexers if indexer in self.fail
        }


def _judge_freshness(state: IISAState, outcomes: dict[str, str]) -> None:
    """Push the same outcome for each indexer on CHAIN for 5 minutes, enough to judge it, ending
    6 minutes ago so the chain's checks are recent enough to be enforced.
    """
    last = minute_index(datetime.now(timezone.utc)) - 6
    for minute in range(last - 4, last + 1):
        checks = [Check(indexer, CHAIN, outcome) for indexer, outcome in outcomes.items()]
        state.freshness.apply(minute, checks, lambda _: None)


# --- What a record holds -------------------------------------------------------------------


class TestTheRecord:
    def test_it_is_named_and_timed(self, load, select):
        load(*RANKED)
        before = datetime.now(timezone.utc)

        first, second = select(), select()

        assert first["selection_id"] != second["selection_id"]
        assert len(first["selection_id"]) == 32 and int(first["selection_id"], 16) >= 0
        selected_at = datetime.fromisoformat(first["selected_at"])
        assert selected_at.utcoffset() is not None
        assert before <= selected_at <= datetime.now(timezone.utc)

    def test_it_repeats_what_the_request_gave_selection(self, load, select):
        """The pending agreements and declines arrive for every deployment dipper handles.
        Selection uses the indexers pending anywhere and the declines of this deployment, so
        those are what the record keeps.
        """
        load(*RANKED)

        record = select(
            num_candidates=2,
            max_grt_per_30_days=4_500.0,
            max_grt_per_billion_entities_per_30_days=2_000.0,
            existing_indexers=[D],
            blocklist=[C],
            pending_agreements={OTHER_DEPLOYMENT: [B], "QmThird": [B, UNKNOWN]},
            declined_indexers={DEPLOYMENT: [E], OTHER_DEPLOYMENT: [A]},
            optimistic_dips_fees={D: 25.0},
        )

        assert record["request"] == {
            "deployment_id": DEPLOYMENT,
            "chain_id": CHAIN,
            "num_candidates": 2,
            "max_grt_per_30_days": 4_500.0,
            "max_grt_per_billion_entities_per_30_days": 2_000.0,
            "existing_indexers": [D],
            "blocklist": [C],
            "pending_indexers": sorted([B, UNKNOWN]),
            "declined_indexers": [E],
            "optimistic_dips_fees": {D: 25.0},
        }
        assert record["outcome"] == "selected" and record["truncated"] is False

    def test_each_pick_is_a_step_and_each_candidate_left_out_has_its_reason(self, load, select):
        load(*RANKED, _scores(E, 0.960))

        record = select(
            num_candidates=2,
            blocklist=[C],
            pending_agreements={OTHER_DEPLOYMENT: [B]},
            declined_indexers={DEPLOYMENT: [E]},
        )

        assert record["final_group"] == [D, A]
        assert [(step["action"], step["indexer"], step["pool"]) for step in record["steps"]] == [
            ("added", D, "all"),
            ("added", A, "all"),
        ]
        assert all(step["decentralisation"] == "met" for step in record["steps"])
        assert {c["indexer"]: (c["picked"], c["reasons"]) for c in record["candidates"]} == {
            D: (True, []),
            A: (True, []),
            C: (False, ["blocklisted"]),
            B: (False, ["pending_agreement"]),
            E: (False, ["declined"]),
        }
        assert not any(candidate["member"] for candidate in record["candidates"])

    def test_a_candidate_that_fits_every_rule_is_left_out_when_the_group_is_full(
        self, load, select
    ):
        load(*RANKED)

        record = select(num_candidates=1)

        assert record["final_group"] == [D]
        assert [c["indexer"] for c in record["candidates"]] == [D, C, B, A]
        assert [c["reasons"] for c in record["candidates"]] == [[], *[["group_full"]] * 3]
        assert {c["pool"] for c in record["candidates"]} == {"all"}

    def test_every_reason_that_applies_is_listed(self, load, select):
        load(*RANKED)

        record = select(blocklist=[C], declined_indexers={DEPLOYMENT: [C]})

        assert _candidate(record, C)["reasons"] == ["blocklisted", "declined"]
        assert _candidate(record, C)["pool"] is None

    def test_the_scores_are_the_numbers_selection_ranked_on(self, load, select, monkeypatch):
        """Every number in the record is read from the table selection ranked the candidates
        on, in full, so the record can never disagree with the pick.
        """
        load(*RANKED)
        built: list[IndexerSelector] = []
        build = endpoints._build_selector

        def remember(*args: Any) -> IndexerSelector:
            built.append(build(*args))
            return built[-1]

        monkeypatch.setattr(endpoints, "_build_selector", remember)

        record = select(num_candidates=2)

        (selector,) = built
        assert len(record["candidates"]) == len(selector.data) == 4
        for _, row in selector.data.iterrows():
            candidate = _candidate(record, row["indexer"])
            assert candidate["weighted_score"] == float(row["weighted_score"])
            assert candidate["factors"] == {name: float(row[f"factor_{name}"]) for name in FACTORS}
            # Freshness isn't weighed on a chain without a week of checks, so it has no part.
            assert candidate["components"] == {
                name: float(row[column])
                for name, column in COMPONENT_COLUMNS.items()
                if name != "freshness"
            }
            assert candidate["org"] == f"org-{row['indexer'][2:4]}"
            assert candidate["location"] == row["destination_loc"]
        for step in record["steps"]:
            assert step["weighted_score"] == _candidate(record, step["indexer"])["weighted_score"]


class TestDecentralisation:
    def test_a_better_candidate_passed_over_to_keep_the_group_spread_says_so(self, load, select):
        load(_scores(A, 0.970), _scores(C, 0.980), _scores(D, 0.990), TWIN)

        record = select(num_candidates=2)

        assert record["final_group"] == [D, C]
        assert record["steps"][1]["passed_over"] == [T]
        assert record["steps"][1]["decentralisation"] == "met"
        assert _candidate(record, T)["reasons"] == ["decentralisation"]
        assert _candidate(record, A)["reasons"] == ["group_full"]
        assert record["decentralisation"] == {"met": True, "fell_back": False}

    def test_a_pick_that_could_not_keep_the_group_spread_is_a_fallback(self, load, select):
        load(_scores(D, 0.990), TWIN)

        record = select(num_candidates=2)

        assert record["final_group"] == [D, T]
        assert [step["decentralisation"] for step in record["steps"]] == ["met", "fallback"]
        assert record["decentralisation"] == {"met": False, "fell_back": True}

    def test_a_group_of_1_has_no_rule_to_meet(self, load, select):
        load(*RANKED)

        assert select(num_candidates=1)["decentralisation"]["met"] is None


class TestPriceChecks:
    def test_an_indexer_the_price_checks_leave_out_is_listed_with_the_check_it_failed(
        self, load, select
    ):
        no_info, wrong_chain, unpriced, dear, bad_entity, dear_entity = (
            "0x" + pair * 20 for pair in ("11", "22", "33", "44", "55", "66")
        )
        load(
            _scores(D, 0.990),
            _scores(no_info, 0.9, dips_info_available=False),
            _scores(wrong_chain, 0.9, dips_supported_networks='["mainnet"]'),
            _scores(unpriced, 0.9, dips_min_grt_per_30_days='{"mainnet": "450"}'),
            _scores(dear, 0.9, dips_min_grt_per_30_days=f'{{"{CHAIN}": "9000"}}'),
            _scores(bad_entity, 0.9, dips_min_grt_per_billion_entities_per_30_days="abc"),
            _scores(dear_entity, 0.9, dips_min_grt_per_billion_entities_per_30_days="900"),
        )

        record = select(max_grt_per_30_days=5_000.0, max_grt_per_billion_entities_per_30_days=500.0)

        assert record["filtered_out"] == [
            {"indexer": no_info, "reason": "no_dips_info"},
            {"indexer": wrong_chain, "reason": "chain_not_supported"},
            {"indexer": unpriced, "reason": "no_price_for_chain"},
            {"indexer": dear, "reason": "over_budget"},
            {"indexer": bad_entity, "reason": "invalid_entity_price"},
            {"indexer": dear_entity, "reason": "over_entity_budget"},
        ]
        assert [candidate["indexer"] for candidate in record["candidates"]] == [D]

    def test_with_nobody_within_the_price_limits_the_record_still_says_why(self, load, select):
        """No selection runs, and the members that may stay are the answer."""
        no_info = "0x" + "11" * 20
        load(*RANKED, _scores(no_info, 0.9, dips_info_available=False))

        record = select(num_candidates=2, max_grt_per_30_days=1.0, existing_indexers=[no_info])

        assert record["outcome"] == "no_candidates"
        assert record["final_group"] == [no_info]
        assert {entry["reason"] for entry in record["filtered_out"]} == {"over_budget"}
        assert sorted(entry["indexer"] for entry in record["filtered_out"]) == [A, B, C, D]
        assert record["candidates"] == [
            {
                "indexer": no_info,
                "member": True,
                "picked": True,
                "weighted_score": None,
                "factors": {},
                "components": {},
                "org": None,
                "location": None,
                "sync_status": None,
                "stale": False,
                "pool": None,
                "reasons": [],
                "unscored_reason": "no_dips_info",
            }
        ]
        assert record["steps"] == [] and record["decentralisation"] is None

    def test_a_request_for_nobody_is_recorded_as_such(self, load, select):
        load(*RANKED)

        record = select(num_candidates=0, existing_indexers=[A])

        assert record["outcome"] == "nothing_asked_for"
        assert record["request"]["existing_indexers"] == [A]
        assert record["candidates"] == [] and record["filtered_out"] == []
        assert record["final_group"] == [] and record["steps"] == []


class TestMembers:
    def test_a_member_stays_with_or_without_a_score(self, load, select):
        """A member whose DIPs info is missing, or that the scores don't know, keeps its place,
        and the record says why it has no score.
        """
        no_info = "0x" + "11" * 20
        load(*RANKED, _scores(no_info, 0.9, dips_info_available=False))

        record = select(num_candidates=3, existing_indexers=[A, no_info, UNKNOWN])

        assert record["final_group"] == [A, no_info, UNKNOWN] and record["steps"] == []
        assert _candidate(record, A)["weighted_score"] is not None
        assert _candidate(record, A)["unscored_reason"] is None
        for member, why in ((no_info, "no_dips_info"), (UNKNOWN, "no_score_row")):
            entry = _candidate(record, member)
            assert (entry["member"], entry["picked"], entry["reasons"]) == (True, True, [])
            assert (entry["weighted_score"], entry["unscored_reason"]) == (None, why)
        assert record["filtered_out"] == []
        # Nobody was looked for, so no candidate stood in a pool.
        for candidate in (B, C, D):
            assert _candidate(record, candidate)["reasons"] == ["group_full"]
            assert _candidate(record, candidate)["pool"] is None

    def test_a_member_taken_out_before_scoring_is_listed_with_why(self, load, select):
        dear = _scores(B, 0.975, dips_min_grt_per_30_days=f'{{"{CHAIN}": "9000"}}')
        load(_scores(A, 0.970), dear, _scores(C, 0.980), _scores(D, 0.990))

        record = select(
            num_candidates=2,
            existing_indexers=[A, B],
            blocklist=[A],
            max_grt_per_30_days=5_000.0,
        )

        assert record["final_group"] == [D, C]
        blocked, priced_out = _candidate(record, A), _candidate(record, B)
        assert (blocked["member"], blocked["picked"]) == (True, False)
        assert blocked["reasons"] == ["blocklisted"] and blocked["weighted_score"] is not None
        assert (priced_out["member"], priced_out["picked"]) == (True, False)
        assert priced_out["reasons"] == ["over_budget"]
        assert (priced_out["weighted_score"], priced_out["unscored_reason"]) == (
            None,
            "over_budget",
        )

    def test_a_failing_member_swapped_out_is_recorded_as_replaced(self, load, select):
        load(*RANKED, FAILING)

        record = select(num_candidates=1, existing_indexers=[X])

        assert record["final_group"] == [D]
        (step,) = record["steps"]
        assert (step["action"], step["indexer"], step["replaced"]) == ("replaced", D, X)
        assert (step["pool"], step["decentralisation"]) == ("all", "met")
        assert step["weighted_score"] == _candidate(record, D)["weighted_score"]
        assert _candidate(record, X)["reasons"] == ["replaced"]
        assert (_candidate(record, X)["member"], _candidate(record, X)["picked"]) == (True, False)

    def test_a_member_removed_from_a_group_over_its_target_is_recorded(self, load, select):
        load(*RANKED)

        record = select(num_candidates=2, existing_indexers=[A, B, C])

        assert record["final_group"] == [B, C]
        (step,) = record["steps"]
        assert (step["action"], step["indexer"]) == ("removed", A)
        assert (step["pool"], step["decentralisation"], step["passed_over"]) == (None, None, [])
        assert step["weighted_score"] == _candidate(record, A)["weighted_score"]
        assert _candidate(record, A)["reasons"] == ["over_target"]


class TestSyncedClaims:
    def test_a_claim_taken_on_trust_is_recorded_with_the_pool_it_put_the_candidate_in(
        self, load, select
    ):
        _claims_synced(load(*RANKED), A)

        record = select(num_candidates=2)

        assert record["final_group"] == [A, D] and record["sync_check"] is None
        assert [(step["indexer"], step["pool"]) for step in record["steps"]] == [
            (A, "synced"),
            (D, "unsynced"),
        ]
        assert _candidate(record, A)["sync_status"] == "synced"
        assert {_candidate(record, other)["sync_status"] for other in (B, C, D)} == {"not_synced"}
        assert {_candidate(record, other)["pool"] for other in (B, C)} == {"unsynced"}

    def test_a_confirmed_claim_is_recorded_as_confirmed(self, load, select):
        state = load(*RANKED)
        _claims_synced(state, A)
        state.sync_checker = FakeChecker()

        record = select()

        assert record["final_group"] == [A]
        assert _candidate(record, A)["sync_status"] == "confirmed"
        assert record["sync_check"] == {
            "asked": [A],
            "unconfirmed": {},
            "not_asked": [],
            "decided_again": False,
        }

    def test_an_unconfirmed_claim_is_recorded_with_the_gateways_reason(self, load, select):
        """The selection is decided again without the claim, and the record is of that one."""
        state = load(*RANKED)
        _claims_synced(state, A)
        state.sync_checker = FakeChecker(fail=(A,))

        record = select()

        assert record["final_group"] == [D]
        assert _candidate(record, A)["sync_status"] == "unconfirmed"
        assert (_candidate(record, A)["pool"], _candidate(record, A)["reasons"]) == (
            "all",
            ["group_full"],
        )
        assert record["sync_check"] == {
            "asked": [A],
            "unconfirmed": {A: "the latest block is 900s old"},
            "not_asked": [],
            "decided_again": True,
        }
        assert len(record["steps"]) == 1

    def test_a_claim_set_aside_unchecked_is_told_apart_from_one_that_counted(self, load, select):
        """B leads on a claim that fails, so selection decides again counting only the claim of
        D, a member, and of A, which was confirmed. C is blocked, so its claim could win nothing
        and was never asked about; it is set aside with the rest.
        """
        state = load(*RANKED)
        _claims_synced(state, A, B, C, D)
        state.sync_checker = FakeChecker(fail=(B,))

        record = select(num_candidates=2, existing_indexers=[D], blocklist=[C])

        statuses = {c["indexer"]: c["sync_status"] for c in record["candidates"]}
        assert statuses == {D: "synced", A: "confirmed", B: "unconfirmed", C: "set_aside"}
        assert record["final_group"] == [D, A]


class TestStaleCandidates:
    def test_a_stale_candidate_is_left_out_while_a_fresh_one_can_fill_the_group(self, load, select):
        _judge_freshness(load(*RANKED), {A: "pass", B: "pass", C: "pass", D: "fail"})

        record = select(num_candidates=2)

        assert record["final_group"] == [C, B] and record["stale_fill"] is False
        assert (_candidate(record, D)["stale"], _candidate(record, D)["reasons"]) == (
            True,
            ["stale"],
        )
        assert not any(_candidate(record, fresh)["stale"] for fresh in (A, B, C))

    def test_a_stale_candidate_that_fills_a_place_is_recorded_as_picked(self, load, select):
        _judge_freshness(load(*RANKED), {A: "fail", B: "pass", C: "pass", D: "fail"})

        record = select(num_candidates=3)

        assert record["final_group"] == [C, B, D] and record["stale_fill"] is True
        assert (_candidate(record, D)["stale"], _candidate(record, D)["picked"]) == (True, True)
        # A was stale too, and could be picked once the stale filled places, but D took the last.
        assert _candidate(record, A)["reasons"] == ["group_full"]

    def test_a_pick_made_before_the_stale_were_scored_in_keeps_the_score_it_was_picked_on(
        self, load, select, monkeypatch
    ):
        """Once no fresh candidate is left, everyone is scored again with the stale candidates
        setting each metric's scale. B was picked before that, on a higher total than the
        second scoring gives it, and the record keeps the total B was picked on.
        """
        state = load(
            _scores(A, 0.970, stake_to_fees=1_000.0),
            _scores(B, 0.975, stake_to_fees=2_000.0),
            _scores(C, 0.980, stake_to_fees=100_000.0),
        )
        _judge_freshness(state, {A: "pass", B: "pass", C: "fail"})
        built: list[IndexerSelector] = []
        build = endpoints._build_selector

        def remember(*args: Any) -> IndexerSelector:
            built.append(build(*args))
            return built[-1]

        monkeypatch.setattr(endpoints, "_build_selector", remember)

        record = select(num_candidates=3)

        assert record["final_group"] == [B, A, C] and record["stale_fill"] is True
        rescored = built[0].data.set_index("indexer")["weighted_score"]
        picked_on = {step["indexer"]: step["weighted_score"] for step in record["steps"]}
        assert picked_on[B] > float(rescored[B])
        assert _candidate(record, B)["weighted_score"] == picked_on[B]
        assert _candidate(record, C)["weighted_score"] == picked_on[C] == float(rescored[C])


# --- The record never touches the reply ------------------------------------------------------


class TestTheReplyIsUntouched:
    REQUEST = {"num_candidates": 2, "existing_indexers": [A], "blocklist": [C]}

    def test_the_reply_is_the_same_with_and_without_the_record(self, load, logged, monkeypatch):
        load(*RANKED)
        recorded = _post(**self.REQUEST)

        monkeypatch.setattr(endpoints, "_record_selection", lambda *_args: None)
        unrecorded = _post(**self.REQUEST)

        assert len(logged) == 1
        assert recorded.status_code == unrecorded.status_code == 200
        assert recorded.content == unrecorded.content
        assert recorded.headers == unrecorded.headers
        assert set(recorded.json()) == {"deployment_id", "indexers"}

    def test_a_record_that_cannot_be_built_never_fails_the_selection(
        self, load, logged, monkeypatch, caplog
    ):
        load(*RANKED)
        expected = _post(**self.REQUEST).content

        def broken(*_args: Any) -> None:
            raise RuntimeError("a fault nobody planned for")

        monkeypatch.setattr(endpoints, "_selection_record", broken)
        with caplog.at_level(logging.ERROR, logger="iisa-service"):
            reply = _post(**self.REQUEST)

        assert reply.status_code == 200 and reply.content == expected
        assert len(logged) == 1
        (error,) = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert DEPLOYMENT in error.getMessage() and error.exc_info is not None

    def test_a_selection_that_gets_no_answer_leaves_no_record(self, load, logged, monkeypatch):
        load(*RANKED)

        def broken(*_args: Any) -> None:
            raise RuntimeError("selection failed")

        monkeypatch.setattr(endpoints, "_build_selector", broken)

        assert _post().status_code == 500
        assert _post(chain_id="").status_code == 422
        endpoints._state._history = None
        assert _post().status_code == 503
        assert logged == []


# --- The size limit --------------------------------------------------------------------------


# Ids of the longest kind a request may carry, enough of them to put a record over the limit.
LONG_LIST = ["0x" + f"{number:0126x}" for number in range(3_000)]


def test_a_record_over_the_size_limit_still_says_who_was_picked_and_how(load, logged, caplog):
    """/select-indexers takes no token, so a huge request must not become a huge record. Only
    the long lists go: the request's own, the candidates and the indexers priced out.
    """
    state = load(*RANKED, FAILING)
    _claims_synced(state, A)
    state.sync_checker = FakeChecker()
    assert len(json.dumps(LONG_LIST)) > MAX_RECORD_BYTES

    with caplog.at_level(logging.WARNING, logger="iisa.selection_record"):
        reply = _post(num_candidates=2, max_grt_per_30_days=4_500.0, blocklist=LONG_LIST)

    assert [indexer["id"] for indexer in reply.json()["indexers"]] == [A, D]
    (record,) = logged
    assert record["truncated"] is True
    assert record["final_group"] == [A, D]
    assert [(step["action"], step["indexer"]) for step in record["steps"]] == [
        ("added", A),
        ("added", D),
    ]
    assert record["sync_check"]["asked"] == [A]
    assert record["decentralisation"] == {"met": True, "fell_back": False}
    assert record["request"]["deployment_id"] == DEPLOYMENT
    assert record["request"]["num_candidates"] == 2
    assert record["request"]["blocklist"] == []
    assert record["candidates"] == [] and record["filtered_out"] == []
    assert len(json.dumps(record)) < 2_000
    (warning,) = [r.getMessage() for r in caplog.records if r.name == "iisa.selection_record"]
    assert DEPLOYMENT in warning and f"{MAX_RECORD_BYTES:,}" in warning


def test_a_record_still_over_the_limit_without_its_long_lists_keeps_only_its_header(load, select):
    """Thousands of members can make the final group and the steps long too, and the limit
    holds whatever is sent.
    """
    load(*RANKED)
    whole = select(num_candidates=2)
    record = SelectionRecord.model_validate({**whole, "final_group": LONG_LIST})

    kept = json.loads(record_json(record))

    assert kept["truncated"] is True
    assert kept["final_group"] == [] and kept["steps"] == [] and kept["candidates"] == []
    assert kept["selection_id"] == whole["selection_id"]
    assert kept["request"]["num_candidates"] == 2 and kept["outcome"] == "selected"
    assert len(json.dumps(kept)) < 2_000


def test_a_record_is_held_at_the_size_the_limit_measured_it_at(load):
    """Python holds text at 4 bytes a character once any 1 character is outside its basic
    plane, so a record held as text could take 4 times the bytes the limit let through.
    """
    load(*RANKED)
    wide = ["\U0001f600" + f"{number:0127x}" for number in range(1_900)]

    assert _post(blocklist=wide).status_code == 200

    (held,) = endpoints._state.selection_records.for_deployment(DEPLOYMENT)
    record = json.loads(held)
    assert record["truncated"] is False and record["request"]["blocklist"] == wide
    # 64 bytes is room for the held object's own header.
    assert sys.getsizeof(held) <= MAX_RECORD_BYTES + 64


# --- The bound on the records kept -------------------------------------------------------------


class TestTheBound:
    def test_a_deployment_keeps_only_its_most_recent_records(self):
        store = SelectionRecordStore()

        for number in range(MAX_RECORDS_PER_DEPLOYMENT + 5):
            store.add(DEPLOYMENT, b"record %d" % number)
        store.add(OTHER_DEPLOYMENT, b"another deployment's record")

        kept = store.for_deployment(DEPLOYMENT)
        assert MAX_RECORDS_PER_DEPLOYMENT == 20 and len(kept) == 20
        assert kept[0] == b"record 24" and kept[-1] == b"record 5"
        assert store.for_deployment(OTHER_DEPLOYMENT) == [b"another deployment's record"]

    def test_small_records_of_many_deployments_do_not_push_each_other_out(self):
        store = SelectionRecordStore()

        for number in range(5_000):
            store.add(f"Qm{number}", b"record %d" % number)

        kept = [store.for_deployment(f"Qm{number}") for number in range(5_000)]
        assert kept == [[b"record %d" % number] for number in range(5_000)]

    def test_over_its_budget_the_store_drops_the_oldest_records_of_all(self):
        """A record counts for its own bytes and RECORD_OVERHEAD_BYTES, so a large one pushes
        out as many old ones as it takes to fit.
        """
        small = RECORD_OVERHEAD_BYTES + 1_000
        store = SelectionRecordStore(budget=10 * small)

        for number in range(12):
            store.add(f"Qm{number}", b"%03d" % number + b"x" * 997)
        still_held = [number for number in range(12) if store.for_deployment(f"Qm{number}")]
        assert still_held == list(range(2, 12))

        store.add("QmLarge", b"x" * (3 * small))
        still_held = [number for number in range(12) if store.for_deployment(f"Qm{number}")]
        assert still_held == list(range(6, 12))
        assert store.for_deployment("QmLarge") == [b"x" * (3 * small)]

    def test_a_full_deployment_gives_up_its_own_oldest_record_not_another_deployments(self):
        """The budget has room for 3 records, and the 2 that DEPLOYMENT gives up for being
        full no longer count towards it.
        """
        store = SelectionRecordStore(per_deployment=2, budget=3 * (RECORD_OVERHEAD_BYTES + 100))

        store.add(OTHER_DEPLOYMENT, b"the oldest record of all")
        for number in range(4):
            store.add(DEPLOYMENT, b"record %d" % number)

        assert store.for_deployment(OTHER_DEPLOYMENT) == [b"the oldest record of all"]
        assert store.for_deployment(DEPLOYMENT) == [b"record 3", b"record 2"]

    def test_what_the_store_holds_fits_its_budget_whatever_is_sent(self):
        """The most the store can spend on bookkeeping: 1 tiny record for each of thousands of
        deployments, each named in 128 characters that Python holds at 4 bytes apiece.
        """
        budget = 4 * 1024 * 1024
        already_tracing = tracemalloc.is_tracing()
        tracemalloc.start()
        before, _ = tracemalloc.get_traced_memory()

        store = SelectionRecordStore(budget=budget)
        for number in range(6_000):
            store.add("\U0001f600" + f"{number:0127d}", b"%d" % number)

        held, _ = tracemalloc.get_traced_memory()
        if not already_tracing:
            tracemalloc.stop()
        assert held - before <= budget
        assert store.for_deployment("\U0001f600" + f"{5_999:0127d}") == [b"5999"]
        assert store.for_deployment("\U0001f600" + f"{0:0127d}") == []

    def test_the_default_budget_is_128_megabytes(self):
        assert MAX_STORE_BYTES == 128 * 1024 * 1024

    def test_records_added_and_read_at_once_stay_within_both_limits(self):
        """Selections add records on the event loop while reads run on worker threads. Threads
        are made to take turns as often as Python allows, so an add or a read that could be cut
        into by another would be.
        """
        store = SelectionRecordStore(per_deployment=5, budget=60 * (RECORD_OVERHEAD_BYTES + 8))
        deployments = [f"Qm{number}" for number in range(40)]
        faults: list[str] = []
        done = threading.Event()

        def add(worker: int) -> None:
            try:
                for number in range(3_000):
                    store.add(deployments[(worker + number) % 40], b"%08d" % number)
            except Exception as fault:
                faults.append(repr(fault))

        def read() -> None:
            try:
                while not done.is_set():
                    held = max(len(store.for_deployment(deployment)) for deployment in deployments)
                    if held > 5:
                        faults.append(f"a deployment held {held} records")
            except Exception as fault:
                faults.append(repr(fault))

        writers = [threading.Thread(target=add, args=(worker,)) for worker in range(8)]
        readers = [threading.Thread(target=read) for _ in range(4)]
        usual_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for thread in writers + readers:
                thread.start()
            for thread in writers:
                thread.join()
            done.set()
            for thread in readers:
                thread.join()
        finally:
            sys.setswitchinterval(usual_interval)

        kept = [store.for_deployment(deployment) for deployment in deployments]
        assert faults == []
        assert max(len(records) for records in kept) <= 5
        assert sum(len(records) for records in kept) == 60

    def test_the_service_keeps_20_selections_of_a_deployment(self, load):
        load(*RANKED)

        for size in range(1, MAX_RECORDS_PER_DEPLOYMENT + 3):
            assert _post(num_candidates=size % 4).status_code == 200

        kept = _kept()
        assert len(kept) == MAX_RECORDS_PER_DEPLOYMENT
        asked_for = [record["request"]["num_candidates"] for record in kept]
        assert asked_for == [size % 4 for size in range(22, 2, -1)]


# --- The route ---------------------------------------------------------------------------------
#
# test_read_token.py covers its token check with the other routes that only read.


class TestTheRoute:
    def test_it_answers_with_one_deployments_records_newest_first(self, load, select):
        """The records it answers with are the ones logged, number for number."""
        load(*RANKED)
        first = select(num_candidates=1)
        elsewhere = select(deployment_id=OTHER_DEPLOYMENT)
        second = select(num_candidates=2, blocklist=[C])

        answer = TestClient(app).get("/selections", params={"deployment": DEPLOYMENT})

        assert answer.status_code == 200
        assert answer.json() == {
            "deployment_id": DEPLOYMENT,
            "count": 2,
            "records": [second, first],
        }
        assert _kept(OTHER_DEPLOYMENT) == [elsewhere]

    def test_a_record_cut_down_for_its_size_is_kept_cut_down(self, load, logged):
        load(*RANKED)

        _post(blocklist=LONG_LIST)

        assert _kept() == logged and logged[0]["truncated"] is True

    def test_a_deployment_with_no_selections_gets_an_empty_list(self, load):
        load(*RANKED)

        answer = TestClient(app).get("/selections", params={"deployment": "QmNeverSelected"})

        assert answer.status_code == 200
        assert answer.json() == {"deployment_id": "QmNeverSelected", "count": 0, "records": []}

    @pytest.mark.parametrize("query", ["", "?deployment=", "?deployment=" + "Q" * 129])
    def test_it_needs_a_deployment_to_filter_by(self, load, query):
        load(*RANKED)

        assert TestClient(app).get(f"/selections{query}").status_code == 422

    def test_its_description_says_the_records_are_lost_on_a_restart(self):
        description = " ".join(app.openapi()["paths"]["/selections"]["get"]["description"].split())

        assert "restart" in description and "logged" in description
        assert f"at most {MAX_RECORDS_PER_DEPLOYMENT} for a deployment" in description
        assert "128 MB in all" in description

    def test_a_restart_starts_with_no_records(self, load, select):
        load(*RANKED)
        select()

        load(*RANKED)

        assert _kept() == []

    def test_a_record_that_cannot_be_built_is_not_kept(self, load, monkeypatch):
        load(*RANKED)

        def broken(*_args: Any) -> None:
            raise RuntimeError("a fault nobody planned for")

        monkeypatch.setattr(endpoints, "_selection_record", broken)

        assert _post().status_code == 200 and _kept() == []
