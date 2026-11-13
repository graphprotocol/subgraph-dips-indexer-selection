"""Tests for the freshness state: the stale mark, the 28-day counts and the saved file."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import pytest

from iisa.freshness import (
    RECENT_ANSWERED_MINUTES,
    Check,
    Enforcement,
    FreshnessStore,
    load_state,
    minute_index,
    minute_text,
)

INDEXER = "0x" + "a1" * 20
OTHER = "0x" + "b2" * 20
CHAIN = "arbitrum-one"
OTHER_CHAIN = "matic"
DAY = 1_440
# 2026-11-10T00:00:00Z, the start of a UTC day.
START = minute_index(datetime(2026, 11, 10, tzinfo=timezone.utc))


def _no_save(_: dict[str, Any]) -> None:
    pass


def _check(outcome: str, indexer: str = INDEXER, chain: str = CHAIN, **fields: Any) -> Check:
    return Check(indexer=indexer, chain=chain, outcome=outcome, **fields)  # type: ignore[arg-type]


def _push(store: FreshnessStore, minute: int, *checks: Check) -> bool:
    return store.apply(minute, list(checks), _no_save)


def _run(store: FreshnessStore, outcomes: list[Optional[str]], start: int = START) -> int:
    """Push 1 minute per entry from ``start``, with the entry as INDEXER's outcome, or with no
    outcome for it when the entry is None. Returns the next minute.
    """
    for offset, outcome in enumerate(outcomes):
        checks = [_check("pass", indexer=OTHER)]
        if outcome is not None:
            checks.append(_check(outcome))
        _push(store, start + offset, *checks)
    return start + len(outcomes)


def _pair(store: FreshnessStore, indexer: str = INDEXER, chain: str = CHAIN) -> dict[str, Any]:
    return store.snapshot()["chains"][chain]["indexers"][indexer]


def test_minute_index_and_text_round_trip():
    assert minute_text(START) == "2026-11-10T00:00:00Z"
    assert minute_text(START + 91) == "2026-11-10T01:31:00Z"
    assert minute_index(datetime(2026, 11, 10, 1, 31, tzinfo=timezone.utc)) == START + 91


class TestStaleMark:
    def test_pair_passing_every_minute_is_not_marked(self):
        store = FreshnessStore()
        _run(store, ["pass"] * 10)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (False, True)
        assert (pair["window_passed"], pair["window_counted"]) == (10, 10)

    def test_pair_failing_6_of_10_is_marked(self):
        store = FreshnessStore()
        _run(store, ["pass"] * 4 + ["fail"] * 6)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (True, True)
        assert (pair["window_passed"], pair["window_counted"]) == (4, 10)

    def test_exactly_half_passing_is_not_marked(self):
        store = FreshnessStore()
        _run(store, ["fail"] * 5 + ["pass"] * 5)

        assert _pair(store)["marked"] is False

    def test_pair_seen_for_the_first_time_starts_marked(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (True, False)

    def test_newcomer_clears_only_once_half_the_window_passed(self):
        """Minutes the service received before the newcomer's first outcome count as not
        passed, so it has to pass 5 of the 10 before its mark clears.
        """
        store = FreshnessStore()
        minute = _run(store, [None] * 10)

        marks = []
        for offset in range(5):
            _push(store, minute + offset, _check("pass"), _check("pass", indexer=OTHER))
            marks.append(_pair(store)["marked"])

        assert marks == [True, True, True, True, False]
        assert _pair(store)["window_passed"] == 5

    @pytest.mark.parametrize(("history", "marked"), [("pass", False), ("fail", True)])
    def test_empty_pushes_after_a_history_leave_the_mark_alone(self, history, marked):
        """From a pair's first outcome on, a received minute without one is void, so a prober
        that sends nothing for a while doesn't mark every indexer it knows.
        """
        store = FreshnessStore()
        minute = _run(store, [history] * 10)
        for offset in range(10):
            _push(store, minute + offset)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"], pair["window_counted"]) == (marked, False, 0)

    def test_pair_left_out_of_shortened_pushes_keeps_its_mark(self):
        store = FreshnessStore()
        _run(store, ["pass"] * 10 + [None] * 6)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (False, False)
        assert (pair["window_passed"], pair["window_counted"]) == (4, 4)

    def test_pair_forgotten_after_28_days_is_a_newcomer_again(self):
        """Once dropped, the pair's earlier received minutes count as not passed again."""
        store = FreshnessStore()
        _run(store, ["pass"] * 10)
        last = START + 9 + 28 * DAY
        _run(store, [None] * 10, start=last - 9)
        assert INDEXER not in store.snapshot()["chains"][CHAIN]["indexers"]

        _push(store, last + 1, _check("pass"), _check("pass", indexer=OTHER))

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (True, True)
        assert (pair["window_passed"], pair["window_counted"]) == (1, 10)

    def test_minutes_never_received_are_void(self):
        """3 passes, 5 minutes with no push, then 2 fails: 3 of 5 counted minutes passed. Had
        the missing minutes counted as not passed, it would be 3 of 10 and marked.
        """
        store = FreshnessStore()
        _run(store, ["pass"] * 3)
        _run(store, ["fail"] * 2, start=START + 8)

        pair = _pair(store)
        assert pair["marked"] is False
        assert (pair["window_passed"], pair["window_counted"]) == (3, 5)

    def test_too_few_counted_minutes_keep_an_unmarked_pair_unmarked(self):
        store = FreshnessStore()
        minute = _run(store, ["pass"] * 10)
        _push(store, minute + 20, _check("fail"))

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (False, False)
        assert pair["window_counted"] == 1

    def test_too_few_counted_minutes_keep_a_marked_pair_marked(self):
        store = FreshnessStore()
        minute = _run(store, ["fail"] * 10)
        _push(store, minute + 20, _check("pass"))

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (True, False)

    def test_void_outcomes_count_neither_way(self):
        store = FreshnessStore()
        _run(store, ["pass"] * 5 + ["void"] * 5)
        assert _pair(store)["marked"] is False
        assert _pair(store)["window_counted"] == 5

        _run(store, ["fail"] * 3 + ["pass"] * 2 + ["void"] * 5, start=START + 10)
        assert _pair(store)["marked"] is True
        assert (_pair(store)["window_passed"], _pair(store)["window_counted"]) == (2, 5)

    def test_void_minutes_leave_a_newcomer_unjudged(self):
        store = FreshnessStore()
        _run(store, ["pass"] * 4 + ["void"] * 6)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (True, False)

    def test_each_chain_is_judged_on_its_own(self):
        store = FreshnessStore()
        for offset in range(10):
            _push(store, START + offset, _check("pass"), _check("fail", chain=OTHER_CHAIN))

        assert _pair(store)["marked"] is False
        assert _pair(store, chain=OTHER_CHAIN)["marked"] is True


NOEV = "0x" + "c3" * 20


def _old_file(tmp_path, save: dict[str, Any]) -> FreshnessStore:
    """A store loaded from ``save`` with what the service didn't keep before it marked
    indexers whose checks stop counting taken out, as a file written then would be."""
    older = json.loads(json.dumps(save))
    older.pop("answered", None)
    older.pop("last_answered", None)
    for pairs in older["chains"].values():
        for pair in pairs.values():
            pair.pop("last_counted", None)
            pair.pop("recent", None)
    path = tmp_path / "freshness.json"
    path.write_text(json.dumps(older))
    return FreshnessStore(load_state(str(path)))


class TestAnsweredMinutes:
    """An answered minute on a chain is one in which some check there passed or failed, so it
    went through the gateway. A pair silent for over 60 of them is marked, and a minute that
    isn't answered on a chain leaves every mark there as it was.
    """

    @pytest.mark.parametrize("silence", ["void", None], ids=["void", "left-out"])
    @pytest.mark.parametrize(("minutes", "marked"), [(56, False), (57, True)])
    def test_a_pair_with_under_5_counted_checks_in_60_answered_minutes_is_marked(
        self, silence, minutes, marked
    ):
        """Its last 5 counted checks fall in answered minutes 6 to 10, and the oldest is more
        than 60 answered minutes old after 57 more."""
        store = FreshnessStore()
        _run(store, ["pass"] * 10 + [silence] * minutes)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (marked, False)
        assert pair["last_counted_minute"] == minute_text(START + 9)
        recent = 4 if marked else 5
        assert (pair["recent_passed"], pair["recent_counted"]) == (recent, recent)

    def test_an_indexer_refusing_9_of_10_checks_and_failing_the_rest_is_marked(self):
        """1 counted check every 10 minutes kept its window from ever holding 5, so it was
        never judged and kept the mark it had."""
        store = FreshnessStore()
        minute = _run(store, ["pass"] * 10)
        _run(store, (["void"] * 9 + ["fail"]) * 6, start=minute)

        pair = _pair(store)
        assert (pair["marked"], pair["judged"]) == (True, False)
        assert (pair["recent_passed"], pair["recent_counted"]) == (0, 5)

    def test_a_marked_indexer_passing_now_and_then_clears_once_3_of_its_last_5_pass(self):
        store = FreshnessStore()
        minute = _run(store, ["fail"] * 10)

        marks = []
        for _ in range(3):
            minute = _run(store, ["void"] * 9 + ["pass"], start=minute)
            marks.append(_pair(store)["marked"])

        assert marks == [True, True, False]
        assert (_pair(store)["recent_passed"], _pair(store)["recent_counted"]) == (3, 5)

    def test_a_2_hour_gateway_outage_marks_nobody_new(self):
        """NOEV has no allocation old enough to check, so its no_evidence outcomes come from
        the prober alone and say nothing about the gateway."""
        store = FreshnessStore()
        minute = _run(store, ["pass"] * 10)
        for offset in range(120):
            _push(
                store,
                minute + offset,
                _check("void"),
                _check("void", indexer=OTHER),
                _check("no_evidence", indexer=NOEV),
            )

        assert store.stale_indexers(CHAIN, [INDEXER, OTHER, NOEV]) == {NOEV}

    def test_the_first_minute_back_does_not_mark_a_pair_whose_own_check_is_void(self):
        store = FreshnessStore()
        minute = _run(store, ["pass"] * 10)
        for offset in range(120):
            _push(store, minute + offset, _check("void"), _check("void", indexer=OTHER))

        _push(store, minute + 120, _check("void"), _check("pass", indexer=OTHER))

        assert store.stale_indexers(CHAIN, [INDEXER, OTHER]) == set()

    def test_another_chain_answering_does_not_age_a_pair(self):
        store = FreshnessStore()
        minute = _run(store, ["pass"] * 10)
        for offset in range(120):
            _push(store, minute + offset, _check("void"), _check("pass", chain=OTHER_CHAIN))

        assert _pair(store)["marked"] is False

    def test_a_pair_that_never_counted_starts_marked_as_before(self):
        store = FreshnessStore()
        _run(store, ["void"] * 120)

        pair = _pair(store)
        assert pair["marked"] is True
        assert pair["last_counted_minute"] is None

    @pytest.mark.parametrize("outcome", ["pass", "fail", "no_evidence"])
    def test_every_outcome_but_void_is_kept_with_the_chains_answered_count(self, outcome):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        store.apply(START, [_check(outcome), _check("pass", indexer=OTHER)], saves.append)
        store.apply(START + 1, [_check("void"), _check("pass", indexer=OTHER)], saves.append)

        assert saves[-1]["answered"] == {CHAIN: 2}
        pair = saves[-1]["chains"][CHAIN][INDEXER]
        assert pair["last_counted"] == START
        assert pair["recent"] == [{"passed": outcome == "pass", "answered": 1}]

    @pytest.mark.parametrize("outcome", ["no_evidence", "void"])
    def test_a_minute_without_a_pass_or_fail_is_not_answered(self, outcome):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        store.apply(START, [_check("pass")], saves.append)
        store.apply(START + 1, [_check(outcome, indexer=OTHER)], saves.append)

        assert saves[-1]["answered"] == {CHAIN: 1}


class TestStaleIndexers:
    def test_marked_and_unrecorded_indexers_are_stale(self):
        store = FreshnessStore()
        _run(store, ["fail"] * 5)
        unknown = "0x" + "c3" * 20

        assert store.stale_indexers(CHAIN, [INDEXER, OTHER, unknown]) == {INDEXER, unknown}
        assert store.stale_indexers(OTHER_CHAIN, [OTHER]) == {OTHER}

    def test_indexers_are_looked_up_in_lower_case(self):
        store = FreshnessStore()
        _run(store, ["pass"] * 5)

        assert store.stale_indexers(CHAIN, [INDEXER.upper().replace("0X", "0x")]) == set()

    def test_snapshot_lists_exempt_chains_even_without_indexers(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))

        chains = store.snapshot(Enforcement(frozenset({"amoy"}), 60, START))["chains"]

        assert {chain: body["enforced"] for chain, body in chains.items()} == {
            "amoy": False,
            CHAIN: True,
        }
        assert chains["amoy"]["indexers"] == {}


def _last_counted(store: FreshnessStore, chain: str = CHAIN) -> Optional[str]:
    """The last minute a check on ``chain`` counted, as GET /freshness reports it."""
    chains = store.snapshot()["chains"]
    return chains[chain]["last_counted_minute"] if chain in chains else None


class TestLastCountedMinute:
    @pytest.mark.parametrize("outcome", ["pass", "fail", "no_evidence"])
    def test_counted_outcome_sets_its_chains_minute(self, outcome):
        store = FreshnessStore()
        _push(store, START, _check(outcome), _check("void", chain=OTHER_CHAIN))

        assert _last_counted(store) == minute_text(START)
        assert _last_counted(store, OTHER_CHAIN) is None

    def test_void_outcomes_and_empty_pushes_leave_it_alone(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))
        _push(store, START + 1, _check("void"), _check("void", indexer=OTHER))
        _push(store, START + 2)

        assert _last_counted(store) == minute_text(START)
        assert store.last_minute == START + 2

    def test_1_counted_outcome_on_the_chain_is_enough(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))
        _push(store, START + 1, _check("void"), _check("fail", indexer=OTHER))

        assert _last_counted(store) == minute_text(START + 1)

    def test_forgotten_with_its_chain(self):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        store.apply(START, [_check("pass", chain=OTHER_CHAIN)], saves.append)
        store.apply(START + 28 * DAY, [_check("pass")], saves.append)

        assert _last_counted(store, OTHER_CHAIN) is None
        assert saves[-1]["last_counted"] == {CHAIN: START + 28 * DAY}


class TestLastAnsweredMinute:
    """The last minute in which a check on the chain passed or failed, so went through the
    gateway, which decides whether its marks and freshness scores are current."""

    @pytest.mark.parametrize("outcome", ["pass", "fail"])
    def test_a_pass_or_fail_sets_its_chains_minute(self, outcome):
        store = FreshnessStore()
        _push(store, START, _check(outcome), _check("no_evidence", chain=OTHER_CHAIN))

        assert store.last_answered_minute(CHAIN) == START
        assert store.last_answered_minute(OTHER_CHAIN) is None

    def test_no_evidence_voids_and_empty_pushes_leave_it_alone(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))
        _push(store, START + 1, _check("no_evidence"), _check("void", indexer=OTHER))
        _push(store, START + 2)

        assert store.last_answered_minute(CHAIN) == START
        assert _last_counted(store) == minute_text(START + 1)

    def test_a_chain_with_only_no_evidence_pushes_is_not_current(self):
        store = FreshnessStore()
        for minute in range(START, START + 10):
            _push(store, minute, _check("no_evidence"), _check("no_evidence", indexer=OTHER))

        body = store.snapshot(Enforcement(frozenset(), 60, START + 9))["chains"][CHAIN]

        assert store.last_answered_minute(CHAIN) is None
        assert (body["enforced"], body["enforcing"]) == (True, False)
        assert body["last_counted_minute"] == minute_text(START + 9)
        assert body["last_answered_minute"] is None

    def test_forgotten_with_its_chain(self):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        store.apply(START, [_check("pass", chain=OTHER_CHAIN)], saves.append)
        store.apply(START + 28 * DAY, [_check("pass")], saves.append)

        assert store.last_answered_minute(OTHER_CHAIN) is None
        assert saves[-1]["last_answered"] == {CHAIN: START + 28 * DAY}

    def test_read_without_waiting_for_the_lock(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))

        with store._lock:
            assert store.last_answered_minute(CHAIN) == START


class TestEnforcement:
    @pytest.mark.parametrize(
        ("last_counted", "current"),
        [(START + 100, True), (START + 40, True), (START + 39, False), (None, False)],
        ids=["this-minute", "at-the-limit", "past-the-limit", "never"],
    )
    def test_checks_are_current_up_to_the_age_limit(self, last_counted, current):
        assert Enforcement(frozenset({CHAIN}), 60, START + 100).current(last_counted) is current

    def test_a_minute_ahead_of_the_clock_is_current(self):
        assert Enforcement(frozenset({CHAIN}), 60, START).current(START + 3) is True

    def test_every_chain_but_the_exempt_ones_is_enforced(self):
        enforcement = Enforcement(frozenset({OTHER_CHAIN}), 60, START)

        assert enforcement.enforced(CHAIN) is True
        assert enforcement.enforced("a-chain-never-seen") is True
        assert enforcement.enforced(OTHER_CHAIN) is False

    def test_snapshot_reports_each_chains_enforcement_and_last_minutes(self):
        """CHAIN's check at START + 1 found no evidence, which counts but isn't an answer, so
        its marks stop being enforced 61 minutes after START."""
        store = FreshnessStore()
        _push(store, START, _check("pass"), _check("pass", chain=OTHER_CHAIN))
        _push(
            store,
            START + 1,
            _check("void"),
            _check("no_evidence", indexer=OTHER),
            _check("void", chain=OTHER_CHAIN),
        )
        exempt = frozenset({OTHER_CHAIN, "amoy"})

        def chains(now: int) -> dict[str, tuple[bool, bool, Optional[str], Optional[str]]]:
            report = store.snapshot(Enforcement(exempt, 60, now))["chains"]
            return {
                chain: (
                    body["enforced"],
                    body["enforcing"],
                    body["last_counted_minute"],
                    body["last_answered_minute"],
                )
                for chain, body in report.items()
            }

        assert chains(START + 60) == {
            CHAIN: (True, True, minute_text(START + 1), minute_text(START)),
            OTHER_CHAIN: (False, False, minute_text(START), minute_text(START)),
            "amoy": (False, False, None, None),
        }
        assert chains(START + 61)[CHAIN] == (
            True,
            False,
            minute_text(START + 1),
            minute_text(START),
        )

    def test_snapshot_without_enforcement_enforces_nothing(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))

        body = store.snapshot()["chains"][CHAIN]

        assert (body["enforced"], body["enforcing"]) == (False, False)
        assert body["last_counted_minute"] == minute_text(START)
        assert body["last_answered_minute"] == minute_text(START)


class TestPushes:
    def test_repeated_or_older_minute_is_ignored(self):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()

        assert store.apply(START + 1, [_check("pass")], saves.append) is True
        assert store.apply(START + 1, [_check("fail")], saves.append) is False
        assert store.apply(START, [_check("fail")], saves.append) is False

        pair = _pair(store)
        assert len(saves) == 1
        assert pair["last"]["outcome"] == "pass"
        assert (pair["passed_28d"], pair["counted_28d"]) == (1, 1)
        assert store.last_minute == START + 1

    def test_latest_outcome_is_reported_with_its_details(self):
        store = FreshnessStore()
        _push(store, START, _check("pass", deployment="QmOld", age_seconds=0.8))
        _push(
            store,
            START + 1,
            _check("void", reason="gateway_error", detail="internal error", deployment="QmNew"),
        )

        assert _pair(store)["last"] == {
            "minute": minute_text(START + 1),
            "outcome": "void",
            "reason": "gateway_error",
            "detail": "internal error",
            "deployment": "QmNew",
            "age_seconds": None,
        }

    def test_empty_store_reports_nothing(self):
        assert FreshnessStore().snapshot() == {"last_minute": None, "chains": {}}
        assert FreshnessStore().last_minute is None


class TestDailyCounts:
    def test_counting_starts_at_the_first_pass_or_fail(self):
        store = FreshnessStore()
        _run(store, ["no_evidence"] * 3 + ["fail"] + ["no_evidence"] * 2 + ["pass", "void"])

        pair = _pair(store)
        assert (pair["passed_28d"], pair["counted_28d"]) == (1, 4)
        assert pair["share_28d"] == 0.25

    def test_share_is_null_before_anything_counts(self):
        store = FreshnessStore()
        _run(store, ["no_evidence", "void"])

        pair = _pair(store)
        assert (pair["passed_28d"], pair["counted_28d"], pair["share_28d"]) == (0, 0, None)

    def test_minutes_with_no_outcome_add_nothing(self):
        store = FreshnessStore()
        _run(store, ["pass", None, None, "fail"])

        assert _pair(store)["counted_28d"] == 2

    def test_days_older_than_28_days_are_dropped(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))
        _push(store, START + 27 * DAY, _check("fail"))
        assert (_pair(store)["passed_28d"], _pair(store)["counted_28d"]) == (1, 2)

        _push(store, START + 28 * DAY, _check("fail"))
        assert (_pair(store)["passed_28d"], _pair(store)["counted_28d"]) == (0, 2)

    def test_pair_with_no_outcome_for_28_days_is_dropped(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"), _check("pass", indexer=OTHER))
        _push(store, START + 28 * DAY - 1, _check("pass", indexer=OTHER))
        assert INDEXER in store.snapshot()["chains"][CHAIN]["indexers"]

        _push(store, START + 28 * DAY, _check("pass", indexer=OTHER))
        assert INDEXER not in store.snapshot()["chains"][CHAIN]["indexers"]

    def test_chain_left_with_no_pairs_is_dropped(self):
        store = FreshnessStore()
        _push(store, START, _check("pass", chain=OTHER_CHAIN))
        _push(store, START + 28 * DAY, _check("pass"))

        assert list(store.snapshot()["chains"]) == [CHAIN]


class TestSavedFile:
    def test_state_survives_a_save_and_load(self, tmp_path):
        path = tmp_path / "freshness.json"

        def save(payload: dict[str, Any]) -> None:
            path.write_text(json.dumps(payload))

        store = FreshnessStore()
        for offset, outcome in enumerate(["no_evidence", "pass", "fail", "void", "pass", "pass"]):
            store.apply(START + offset, [_check(outcome), _check("fail", indexer=OTHER)], save)
        assert json.loads(path.read_text())["version"] == 1
        assert json.loads(path.read_text())["last_counted"] == {CHAIN: START + 5}
        assert json.loads(path.read_text())["last_answered"] == {CHAIN: START + 5}

        reloaded = FreshnessStore(load_state(str(path)))

        assert reloaded.snapshot() == store.snapshot()
        assert reloaded.last_minute == START + 5
        assert reloaded.last_answered_minute(CHAIN) == START + 5
        assert reloaded.stale_indexers(CHAIN, [INDEXER, OTHER]) == {OTHER}
        # The reloaded store keeps judging from where the saved one stopped.
        reloaded.apply(START + 6, [_check("pass")], _no_save)
        store.apply(START + 6, [_check("pass")], _no_save)
        assert reloaded.snapshot() == store.snapshot()

    def test_file_saved_before_the_last_counted_minute_was_kept_loads(self, tmp_path, caplog):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        for offset in range(5):
            store.apply(
                START + offset, [_check("pass"), _check("fail", indexer=OTHER)], saves.append
            )
        older = {
            key: value
            for key, value in saves[-1].items()
            if key not in ("last_counted", "last_answered")
        }
        path = tmp_path / "freshness.json"
        path.write_text(json.dumps(older))

        with caplog.at_level(logging.WARNING, logger="iisa.freshness"):
            reloaded = FreshnessStore(load_state(str(path)))

        assert caplog.records == []
        assert reloaded.last_minute == START + 4
        assert reloaded.stale_indexers(CHAIN, [INDEXER, OTHER]) == {OTHER}
        assert reloaded.last_answered_minute(CHAIN) is None
        reloaded.apply(START + 5, [_check("pass")], _no_save)
        assert reloaded.last_answered_minute(CHAIN) == START + 5
        assert _last_counted(reloaded) == minute_text(START + 5)

    def test_an_older_file_takes_each_chains_last_answered_minute_from_its_last_counted(
        self, tmp_path
    ):
        """So the chain stays current as it was until its first answered minute, even when its
        last counted check found no evidence."""
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        store.apply(START, [_check("pass")], saves.append)
        store.apply(START + 1, [_check("no_evidence")], saves.append)

        reloaded = _old_file(tmp_path, saves[-1])

        assert reloaded.last_answered_minute(CHAIN) == START + 1
        reloaded.apply(START + 2, [_check("no_evidence")], _no_save)
        assert reloaded.last_answered_minute(CHAIN) == START + 1
        reloaded.apply(START + 3, [_check("fail")], _no_save)
        assert reloaded.last_answered_minute(CHAIN) == START + 3

    def test_an_older_file_loads_and_marks_nobody_who_was_fine(self, tmp_path, caplog):
        """Each pair's recent outcomes are seeded from the counted minutes in its window, so
        INDEXER, whose window soon holds too few, is judged on the 5 passes it had."""
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        for minute in range(START, START + 10):
            store.apply(minute, [_check("pass"), _check("pass", indexer=OTHER)], saves.append)

        with caplog.at_level(logging.WARNING, logger="iisa.freshness"):
            reloaded = _old_file(tmp_path, saves[-1])
        _run(reloaded, ["void"] * 8, start=START + 10)

        assert caplog.records == []
        assert reloaded.stale_indexers(CHAIN, [INDEXER, OTHER]) == set()
        assert _pair(reloaded)["window_counted"] == 2
        assert _pair(reloaded)["last_counted_minute"] == minute_text(START + 9)

    def test_an_older_files_pair_with_nothing_counted_in_its_window_is_marked_in_time(
        self, tmp_path
    ):
        """It was fine when saved, so it starts from 5 passes, but its checks stay void, so it
        is marked once those are over 60 answered minutes old rather than kept fine for ever."""
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        for minute in range(START, START + 10):
            store.apply(minute, [_check("pass"), _check("pass", indexer=OTHER)], saves.append)
        for minute in range(START + 10, START + 21):
            store.apply(minute, [_check("void")], saves.append)

        reloaded = _old_file(tmp_path, saves[-1])
        assert _pair(reloaded)["marked"] is False
        after = _run(reloaded, ["void"] * RECENT_ANSWERED_MINUTES, start=START + 21)
        assert _pair(reloaded)["marked"] is False
        _run(reloaded, ["void"], start=after)

        assert _pair(reloaded)["marked"] is True

    def test_an_older_files_pair_refusing_most_payments_stays_pickable(self, tmp_path):
        """INDEXER refuses 7 of every 10 payments, which are void, and passes the rest, so its
        window never holds 5 counted minutes, and it was fine when saved. It loads with its 3
        passes in the window and passes before them up to 5, so it isn't marked after loading.
        """
        cycle = ["void", "void", "pass", "void", "void", "void", "pass", "void", "pass", "void"]
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        for offset, outcome in enumerate(["pass"] * 30 + cycle * 3):
            store.apply(
                START + offset, [_check(outcome), _check("pass", indexer=OTHER)], saves.append
            )
        assert _pair(store)["marked"] is False

        reloaded = _old_file(tmp_path, saves[-1])
        marks = []
        for offset, outcome in enumerate(cycle):
            _push(reloaded, START + 63 + offset, _check(outcome), _check("pass", indexer=OTHER))
            marks.append(_pair(reloaded)["marked"])

        assert marks == [False] * len(cycle)

    def test_an_older_file_saved_straight_after_a_gateway_outage_marks_nobody(self, tmp_path):
        """Every check was void for the last 8 minutes before the save, leaving 2 counted
        minutes in each window, and nobody was marked then, nor while checks pass again."""
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        for minute in range(START, START + 30):
            store.apply(minute, [_check("pass"), _check("pass", indexer=OTHER)], saves.append)
        for minute in range(START + 30, START + 38):
            store.apply(minute, [_check("void"), _check("void", indexer=OTHER)], saves.append)
        assert store.stale_indexers(CHAIN, [INDEXER, OTHER]) == set()

        reloaded = _old_file(tmp_path, saves[-1])
        stale = []
        for minute in range(START + 41, START + 47):
            _push(reloaded, minute, _check("pass"), _check("pass", indexer=OTHER))
            stale.append(reloaded.stale_indexers(CHAIN, [INDEXER, OTHER]))

        assert stale == [set()] * 6

    def test_an_older_files_marked_pair_starts_from_its_own_outcomes_only(self, tmp_path):
        """INDEXER failed until the outage, so it was marked when saved, and only passes added
        for a pair that was fine could clear it."""
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        for minute in range(START, START + 30):
            store.apply(minute, [_check("fail"), _check("pass", indexer=OTHER)], saves.append)
        for minute in range(START + 30, START + 38):
            store.apply(minute, [_check("void"), _check("void", indexer=OTHER)], saves.append)

        reloaded = _old_file(tmp_path, saves[-1])
        _push(reloaded, START + 41, _check("void"), _check("pass", indexer=OTHER))

        assert (_pair(reloaded)["recent_counted"], _pair(reloaded)["recent_passed"]) == (2, 0)
        assert _pair(reloaded)["marked"] is True
        assert _pair(reloaded, OTHER)["marked"] is False

    def test_saved_file_keeps_only_the_window(self):
        saves: list[dict[str, Any]] = []
        store = FreshnessStore()
        store.apply(START, [_check("pass")], saves.append)
        store.apply(START + 5, [_check("pass")], saves.append)
        store.apply(START + 12, [_check("fail")], saves.append)

        saved = saves[-1]
        assert sorted(saved["received"]) == [START + 5, START + 12]
        assert saved["chains"][CHAIN][INDEXER]["minutes"] == {
            str(START + 5): "pass",
            str(START + 12): "fail",
        }

    def test_missing_file_gives_empty_state_without_a_warning(self, tmp_path, caplog):
        with caplog.at_level(logging.DEBUG, logger="iisa.freshness"):
            state = load_state(str(tmp_path / "absent.json"))

        assert state.last_minute is None and state.chains == {}
        assert [r.levelno for r in caplog.records] == [logging.DEBUG]

    @pytest.mark.parametrize(
        "content",
        [
            b"not json",
            b"\xff\xfe",
            b"[]",
            b'{"version": 2, "last_minute": null, "received": [], "chains": {}}',
            b'{"version": 1, "last_minute": "soon", "received": [], "chains": {}}',
            b'{"version": 1, "chains": {"matic": {"0xaa": {"marked": true}}}}',
        ],
        ids=["not-json", "not-text", "not-object", "newer-version", "bad-minute", "no-last"],
    )
    def test_malformed_file_gives_empty_state_with_a_warning(self, tmp_path, caplog, content):
        path = tmp_path / "freshness.json"
        path.write_bytes(content)

        with caplog.at_level(logging.WARNING, logger="iisa.freshness"):
            state = load_state(str(path))

        assert state.last_minute is None and state.chains == {}
        assert len(caplog.records) == 1
        assert "malformed" in caplog.records[0].getMessage()

    def test_unreadable_file_gives_empty_state_with_a_warning(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="iisa.freshness"):
            state = load_state(str(tmp_path))

        assert state.last_minute is None
        assert "Cannot read freshness file" in caplog.records[0].getMessage()
