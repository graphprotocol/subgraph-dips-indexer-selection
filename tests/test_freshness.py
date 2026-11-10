"""Tests for the freshness state: the stale mark, the 28-day counts and the saved file."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import pytest

from iisa.freshness import (
    Check,
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

    def test_snapshot_lists_enforced_chains_even_without_indexers(self):
        store = FreshnessStore()
        _push(store, START, _check("pass"))

        chains = store.snapshot(frozenset({"amoy"}))["chains"]

        assert {chain: body["enforced"] for chain, body in chains.items()} == {
            "amoy": True,
            CHAIN: False,
        }
        assert chains["amoy"]["indexers"] == {}


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

        reloaded = FreshnessStore(load_state(str(path)))

        assert reloaded.snapshot() == store.snapshot()
        assert reloaded.last_minute == START + 5
        # The reloaded store keeps judging from where the saved one stopped.
        reloaded.apply(START + 6, [_check("pass")], _no_save)
        store.apply(START + 6, [_check("pass")], _no_save)
        assert reloaded.snapshot() == store.snapshot()

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
