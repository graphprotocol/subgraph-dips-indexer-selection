"""A score run that can measure nothing carries an earlier run's measurements over.

When the replay of gateway queries fails, the run has no latency, uptime or success scores of
its own. It used to push the fallback scores, the same for every indexer. Now it reads today's
prices and indexer list as before, and takes each indexer's quality, stake and fees from the
scores iisa holds, where both were measured less than 60 hours ago. An indexer with nothing
recent is left out, and the fallback scores go out only when nothing at all can be carried over.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import main  # noqa: E402
import processing  # noqa: E402
from iisa_client import IISAPushError  # noqa: E402
from redpanda import RedpandaProvider  # noqa: E402
from test_run_without_stake import (  # noqa: E402
    INDEXERS,
    NOW,
    REJECTED_KEY,
    URLS,
    WORKING,
    FakeIisa,
    _first,
    _pricing,
    _provider,
    _queries,
    _run_job,
)

A, B, C = INDEXERS
MEASURED_AT = "quality_measured_at"
READ_AT = "stake_read_at"
STAKE_COLUMNS = ["stake_to_fees", "total_query_fees", "last_known_slashable_stake"]
QUALITY_COLUMNS = [
    "lat_lin_reg_coefficient",
    "lat_coefficient_std_error",
    "lat_coefficient_upper_bound",
    "lat_normalized_score",
    "latency_ms",
    "uptime_score",
    "observed_duration_seconds",
    "uptime_duration_seconds",
    "success_rate",
    "uptime_by_chain",
    "success_rate_by_chain",
    "org",
    "dst_lat",
    "dst_lon",
    "query_count",
    "scoring_mode",
]
# The 3 quality columns iisa hands back under the names its selector uses.
SERVED_AS = {
    "lat_coefficient_upper_bound": "Latency Coefficient + Error Confidence Interval",
    "success_rate": "average_status",
    "lat_normalized_score": "norm_lat_lin_reg_coefficient",
}
DAY = timedelta(days=1)


class ServingIisa(FakeIisa):
    """Keeps what the job pushes and hands it back the way the real service does on GET
    /scores: 3 quality columns renamed, and 2 columns of its own added.
    """

    def get(self, _url: str, _token: Optional[str]) -> dict[str, Any]:
        if self.failure is not None:
            raise self.failure
        served = []
        for row in self.held:
            renamed = {SERVED_AS.get(name, name): value for name, value in row.items()}
            renamed["% up_x"] = (row.get("uptime_score") or 0) * 100
            renamed["destination_loc"] = f"{row.get('dst_lat') or 0},{row.get('dst_lon') or 0}"
            served.append(renamed)
        return {"computed_at": None, "count": len(served), "scores": served}


@pytest.fixture
def iisa(monkeypatch: pytest.MonkeyPatch) -> ServingIisa:
    fake = ServingIisa()
    monkeypatch.setattr("redpanda.post_scores", fake.post)
    monkeypatch.setattr("redpanda.get_scores", fake.get)
    monkeypatch.setattr(processing, "fetch_dips_info", _pricing)
    # With no minimum set, the real version check asks no indexer and only adds its columns.
    monkeypatch.delenv("MIN_GRAPH_NODE_VERSION", raising=False)
    return fake


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, datetime]:
    """The time the job sees; move it with clock["now"]."""
    clock = {"now": NOW}
    for target in ("redpanda.utc_now", "main.utc_now", "processing.utc_now"):
        monkeypatch.setattr(target, lambda: clock["now"])
    return clock


def _good_run(monkeypatch: pytest.MonkeyPatch, iisa: ServingIisa) -> list[dict[str, Any]]:
    """Run the whole job on a day everything works; return the rows it pushed."""
    initial = pd.DataFrame(
        {"deployment_hash": ["QmTestDeployment"] * 3, "indexer": INDEXERS, "num_rows": [40] * 3}
    )
    monkeypatch.setattr(RedpandaProvider, "fetch_initial_query_results", lambda self, *_a: initial)
    monkeypatch.setattr(
        RedpandaProvider, "fetch_combined_query_results", lambda self, *_a: _queries()
    )
    return _run_job(monkeypatch, iisa, WORKING)


def _failed_run(
    monkeypatch: pytest.MonkeyPatch, iisa: ServingIisa, urls: dict[str, str] = URLS
) -> list[dict[str, Any]]:
    """Run the whole job on a day the replay cannot be read; return the rows it pushed."""

    def no_replay(self: RedpandaProvider, *_a: Any) -> pd.DataFrame:
        raise RuntimeError("Redpanda unreachable")

    monkeypatch.setattr(RedpandaProvider, "fetch_initial_query_results", no_replay)
    monkeypatch.setattr(main, "RedpandaProvider", _provider)
    monkeypatch.setattr(main, "validate_geoip_databases", lambda: False)
    monkeypatch.setattr(main, "discover_indexers_from_network_subgraph", lambda _url: urls)
    assert main.run_scoring() is True
    return iisa.held


def _by_indexer(rows: list[dict[str, Any]], columns: list[str]) -> dict[str, list[Any]]:
    return {row["indexer"]: [row[column] for column in columns] for row in rows}


def _is_fallback(rows: list[dict[str, Any]]) -> bool:
    """Whether rows are the fallback scores: equal for everyone, with nothing measured."""
    return all(
        row["norm_stake_to_fees"] == 0.5 and row["success_rate"] == 1.0 and row[MEASURED_AT] is None
        for row in rows
    )


class TestAFailedRun:
    """The day after a good run, a run whose replay fails carries that run's scores over."""

    def test_quality_stake_and_fees_are_the_last_good_runs(self, monkeypatch, iisa, clock, caplog):
        measured = _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY

        with caplog.at_level("INFO"):
            pushed = _failed_run(monkeypatch, iisa)

        columns = [*QUALITY_COLUMNS, *STAKE_COLUMNS, READ_AT, MEASURED_AT]
        assert _by_indexer(pushed, columns) == _by_indexer(measured, columns)
        assert [row["success_rate"] for row in pushed] == [1.0, 0.75, 0.5]
        assert [row["success_rate_by_chain"] for row in pushed] == [
            {"arbitrum": 1.0},
            {"arbitrum": 0.75},
            {"arbitrum": 0.5},
        ]
        assert {pd.Timestamp(row[MEASURED_AT]) for row in pushed} == {NOW}
        assert any("mode=carried_over" in r.getMessage() for r in caplog.records)
        warning = _first(caplog.records, "WARNING", "carried over for 3 of today's 3 indexer(s)")
        assert warning is not None
        assert warning.getMessage().count(f"{NOW.isoformat()} (24 hours before this run)") == 2

    def test_the_rows_have_the_columns_a_good_run_pushes(self, monkeypatch, iisa, clock):
        measured = _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY

        pushed = _failed_run(monkeypatch, iisa)

        assert [set(row) ^ set(measured[0]) for row in pushed] == [set()] * 3
        assert all("norm_stake_to_fees" not in row for row in pushed)

    def test_prices_and_urls_are_read_today(self, monkeypatch, iisa, clock):
        _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY
        dearer = _pricing(URLS).assign(dips_min_grt_per_30_days='{"arbitrum-one": 900}')
        monkeypatch.setattr(processing, "fetch_dips_info", lambda _urls: dearer)
        moved = {**URLS, A: "https://moved.example/"}

        pushed = _failed_run(monkeypatch, iisa, moved)

        assert {row["dips_min_grt_per_30_days"] for row in pushed} == {'{"arbitrum-one": 900}'}
        assert _by_indexer(pushed, ["url"])[A] == ["https://moved.example/"]

    def test_two_failed_days_carry_over_and_the_third_falls_back(self, monkeypatch, iisa, clock):
        """Day 1 measures; days 2 and 3 carry it over, keeping its times; day 4 is 72 hours on."""
        _good_run(monkeypatch, iisa)
        carried = []
        for day in (2, 3, 4):
            clock["now"] = NOW + (day - 1) * DAY
            pushed = _failed_run(monkeypatch, iisa)
            carried.append({pd.Timestamp(row[MEASURED_AT]) for row in pushed} == {NOW})

        assert carried == [True, True, False]
        assert _is_fallback(iisa.held)

    def test_a_carried_over_reading_serves_a_later_failed_stake_lookup(
        self, monkeypatch, iisa, clock
    ):
        """Day 2 carries day 1's scores over; day 3 measures quality but cannot read stake, and
        finds day 1's reading still there, 48 hours old.
        """
        measured = _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY
        _failed_run(monkeypatch, iisa)
        clock["now"] = NOW + 2 * DAY

        initial = pd.DataFrame(
            {"deployment_hash": ["QmTestDeployment"] * 3, "indexer": INDEXERS, "num_rows": [40] * 3}
        )
        monkeypatch.setattr(
            RedpandaProvider, "fetch_initial_query_results", lambda self, *_a: initial
        )
        pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        columns = [*STAKE_COLUMNS, READ_AT]
        assert _by_indexer(pushed, columns) == _by_indexer(measured, columns)
        assert {pd.Timestamp(row[MEASURED_AT]) for row in pushed} == {NOW + 2 * DAY}


class TestWhoIsCarriedOver:
    """Only an indexer listed today, with recent quality and recent stake, is carried over."""

    def test_an_indexer_with_nothing_held_is_left_out(self, monkeypatch, iisa, clock, caplog):
        """c joined after the good run. No run has measured it, so it gets no score at all."""
        _good_run(monkeypatch, iisa)
        iisa.held = [row for row in iisa.held if row["indexer"] != C]
        clock["now"] = NOW + DAY

        with caplog.at_level("INFO"):
            pushed = _failed_run(monkeypatch, iisa)

        assert [row["indexer"] for row in pushed] == [A, B]
        assert _first(caplog.records, "WARNING", "carried over for 2 of today's 3 indexer(s)")

    def test_an_indexer_no_longer_listed_is_dropped(self, monkeypatch, iisa, clock):
        _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY

        pushed = _failed_run(monkeypatch, iisa, {A: URLS[A], B: URLS[B]})

        assert [row["indexer"] for row in pushed] == [A, B]
        assert {pd.Timestamp(row[MEASURED_AT]) for row in pushed} == {NOW}

    @pytest.mark.parametrize("stale", [MEASURED_AT, READ_AT])
    def test_an_indexer_with_quality_or_stake_too_old_is_left_out(
        self, monkeypatch, iisa, clock, stale
    ):
        _good_run(monkeypatch, iisa)
        long_ago = (NOW - 2 * DAY).isoformat()
        iisa.held = [{**row, stale: long_ago} if row["indexer"] == C else row for row in iisa.held]
        clock["now"] = NOW + DAY

        pushed = _failed_run(monkeypatch, iisa)

        assert [row["indexer"] for row in pushed] == [A, B]


class TestNothingToCarryOver:
    """With nothing usable held, the run pushes the fallback scores exactly as it used to."""

    def test_an_empty_service_gets_the_fallback_scores(self, monkeypatch, iisa, clock, caplog):
        with caplog.at_level("INFO"):
            pushed = _failed_run(monkeypatch, iisa)

        assert [row["indexer"] for row in pushed] == INDEXERS and _is_fallback(pushed)
        assert any("mode=degraded" in r.getMessage() for r in caplog.records)

    def test_fallback_scores_are_never_carried_over(self, monkeypatch, iisa, clock):
        fallback = _failed_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY

        pushed = _failed_run(monkeypatch, iisa)

        assert _is_fallback(fallback) and _is_fallback(pushed)

    def test_scores_pushed_before_measure_times_were_recorded_are_not_carried_over(
        self, monkeypatch, iisa, clock
    ):
        _good_run(monkeypatch, iisa)
        iisa.held = [{k: v for k, v in row.items() if k != MEASURED_AT} for row in iisa.held]
        clock["now"] = NOW + DAY

        assert _is_fallback(_failed_run(monkeypatch, iisa))

    @pytest.mark.parametrize(
        "failure",
        [IISAPushError("GET /scores failed after 10 attempts"), ValueError("not JSON")],
        ids=["unreachable", "not-json"],
    )
    def test_a_failed_read_of_the_held_scores_gives_the_fallback_scores(
        self, monkeypatch, iisa, clock, caplog, failure
    ):
        _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY
        iisa.failure = failure

        with caplog.at_level("INFO"):
            pushed = _failed_run(monkeypatch, iisa)

        assert _is_fallback(pushed)
        assert _first(caplog.records, "WARNING", "Could not read the scores iisa holds")
        assert _first(caplog.records, "WARNING", "holds nothing recent enough") is None

    def test_a_failure_while_carrying_over_gives_the_fallback_scores(
        self, monkeypatch, iisa, clock, caplog
    ):
        def broken(*_a: Any) -> pd.DataFrame:
            raise RuntimeError("could not build the scores")

        _good_run(monkeypatch, iisa)
        clock["now"] = NOW + DAY
        monkeypatch.setattr(main, "compute_carried_over_scores", broken)

        with caplog.at_level("INFO"):
            pushed = _failed_run(monkeypatch, iisa)

        assert _is_fallback(pushed)
        error = _first(caplog.records, "ERROR", "Carrying scores over failed")
        assert error is not None and error.exc_info is not None


class TestCarriedOver:
    """Which held rows count, whatever shape the reply is in."""

    @staticmethod
    def _row(indexer: str, **changes: Any) -> dict[str, Any]:
        recent = (NOW - timedelta(hours=1)).isoformat()
        row = {
            "indexer": indexer,
            "uptime_score": 0.9,
            "average_status": 0.8,
            "last_known_slashable_stake": 1_000.0,
            "total_query_fees": 10.0,
            READ_AT: recent,
            MEASURED_AT: recent,
        }
        return {**row, **changes}

    @pytest.mark.parametrize(
        "reply", [None, [], {}, {"scores": None}, {"scores": "rows"}, {"scores": []}]
    )
    def test_a_reply_without_rows_gives_nothing(self, reply):
        from carry_over import carried_over

        assert carried_over(reply, INDEXERS, NOW).empty

    def test_unusable_rows_are_left_out(self):
        from carry_over import carried_over

        d, e, f = ("0x" + c * 40 for c in "def")
        rows = [
            self._row(A),
            self._row(B, **{MEASURED_AT: (NOW + timedelta(hours=1)).isoformat()}),
            self._row(C, **{MEASURED_AT: 1_700_000_000}),
            self._row(d, **{MEASURED_AT: "yesterday"}),
            self._row(e),
            self._row(e),
            self._row(7),
            self._row(f, last_known_slashable_stake="lots"),
            "not a row",
        ]

        carried = carried_over({"scores": rows}, [A, B, C, d, e, f], NOW)

        assert carried["indexer"].tolist() == [A]

    def test_a_quality_column_is_read_under_either_name(self):
        """success_rate comes back from iisa as average_status; a reply that kept the pushed
        name must work too.
        """
        from carry_over import carried_over

        as_served = self._row(A)
        as_pushed = {**self._row(B), "success_rate": 0.7}
        del as_pushed["average_status"]

        carried = carried_over({"scores": [as_served, as_pushed]}, [A, B], NOW)

        assert carried.set_index("indexer")["success_rate"].to_dict() == {A: 0.8, B: 0.7}

    def test_latency_in_milliseconds_is_carried_over(self):
        from carry_over import carried_over

        rows = [self._row(A, latency_ms=182.5), self._row(B)]

        carried = carried_over({"scores": rows}, [A, B], NOW).set_index("indexer")

        assert carried.loc[A, "latency_ms"] == 182.5
        assert pd.isna(carried.loc[B, "latency_ms"])
