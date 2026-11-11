"""A score run whose stake lookup fails still ships the quality scores it measured.

Stake comes from the network subgraph, read near the end of the run, after the long replay of
gateway queries. When that read fails the run should carry on and push its real latency,
uptime and success scores, rather than throw them all away for the fallback's equal neutral
scores. For stake and query fees it reuses what an earlier run read, if that was less than 60
hours ago, and otherwise sends them as missing.
"""

import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import main  # noqa: E402
import processing  # noqa: E402
from iisa_client import IISAPushError  # noqa: E402
from processing import compute_all_scores, merge_and_prepare_dataframes  # noqa: E402
from redpanda import RedpandaProvider  # noqa: E402

INDEXERS = ["0x" + c * 40 for c in "abc"]
URLS = {indexer: f"https://{indexer[2:8]}.example/" for indexer in INDEXERS}
STAKE_COLUMNS = ["stake_to_fees", "total_query_fees", "last_known_slashable_stake"]
READ_AT = "stake_read_at"
QUALITY_COLUMNS = ["uptime_score", "success_rate", "lat_lin_reg_coefficient", "query_count"]
STAKE = [{"id": indexer, "stakedTokens": "1000000", "lockedTokens": "0"} for indexer in INDEXERS]
REJECTED_KEY = {"errors": [{"message": "auth error: API key not found"}]}
WORKING = {"data": {"indexers": STAKE}}
NOW = datetime(2026, 11, 11, 11, 30, tzinfo=timezone.utc)
HELD_STAKE = [500_000.0, 600_000.0, 700_000.0]
HELD_FEES = [50.0, 0.0, 70.0]


def _queries() -> pd.DataFrame:
    """40 queries per indexer a minute apart: a answers all, b fails 1 in 4 and c 1 in 2."""
    fails_every = {INDEXERS[0]: 0, INDEXERS[1]: 4, INDEXERS[2]: 2}
    return pd.DataFrame(
        [
            {
                "query_id": f"q-{indexer[2:8]}-{i:03d}-LAX",
                "deployment_hash": "QmTestDeployment",
                "fee": 1.0,
                "timestamp": pd.Timestamp("2026-05-01") + pd.Timedelta(minutes=i),
                "blocks_behind": 0,
                "response_time_ms": 100.0,
                "indexer": indexer,
                "status": "Timeout" if every and i % every == 0 else "200 OK",
                "day_partition": "2026-05-01",
                "subgraph_network": "arbitrum",
                "url": URLS[indexer],
            }
            for indexer, every in fails_every.items()
            for i in range(40)
        ]
    )


def _provider() -> RedpandaProvider:
    """A real provider whose gateway-query replay is stood in for, with fees from that replay."""
    with patch.dict(
        os.environ,
        {
            "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
            "GRAPH_NETWORK_SUBGRAPH_URL": "http://graph-node:8000/subgraphs/name/graph-network",
            "IISA_API_URL": "http://iisa:8080",
        },
    ):
        provider = RedpandaProvider()
    provider._fees_per_indexer = {INDEXERS[0]: 10.0, INDEXERS[1]: 20.0}
    return provider


def _subgraph_reply(body: dict[str, Any]) -> MagicMock:
    reply = MagicMock()
    reply.status_code = 200
    reply.json.return_value = body
    return reply


def _pricing(_urls: dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "indexer": INDEXERS,
            "dips_info_available": [True] * 3,
            "dips_min_grt_per_30_days": ['{"arbitrum-one": 100}'] * 3,
            "dips_min_grt_per_billion_entities_per_30_days": [None] * 3,
            "dips_supported_networks": ['["arbitrum-one"]'] * 3,
        }
    )


@pytest.fixture
def replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stand in for the replay, the DIPs price probes and the graph-node version check."""
    initial = pd.DataFrame(
        {"deployment_hash": ["QmTestDeployment"] * 3, "indexer": INDEXERS, "num_rows": [40] * 3}
    )
    monkeypatch.setattr(RedpandaProvider, "fetch_initial_query_results", lambda self, *_a: initial)
    monkeypatch.setattr(
        RedpandaProvider, "fetch_combined_query_results", lambda self, *_a: _queries()
    )
    monkeypatch.setattr(processing, "fetch_dips_info", _pricing)
    monkeypatch.setattr(processing, "fetch_and_filter_graph_node_versions", lambda df, _u: df)


def _scores(stake_reply: dict[str, Any]) -> pd.DataFrame:
    """Run the score computation without GeoIP, the stake read answered by stake_reply."""
    with patch("subgraph.requests.post", return_value=_subgraph_reply(stake_reply)):
        return compute_all_scores(
            provider=_provider(),
            indexer_urls=URLS,
            start_date=date(2026, 5, 1),
            num_days=28,
            target_rows=20_000_000,
            geoip_available=False,
        ).set_index("indexer")


class FakeIisa:
    """Stands in for iisa: keeps what the job pushes and answers GET /scores with it.

    The real service renames some quality columns in that reply, but sends the stake columns
    and their read time back exactly as they were pushed, and those are all a reuse reads.
    """

    def __init__(self) -> None:
        self.held: list[dict[str, Any]] = []
        self.failure: Optional[Exception] = None

    def post(self, _url: str, _token: Optional[str], rows: list[dict[str, Any]]) -> None:
        self.held = rows

    def get(self, _url: str, _token: Optional[str]) -> dict[str, Any]:
        if self.failure is not None:
            raise self.failure
        return {"computed_at": None, "count": len(self.held), "scores": self.held}


@pytest.fixture
def iisa(monkeypatch: pytest.MonkeyPatch) -> FakeIisa:
    fake = FakeIisa()
    monkeypatch.setattr("redpanda.post_scores", fake.post)
    monkeypatch.setattr("redpanda.get_scores", fake.get, raising=False)
    return fake


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, datetime]:
    """The time the job sees when it reads, reuses or reports stake; move it with clock["now"]."""
    clock = {"now": NOW}
    for target in ("redpanda.utc_now", "main.utc_now", "processing.utc_now"):
        monkeypatch.setattr(target, lambda: clock["now"], raising=False)
    return clock


def _held(read_at: Optional[datetime], indexers: list[str] = INDEXERS) -> list[dict[str, Any]]:
    """Rows as iisa holds them after an earlier run read stake at read_at: b had no fees."""
    return [
        {
            "indexer": indexer,
            "success_rate": 0.2,
            "last_known_slashable_stake": stake,
            "total_query_fees": fees,
            "stake_to_fees": stake / fees if fees else None,
            READ_AT: read_at.isoformat() if read_at else None,
        }
        for indexer, stake, fees in zip(INDEXERS, HELD_STAKE, HELD_FEES)
        if indexer in indexers
    ]


def _held_stake(indexers: list[str] = INDEXERS) -> dict[str, list[Optional[float]]]:
    """The stake, fees and ratio _held gives each of indexers, in STAKE_COLUMNS order."""
    return {
        indexer: [stake / fees if fees else None, fees, stake]
        for indexer, stake, fees in zip(INDEXERS, HELD_STAKE, HELD_FEES)
        if indexer in indexers
    }


def _pushed_stake(rows: list[dict[str, Any]]) -> dict[str, list[Optional[float]]]:
    return {row["indexer"]: [row[column] for column in STAKE_COLUMNS] for row in rows}


def _pushed_read_at(rows: list[dict[str, Any]]) -> dict[str, Optional[pd.Timestamp]]:
    return {row["indexer"]: pd.Timestamp(row[READ_AT]) if row[READ_AT] else None for row in rows}


def _run_job(
    monkeypatch: pytest.MonkeyPatch, iisa: FakeIisa, stake_reply: dict[str, Any]
) -> list[dict[str, Any]]:
    """Run the whole job without GeoIP, the stake read answered by stake_reply; return the rows
    it pushed to iisa, which iisa then holds for the next run.
    """
    monkeypatch.setattr(main, "RedpandaProvider", _provider)
    monkeypatch.setattr(main, "validate_geoip_databases", lambda: False)
    monkeypatch.setattr(main, "discover_indexers_from_network_subgraph", lambda _url: URLS)
    with patch("subgraph.requests.post", return_value=_subgraph_reply(stake_reply)):
        assert main.run_scoring() is True
    return iisa.held


def _failed_stake_lookup() -> pd.DataFrame:
    with patch("subgraph.requests.post", return_value=_subgraph_reply(REJECTED_KEY)):
        return _provider().fetch_stake_to_fees()


@pytest.mark.usefixtures("iisa")
class TestFailedStakeLookup:
    """With nothing to reuse, a failed lookup leaves stake missing and keeps today's scores."""

    @pytest.mark.parametrize("drop_missing_latency", [True, False], ids=["full", "partial"])
    def test_its_answer_goes_through_the_merge_with_stake_missing(self, drop_missing_latency):
        uptime = pd.DataFrame({"indexer": INDEXERS, "% up_x": [99.0, 95.0, 90.0]})
        latency = pd.DataFrame(
            {
                "indexer": INDEXERS,
                processing.LATENCY_COEFFICIENT_COLUMN: [0.01, 0.02, 0.03],
                processing.STANDARD_ERROR_COLUMN: [0.001, 0.002, 0.003],
                "p-value": [0.01, 0.01, 0.01],
            }
        )
        info = pd.DataFrame({"indexer": INDEXERS, "url": list(URLS.values())})
        success = pd.DataFrame({"indexer": INDEXERS, "average_status": [1.0, 0.75, 0.5]})
        query_count = pd.DataFrame({"indexer": INDEXERS, "query_count": [40, 40, 40]})

        merged = merge_and_prepare_dataframes(
            uptime,
            latency,
            info,
            success,
            _failed_stake_lookup(),
            query_count,
            drop_missing_latency=drop_missing_latency,
        ).set_index("indexer")

        assert merged.loc[INDEXERS, "% up_x"].tolist() == [99.0, 95.0, 90.0]
        assert merged.loc[INDEXERS, "average_status"].tolist() == [1.0, 0.75, 0.5]
        assert merged.loc[INDEXERS, [*STAKE_COLUMNS, READ_AT]].isna().all().all()

    @pytest.mark.usefixtures("replay")
    def test_the_run_keeps_the_quality_scores_a_working_lookup_gives(self):
        working = _scores({"data": {"indexers": STAKE}})
        failed = _scores(REJECTED_KEY)

        pd.testing.assert_frame_equal(
            failed.loc[INDEXERS, QUALITY_COLUMNS], working.loc[INDEXERS, QUALITY_COLUMNS]
        )
        assert failed.loc[INDEXERS, "success_rate"].tolist() == [1.0, 0.75, 0.5]
        assert failed.loc[INDEXERS, STAKE_COLUMNS].isna().all().all()
        assert working.loc[INDEXERS, "last_known_slashable_stake"].tolist() == [1_000_000.0] * 3

    @pytest.mark.usefixtures("replay")
    def test_the_job_pushes_measured_scores_with_stake_missing(self, monkeypatch, iisa, caplog):
        with caplog.at_level("INFO"):
            pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert any("mode=partial" in r.getMessage() for r in caplog.records)
        rows = {row["indexer"]: row for row in pushed}
        assert [rows[i]["success_rate"] for i in INDEXERS] == [1.0, 0.75, 0.5]
        for row in rows.values():
            assert [row[column] for column in [*STAKE_COLUMNS, READ_AT]] == [None] * 4
            assert "norm_stake_to_fees" not in row
        lookup_error = _first(caplog.records, "ERROR", "Failed to fetch stake data")
        assert lookup_error is not None and lookup_error.exc_info is not None
        assert _first(caplog.records, "WARNING", "Scores pushed without stake data") is not None


@pytest.mark.usefixtures("iisa")
class TestStakeReadTime:
    """Each run records when its stake was read from the network subgraph."""

    @pytest.mark.usefixtures("replay")
    def test_a_successful_lookup_stamps_the_read_time_as_now(self):
        before = datetime.now(timezone.utc)
        scores = _scores({"data": {"indexers": STAKE}})
        after = datetime.now(timezone.utc)

        read_at = pd.to_datetime(scores.loc[INDEXERS, READ_AT], utc=True)
        assert ((read_at >= before) & (read_at <= after)).all()

    @pytest.mark.usefixtures("replay")
    def test_the_pushed_scores_carry_the_read_time(self, monkeypatch, iisa):
        before = datetime.now(timezone.utc).replace(microsecond=0)
        pushed = _run_job(monkeypatch, iisa, WORKING)
        after = datetime.now(timezone.utc)

        read_at = pd.to_datetime([row[READ_AT] for row in pushed], utc=True)
        assert ((read_at >= before) & (read_at <= after)).all()

    def test_the_fallback_scores_have_no_read_time(self, monkeypatch):
        """The fallback never reads stake, so its zeros can never pass for a reading."""
        monkeypatch.setattr(processing, "fetch_dips_info", _pricing)
        monkeypatch.setattr(processing, "fetch_and_filter_graph_node_versions", lambda df, _u: df)

        scores = processing.compute_degraded_scores(URLS)

        assert scores[READ_AT].isna().all()


@pytest.mark.usefixtures("replay")
class TestReusingAnEarlierRead:
    """A failed lookup reuses the stake iisa holds from a run that read it under 60 hours ago."""

    @pytest.mark.parametrize("hours", [24, 48])
    def test_reuses_stake_read_less_than_60_hours_ago(
        self, monkeypatch, iisa, clock, caplog, hours
    ):
        read_at = NOW - timedelta(hours=hours)
        iisa.held = _held(read_at)

        with caplog.at_level("INFO"):
            pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert _pushed_stake(pushed) == _held_stake()
        assert set(_pushed_read_at(pushed).values()) == {read_at}
        assert [row["success_rate"] for row in pushed] == [1.0, 0.75, 0.5]
        warning = _first(caplog.records, "WARNING", "reused for 3 indexer(s)")
        assert warning is not None and read_at.isoformat() in warning.getMessage()
        assert _first(caplog.records, "WARNING", "out of today's scores") is None
        assert _first(caplog.records, "WARNING", "Scores pushed without stake data") is None

    @pytest.mark.parametrize("hours", [60, 72])
    def test_leaves_stake_missing_once_60_hours_have_passed(
        self, monkeypatch, iisa, clock, caplog, hours
    ):
        iisa.held = _held(NOW - timedelta(hours=hours))

        with caplog.at_level("INFO"):
            pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert _pushed_stake(pushed) == {indexer: [None] * 3 for indexer in INDEXERS}
        assert set(_pushed_read_at(pushed).values()) == {None}
        assert _first(caplog.records, "WARNING", "Scores pushed without stake data") is not None

    def test_three_failed_days_in_a_row_reuse_twice(self, monkeypatch, iisa, clock):
        """Day 1 reads stake; days 2 and 3 reuse it and keep its read time; day 4 has none."""
        _run_job(monkeypatch, iisa, WORKING)
        read_on_day_1 = _pushed_stake(iisa.held)
        reused = []
        for day in (2, 3, 4):
            clock["now"] = NOW + timedelta(days=day - 1)
            pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)
            reused.append(set(_pushed_read_at(pushed).values()) == {NOW})

        assert reused == [True, True, False]
        assert _pushed_stake(iisa.held) == {indexer: [None] * 3 for indexer in INDEXERS}
        assert read_on_day_1[INDEXERS[0]] == [100_000.0, 10.0, 1_000_000.0]

    def test_an_indexer_with_no_reading_is_left_out_beside_reused_stake(
        self, monkeypatch, iisa, clock, caplog
    ):
        """c is new since the reading. Sent with stake missing beside a and b it would score
        best on stake to fees, a rank nobody measured, so it waits for a run that reads stake.
        """
        iisa.held = _held(NOW - timedelta(hours=24), indexers=INDEXERS[:2])

        with caplog.at_level("INFO"):
            pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert _pushed_stake(pushed) == _held_stake(INDEXERS[:2])
        assert _first(caplog.records, "WARNING", "reused for 2 indexer(s)") is not None
        left_out = _first(caplog.records, "WARNING", "Left 1 indexer(s) out of today's scores")
        assert left_out is not None and INDEXERS[2] in left_out.getMessage()

    def test_a_working_lookup_leaves_nobody_out(self, monkeypatch, iisa, clock, caplog):
        """The subgraph answers, but without c. That is no reuse, so c stays, as it always has."""
        without_c = {"data": {"indexers": STAKE[:2]}}

        with caplog.at_level("INFO"):
            pushed = _run_job(monkeypatch, iisa, without_c)

        assert [row["indexer"] for row in pushed] == INDEXERS
        assert _pushed_stake(pushed)[INDEXERS[2]] == [None] * 3
        assert _first(caplog.records, "WARNING", "out of today's scores") is None

    def test_the_fallback_snapshot_is_never_reused(self, monkeypatch, iisa, clock):
        """The fallback sends stake and fees as 0 with a neutral score and no read time."""
        iisa.held = [
            {
                "indexer": indexer,
                "last_known_slashable_stake": 0.0,
                "total_query_fees": 0.0,
                "stake_to_fees": None,
                "norm_stake_to_fees": 0.5,
                READ_AT: None,
            }
            for indexer in INDEXERS
        ]

        pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert _pushed_stake(pushed) == {indexer: [None] * 3 for indexer in INDEXERS}

    def test_scores_pushed_before_read_times_were_recorded_are_not_reused(
        self, monkeypatch, iisa, clock
    ):
        iisa.held = [
            {key: value for key, value in row.items() if key != READ_AT}
            for row in _held(NOW - timedelta(hours=24))
        ]

        pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert _pushed_stake(pushed) == {indexer: [None] * 3 for indexer in INDEXERS}

    @pytest.mark.parametrize(
        "failure",
        [IISAPushError("GET /scores failed after 10 attempts"), ValueError("not JSON")],
        ids=["unreachable", "not-json"],
    )
    def test_a_failed_read_of_the_held_scores_leaves_stake_missing(
        self, monkeypatch, iisa, clock, caplog, failure
    ):
        iisa.held = _held(NOW - timedelta(hours=24))
        iisa.failure = failure

        with caplog.at_level("INFO"):
            pushed = _run_job(monkeypatch, iisa, REJECTED_KEY)

        assert _pushed_stake(pushed) == {indexer: [None] * 3 for indexer in INDEXERS}
        assert [row["success_rate"] for row in pushed] == [1.0, 0.75, 0.5]
        assert _first(caplog.records, "WARNING", "Could not read the scores iisa holds")
        assert any("mode=partial" in r.getMessage() for r in caplog.records)


class TestReusableStake:
    """Only well-formed rows read under 60 hours ago, once each, can be reused."""

    @pytest.mark.parametrize(
        "reply", [None, [], {}, {"scores": None}, {"scores": "rows"}, {"scores": []}]
    )
    def test_a_reply_without_rows_gives_nothing(self, reply):
        from stake_reads import reusable_stake

        assert reusable_stake(reply, NOW).empty

    def test_unusable_rows_are_left_out(self):
        from stake_reads import reusable_stake

        good = _held(NOW - timedelta(hours=1))[0]
        rows = [
            good,
            {**good, "indexer": INDEXERS[1], READ_AT: (NOW + timedelta(hours=1)).isoformat()},
            {**good, "indexer": INDEXERS[2], "last_known_slashable_stake": "lots"},
            {**good, "indexer": "0x" + "d" * 40, "total_query_fees": -1.0},
            {**good, "indexer": "0x" + "e" * 40, READ_AT: 1_700_000_000},
            {**good, "indexer": 7},
            {**good, "indexer": "0x" + "f" * 40},
            {**good, "indexer": "0x" + "f" * 40},
            "not a row",
        ]

        answer = reusable_stake({"scores": rows}, NOW)

        assert answer.index.tolist() == [INDEXERS[0]]
        assert answer.loc[INDEXERS[0], STAKE_COLUMNS].tolist() == _held_stake()[INDEXERS[0]]


def _first(records: list[logging.LogRecord], level: str, text: str) -> Optional[logging.LogRecord]:
    """The first record at level whose message holds text."""
    return next((r for r in records if r.levelname == level and text in r.getMessage()), None)
