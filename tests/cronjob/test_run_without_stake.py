"""A score run whose stake lookup fails still ships the quality scores it measured.

Stake comes from the network subgraph, read near the end of the run, after the long replay of
gateway queries. When that read fails the run should carry on and push its real latency,
uptime and success scores with stake and query fees missing, rather than throw them all away
for the fallback's equal neutral scores.
"""

import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import main  # noqa: E402
import processing  # noqa: E402
from processing import compute_all_scores, merge_and_prepare_dataframes  # noqa: E402
from redpanda import RedpandaProvider  # noqa: E402

INDEXERS = ["0x" + c * 40 for c in "abc"]
URLS = {indexer: f"https://{indexer[2:8]}.example/" for indexer in INDEXERS}
STAKE_COLUMNS = ["stake_to_fees", "total_query_fees", "last_known_slashable_stake"]
QUALITY_COLUMNS = ["uptime_score", "success_rate", "lat_lin_reg_coefficient", "query_count"]
STAKE = [{"id": indexer, "stakedTokens": "1000000", "lockedTokens": "0"} for indexer in INDEXERS]
REJECTED_KEY = {"errors": [{"message": "auth error: API key not found"}]}


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


def _failed_stake_lookup() -> pd.DataFrame:
    with patch("subgraph.requests.post", return_value=_subgraph_reply(REJECTED_KEY)):
        return _provider().fetch_stake_to_fees()


class TestFailedStakeLookup:
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
        assert merged.loc[INDEXERS, STAKE_COLUMNS].isna().all().all()

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
    def test_the_job_pushes_measured_scores_with_stake_missing(self, monkeypatch, caplog):
        pushed: list[list[dict[str, Any]]] = []
        provider = _provider()
        monkeypatch.setattr(main, "RedpandaProvider", lambda: provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: False)
        monkeypatch.setattr(main, "discover_indexers_from_network_subgraph", lambda _url: URLS)
        monkeypatch.setattr("redpanda.post_scores", lambda _url, _token, rows: pushed.append(rows))

        with (
            patch("subgraph.requests.post", return_value=_subgraph_reply(REJECTED_KEY)),
            caplog.at_level("INFO"),
        ):
            assert main.run_scoring() is True

        assert any("mode=partial" in r.getMessage() for r in caplog.records)
        rows = {row["indexer"]: row for row in pushed[0]}
        assert [rows[i]["success_rate"] for i in INDEXERS] == [1.0, 0.75, 0.5]
        for row in rows.values():
            assert [row[column] for column in STAKE_COLUMNS] == [None, None, None]
            assert "norm_stake_to_fees" not in row
        lookup_error = _first(caplog.records, "ERROR", "Failed to fetch stake data")
        assert lookup_error is not None and lookup_error.exc_info is not None
        assert _first(caplog.records, "WARNING", "Scores pushed without stake data") is not None


def _first(records: list[logging.LogRecord], level: str, text: str) -> Optional[logging.LogRecord]:
    """The first record at level whose message holds text."""
    return next((r for r in records if r.levelname == level and text in r.getMessage()), None)
