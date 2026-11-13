"""Uptime and success rate worked out on each chain an indexer serves.

The replay's rows each name the chain of the subgraph queried. A chain with few queries leans on
the indexer's overall value, the query-weighted average over all its chains, until it has 10,000.
"""

import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import processing  # noqa: E402
from chain_quality import FULL_WEIGHT_QUERIES, PRIOR_QUERIES, blend, own_weight  # noqa: E402
from processing import compute_all_scores, compute_degraded_scores  # noqa: E402
from redpanda import RedpandaProvider  # noqa: E402
from test_run_without_stake import (  # noqa: E402
    INDEXERS,
    URLS,
    WORKING,
    _pricing,
    _provider,
    _subgraph_reply,
)

A, B, C = INDEXERS
OTHERS = {"mainnet": 0.9, "matic": 0.7}
OTHER_QUERIES = {"mainnet": 30_000, "matic": 10_000}


def _overall(base_queries: int) -> float:
    """The query-weighted average over base at 0.5 and OTHERS, with base_queries on base."""
    return (base_queries * 0.5 + 30_000 * 0.9 + 10_000 * 0.7) / (base_queries + 40_000)


class TestBlend:
    @pytest.mark.parametrize(
        ("queries", "weight"),
        [(0, 0.0), (250, 0.5), (9_999, 9_999 / 10_249), (10_000, 1.0), (1_000_000, 1.0)],
    )
    def test_a_chains_own_weight_grows_with_its_queries(self, queries, weight):
        assert (FULL_WEIGHT_QUERIES, PRIOR_QUERIES) == (10_000, 250)
        assert own_weight(queries) == pytest.approx(weight)

    @pytest.mark.parametrize(
        ("queries", "blended"),
        [
            (0, _overall(0)),
            (250, 0.5 * 0.5 + 0.5 * _overall(250)),
            (9_999, (9_999 * 0.5 + 250 * _overall(9_999)) / 10_249),
            (10_000, 0.5),
        ],
    )
    def test_a_chain_leans_on_the_indexers_overall_value_until_it_has_10000_queries(
        self, queries, blended
    ):
        _, by_chain = blend({"base": 0.5, **OTHERS}, {"base": queries, **OTHER_QUERIES})

        assert by_chain["base"] == pytest.approx(blended)

    def test_the_prior_is_the_query_weighted_average_over_every_chain(self):
        """Matic's prior leans to mainnet, which had 30 times matic's queries, and counts
        matic's own queries too."""
        values = {"base": 0.5, "mainnet": 0.9, "matic": 0.7}
        queries = {"base": 1_000, "mainnet": 30_000, "matic": 1_000}

        overall, by_chain = blend(values, queries)

        prior = (1_000 * 0.5 + 30_000 * 0.9 + 1_000 * 0.7) / 32_000
        weight = 1_000 / 1_250
        assert overall == pytest.approx(prior)
        assert by_chain["matic"] == pytest.approx(weight * 0.7 + (1 - weight) * prior)

    def test_2_small_chains_keep_their_order(self):
        """Weighing only the other chain would swap them: 0.2 and 1.0 would blend to 0.771
        and 0.429."""
        _, by_chain = blend({"base": 0.2, "matic": 1.0}, {"base": 100, "matic": 100})

        weight = 100 / 350
        assert by_chain == pytest.approx(
            {"base": weight * 0.2 + (1 - weight) * 0.6, "matic": weight * 1.0 + (1 - weight) * 0.6}
        )
        assert by_chain["base"] < by_chain["matic"]

    @pytest.mark.parametrize("queries", [0, 10, 100_000])
    def test_with_no_other_chain_the_chains_own_value_stands(self, queries):
        assert blend({"base": 0.6}, {"base": queries})[1] == {"base": 0.6}

    def test_a_chain_without_queries_adds_nothing_to_the_prior_and_takes_it_whole(self):
        _, by_chain = blend({"base": 0.6, "mainnet": 0.1}, {"base": 10, "mainnet": 0})

        assert by_chain == pytest.approx({"base": 0.6, "mainnet": 0.6})

    def test_no_queries_on_any_chain_gives_no_overall_value(self):
        assert blend({"base": 0.6}, {"base": 0})[0] is None


def _rows(indexer: str, deployment: str, chain: str, status: str) -> list[dict[str, Any]]:
    """40 queries a minute apart from indexer on deployment, each answered with status."""
    return [
        {
            "query_id": f"q-{indexer[2:8]}-{deployment}-{i:03d}-LAX",
            "deployment_hash": deployment,
            "fee": 1.0,
            "timestamp": pd.Timestamp("2026-05-01") + pd.Timedelta(minutes=i),
            "blocks_behind": 0,
            "response_time_ms": 100.0,
            "indexer": indexer,
            "status": status,
            "day_partition": "2026-05-01",
            "subgraph_network": chain,
            "url": URLS[indexer],
        }
        for i in range(40)
    ]


def _queries() -> pd.DataFrame:
    """a answers every query on mainnet and none on base, b only serves mainnet and c only
    base, both answering everything."""
    return pd.DataFrame(
        [
            *_rows(A, "QmMain", "mainnet", "200 OK"),
            *_rows(A, "QmBase", "base", "Timeout"),
            *_rows(B, "QmMain", "mainnet", "200 OK"),
            *_rows(C, "QmBase", "base", "200 OK"),
        ]
    )


# What the count pass counted for each (deployment, indexer) over the whole replay. QmGone had
# no query sampled, so its chain is unknown.
COUNTS = pd.DataFrame(
    {
        "deployment_hash": ["QmMain", "QmBase", "QmMain", "QmBase", "QmGone"],
        "indexer": [A, A, B, C, A],
        "num_rows": [20_000, 250, 100, 5_000, 7_000],
    }
)


def _scores(monkeypatch: pytest.MonkeyPatch, counts: pd.DataFrame) -> pd.DataFrame:
    """Run the score computation without GeoIP on _queries(), the count pass giving counts."""
    monkeypatch.setattr(RedpandaProvider, "fetch_initial_query_results", lambda self, *_a: counts)
    monkeypatch.setattr(
        RedpandaProvider, "fetch_combined_query_results", lambda self, *_a: _queries()
    )
    monkeypatch.setattr(processing, "fetch_dips_info", _pricing)
    monkeypatch.setattr(processing, "fetch_and_filter_graph_node_versions", lambda df, _u: df)
    with patch("subgraph.requests.post", return_value=_subgraph_reply(WORKING)):
        return compute_all_scores(
            provider=_provider(),
            indexer_urls=URLS,
            start_date=date(2026, 5, 1),
            num_days=28,
            target_rows=20_000_000,
            geoip_available=False,
        ).set_index("indexer")


class TestScoresByChain:
    def test_each_indexer_gets_a_value_on_each_chain_it_served(self, monkeypatch):
        scores = _scores(monkeypatch, COUNTS)

        # a's 250 queries on base weigh its own 0 and its overall 20,000 / 20,250 equally.
        for column in ("uptime_by_chain", "success_rate_by_chain"):
            assert scores.loc[A, column] == pytest.approx(
                {"mainnet": 1.0, "base": 0.5 * 20_000 / 20_250}
            )
            assert scores.loc[B, column] == {"mainnet": 1.0}
            assert scores.loc[C, column] == {"base": 1.0}

    def test_each_indexer_reports_the_queries_the_blend_weighed_on_each_chain(self, monkeypatch):
        """The count pass's counts, each subgraph on its chain: a's 250 on base are what gave
        base equal weight with a's overall value above."""
        scores = _scores(monkeypatch, COUNTS)

        assert scores.loc[A, "queries_by_chain"] == {"mainnet": 20_000, "base": 250}
        assert scores.loc[B, "queries_by_chain"] == {"mainnet": 100}
        assert scores.loc[C, "queries_by_chain"] == {"base": 5_000}

    def test_the_overall_values_are_the_query_weighted_average_over_chains(self, monkeypatch):
        scores = _scores(monkeypatch, COUNTS)

        assert scores.loc[A, "uptime_score"] == pytest.approx(20_000 / 20_250)
        assert scores.loc[A, "success_rate"] == pytest.approx(20_000 / 20_250)
        assert scores.loc[B, ["uptime_score", "success_rate"]].tolist() == [1.0, 1.0]

    def test_queries_on_a_subgraph_with_no_sampled_row_are_left_out_with_a_note(
        self, monkeypatch, caplog
    ):
        with caplog.at_level(logging.INFO, logger="processing"):
            _scores(monkeypatch, COUNTS)

        notes = [r.getMessage() for r in caplog.records if "chain is unknown" in r.getMessage()]
        assert len(notes) == 1
        assert "7,000 of the count pass's 32,350 queries" in notes[0]

    def test_without_the_count_pass_the_sample_counts_with_a_warning(self, monkeypatch, caplog):
        """Each chain then has 40 queries, so each of a's 2 chains leans mostly on its overall
        0.5, while mainnet stays above base."""
        empty = pd.DataFrame(columns=["deployment_hash", "indexer", "num_rows"])

        with caplog.at_level(logging.WARNING, logger="processing"):
            scores = _scores(monkeypatch, empty)

        assert scores.loc[A, "success_rate_by_chain"] == pytest.approx(
            {"mainnet": (40 + 250 * 0.5) / 290, "base": 250 * 0.5 / 290}
        )
        assert scores.loc[A, "success_rate"] == pytest.approx(0.5)
        assert scores.loc[A, "queries_by_chain"] == {"mainnet": 40, "base": 40}
        assert any("sampled queries" in r.getMessage() for r in caplog.records)

    def test_the_fallback_scores_send_no_values_by_chain(self, monkeypatch):
        monkeypatch.setattr(processing, "fetch_dips_info", lambda _: pd.DataFrame())
        monkeypatch.setattr(processing, "fetch_and_filter_graph_node_versions", lambda df, _: df)

        scores = compute_degraded_scores({A: URLS[A]})

        assert scores["uptime_by_chain"].tolist() == [None]
        assert scores["success_rate_by_chain"].tolist() == [None]
        assert scores["queries_by_chain"].tolist() == [None]
