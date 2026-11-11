"""Stake to fees on the score job's fallback scores.

When the score job's main pipeline fails it pushes fallback scores: every indexer gets the same
neutral stake-to-fees score of 0.5, stake and query fees are sent as unknown, and every row is
labelled "degraded". Dipper's expected DIPs fees must leave those scores alone, while scores a
full run measured go on counting the expected fees exactly as before.
"""

from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from iisa import DataManager
from iisa import iisa_http_endpoints as endpoints
from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor
from iisa.indexer_selection import IndexerSelector

CHAIN = "arbitrum-one"
DEPLOYMENT = "QmDegradedStakeToFees"
A, B, C, D, E = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4", "e5"))
MAINNET_ONLY = {
    "dips_min_grt_per_30_days": '{"mainnet": "450"}',
    "dips_supported_networks": '["mainnet"]',
}


def _fallback_record(indexer: str, **extra: Any) -> dict[str, Any]:
    """One indexer as the score job's fallback pushes it: equal quality metrics, a neutral
    stake-to-fees score, stake and query fees unknown, the "degraded" label, and the indexer's
    real DIPs price.
    """
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.0,
        "lat_coefficient_std_error": 0.0,
        "lat_coefficient_upper_bound": 0.0,
        "lat_normalized_score": 0.5,
        "uptime_score": 0.5,
        "observed_duration_seconds": None,
        "uptime_duration_seconds": None,
        "success_rate": 0.5,
        "stake_to_fees": None,
        "norm_stake_to_fees": 0.5,
        "org": None,
        "dst_lat": None,
        "dst_lon": None,
        "total_query_fees": None,
        "last_known_slashable_stake": None,
        "computed_at": "2026-11-10T00:00:00+00:00",
        "query_count": 0,
        "scoring_mode": "degraded",
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "450"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": f'["{CHAIN}"]',
        "graph_node_version": None,
        "graph_node_commit": None,
        **extra,
    }


def _full_record(
    indexer: str, stake: Optional[float], fees: Optional[float], **extra: Any
) -> dict[str, Any]:
    """One indexer as a full run pushes it: its own stake and query fees, the "full" label,
    and no neutral stake-to-fees score, which only the fallback sends.
    """
    record = _fallback_record(indexer)
    del record["norm_stake_to_fees"]
    return {
        **record,
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "success_rate": 0.98,
        "stake_to_fees": stake / fees if stake is not None and fees else None,
        "total_query_fees": fees,
        "last_known_slashable_stake": stake,
        "org": f"org-{indexer[2:4]}",
        "dst_lat": 52.4,
        "dst_lon": 4.9,
        "scoring_mode": "full",
        **extra,
    }


def _select(
    records: list[dict[str, Any]], fees: Optional[dict[str, float]] = None, **request: Any
) -> pd.DataFrame:
    """Load records through the score loader, run /select-indexers selection on them and
    return the selector's scored rows, indexed by indexer.
    """
    history = DataManager(MagicMock()).transform_scores_df(pd.DataFrame(records))
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    selection = SelectionRequest(
        deployment_id=DEPLOYMENT,
        chain_id=CHAIN,
        num_candidates=request.pop("num_candidates", 3),
        optimistic_dips_fees=fees,
        **request,
    )
    state = SimpleNamespace(history=history, sync_status=None)
    with (
        patch.object(endpoints, "_state", state),
        patch.object(endpoints, "IndexerSelector", side_effect=build),
    ):
        _select_with_processor(selection)
    return built[0].data.set_index("indexer")


class TestFallbackScores:
    """Scores labelled "degraded" keep their neutral stake to fees whatever dipper expects."""

    FEES = {A: 50.0, B: 900.0}

    def test_every_indexer_keeps_the_neutral_score(self):
        records = [_fallback_record(indexer) for indexer in (A, B, C, D)]

        data = _select(records, fees=self.FEES)

        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == [0.5] * 4

    def test_expected_fees_change_no_score(self):
        """Selection compares scores with fixed bars, such as the 0.60 a synced indexer must
        reach to be preferred, so a request that carries expected fees must not lift every
        score above one that carries none.
        """
        records = [_fallback_record(indexer) for indexer in (A, B, C, D)]

        with_fees = _select(records, fees=self.FEES)
        without_fees = _select(records)

        assert with_fees.loc[[A, B, C, D], "weighted_score"].tolist() == pytest.approx(
            without_fees.loc[[A, B, C, D], "weighted_score"].tolist()
        )
        assert with_fees["weighted_score"].nunique() == 1

    def test_indexers_left_after_the_chain_filter_keep_the_neutral_score(self):
        """Every row carries the label, so the part of the scores a request is left with is
        still known to be the fallback's.
        """
        records = [_fallback_record(indexer) for indexer in (A, B, C)]
        records += [_fallback_record(indexer, **MAINNET_ONLY) for indexer in (D, E)]

        data = _select(records, fees=self.FEES)

        assert sorted(data.index) == [A, B, C]
        assert data.loc[[A, B, C], "norm_stake_to_fees"].tolist() == [0.5] * 3


class TestMeasuredScores:
    """On scores a run measured, expected DIPs fees count like extra query fees, as before."""

    RECORDS = [
        _full_record(A, 1_000.0, 10.0),
        _full_record(B, 2_000.0, 10.0),
        _full_record(C, 3_000.0, 0.0),
        _full_record(D, 4_000.0, 10.0),
    ]
    FEES = {A: 90.0, C: 30.0}
    # A 1,000 / 100 = 10, B 2,000 / 10 = 200, C 3,000 / 30 = 100, D 4,000 / 10 = 400.
    RATIOS = [10.0, 200.0, 100.0, 400.0]
    SCORES = [0.0, 190 / 390, 90 / 390, 1.0]

    def test_dips_fees_join_the_query_fees_before_scaling(self):
        data = _select(self.RECORDS, fees=self.FEES)

        assert data.loc[[A, B, C, D], "stake_to_fees"].tolist() == self.RATIOS
        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(self.SCORES)

    def test_dips_fees_score_like_the_same_fees_already_in_the_scores(self):
        moved = [
            _full_record(
                r["indexer"],
                r["last_known_slashable_stake"],
                r["total_query_fees"] + self.FEES.get(r["indexer"], 0.0),
            )
            for r in self.RECORDS
        ]

        with_fees = _select(self.RECORDS, fees=self.FEES)
        already_in = _select(moved)

        columns = ["norm_stake_to_fees", "weighted_score"]
        assert with_fees.loc[[A, B, C, D], columns].to_numpy() == pytest.approx(
            already_in.loc[[A, B, C, D], columns].to_numpy()
        )

    def test_scores_from_a_run_without_geoip_count_the_fees_too(self):
        """Such a run measures stake and fees like a full one; only its latency is neutral."""
        records = [{**r, "scoring_mode": "partial_no_geoip"} for r in self.RECORDS]

        data = _select(records, fees=self.FEES)

        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(self.SCORES)

    def test_scores_with_no_label_count_the_fees_too(self):
        """Only the "degraded" label sets scores apart, so scores pushed by hand without one
        are treated as measured.
        """
        records = [{k: v for k, v in r.items() if k != "scoring_mode"} for r in self.RECORDS]

        data = _select(records, fees=self.FEES)

        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(self.SCORES)

    def test_a_carried_neutral_score_is_worked_out_again(self):
        """The label decides, not the neutral column, so measured stake is never ignored."""
        records = [{**r, "norm_stake_to_fees": 0.5} for r in self.RECORDS]

        data = _select(records, fees=self.FEES)

        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(self.SCORES)

    def test_an_indexer_without_stake_scores_worst_once_it_holds_fees(self):
        """A has no slashable stake, so its expected DIPs fees give it a ratio of 0, the
        worst, and B with no fees at all keeps the best score of 1, set just above D.
        """
        records = [
            _full_record(A, 0.0, 0.0),
            _full_record(B, 2_000.0, 0.0),
            _full_record(C, 3_000.0, 10.0),
            _full_record(D, 4_000.0, 10.0),
        ]

        data = _select(records, fees={A: 50.0})

        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(
            [0.0, 1.0, 300 / 401, 400 / 401]
        )

    def test_stake_and_fees_unknown_for_everyone_scores_everyone_alike(self):
        """A run that measured everything but stake sends stake, fees and their ratio as
        unknown for every indexer, with no neutral score, and no indexer may gain from it.
        """
        records = [_full_record(indexer, None, None) for indexer in (A, B, C, D)]

        with_fees = _select(records, fees={A: 50.0, B: 900.0})
        without_fees = _select(records)

        assert with_fees.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == (
            without_fees.loc[[A, B, C, D], "norm_stake_to_fees"].tolist()
        )
        assert with_fees["norm_stake_to_fees"].nunique() == 1
