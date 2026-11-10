"""Stake to fees when the pushed scores carry no stake to divide expected DIPs fees into.

When the score job's main pipeline fails it pushes a fallback snapshot that gives every
indexer the same neutral stake-to-fees score of 0.5, with stake and query fees sent as 0.
Dipper's expected DIPs fees must not turn that into 0 for indexers holding agreements and 1
for everyone else, while a snapshot with real stake goes on scoring exactly as before.
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
A, B, C, D = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4"))


def _degraded_record(indexer: str, price: str = "450") -> dict[str, Any]:
    """One indexer as the score job's fallback pushes it: equal quality metrics, a neutral
    stake-to-fees score, no stake or query fees, and the indexer's real DIPs price.
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
        "total_query_fees": 0.0,
        "last_known_slashable_stake": 0.0,
        "computed_at": "2026-11-10T00:00:00+00:00",
        "query_count": 0,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "{price}"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": f'["{CHAIN}"]',
        "graph_node_version": None,
        "graph_node_commit": None,
    }


def _full_record(indexer: str, stake: float, fees: float) -> dict[str, Any]:
    """One indexer as the full pipeline pushes it: its own stake and query fees, and no
    neutral stake-to-fees score, which only the fallback sends.
    """
    record = _degraded_record(indexer)
    del record["norm_stake_to_fees"]
    return {
        **record,
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "success_rate": 0.98,
        "stake_to_fees": stake / fees if fees else None,
        "total_query_fees": fees,
        "last_known_slashable_stake": stake,
        "org": f"org-{indexer[2:4]}",
        "dst_lat": 52.4,
        "dst_lon": 4.9,
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


class TestSnapshotWithoutStake:
    """With no stake anywhere in the snapshot, expected DIPs fees leave stake to fees alone."""

    def test_degraded_snapshot_keeps_the_neutral_score_for_every_indexer(self):
        records = [_degraded_record(indexer) for indexer in (A, B, C, D)]

        data = _select(records, fees={A: 50.0, B: 900.0})

        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == [0.5] * 4

    def test_degraded_snapshot_scores_the_same_with_or_without_dips_fees(self):
        """Equal prices then mean equal scores, so holding an agreement costs nothing."""
        records = [_degraded_record(indexer) for indexer in (A, B, C, D)]

        with_fees = _select(records, fees={A: 50.0, B: 900.0})
        without_fees = _select(records)

        assert with_fees.loc[[A, B, C, D], "weighted_score"].tolist() == pytest.approx(
            without_fees.loc[[A, B, C, D], "weighted_score"].tolist()
        )
        assert with_fees["weighted_score"].nunique() == 1

    def test_snapshot_whose_stake_lookup_failed_scores_everyone_alike(self):
        """The full pipeline sends stake and query fees as 0 when it cannot read stake, with no
        neutral score, and every indexer already scores the same there without DIPs fees.
        """
        records = [_full_record(indexer, 0.0, 0.0) for indexer in (A, B, C, D)]

        with_fees = _select(records, fees={A: 50.0, B: 900.0})
        without_fees = _select(records)

        assert with_fees.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == (
            without_fees.loc[[A, B, C, D], "norm_stake_to_fees"].tolist()
        )
        assert with_fees["norm_stake_to_fees"].nunique() == 1


class TestSnapshotWithStake:
    """With real stake, expected DIPs fees count exactly like extra query fees, as before."""

    RECORDS = [
        _full_record(A, 1_000.0, 10.0),
        _full_record(B, 2_000.0, 10.0),
        _full_record(C, 3_000.0, 0.0),
        _full_record(D, 4_000.0, 10.0),
    ]
    FEES = {A: 90.0, C: 30.0}

    def test_dips_fees_join_the_query_fees_before_scaling(self):
        """A 1,000 / 100 = 10, B 2,000 / 10 = 200, C 3,000 / 30 = 100, D 4,000 / 10 = 400."""
        data = _select(self.RECORDS, fees=self.FEES)

        assert data.loc[[A, B, C, D], "stake_to_fees"].tolist() == [10.0, 200.0, 100.0, 400.0]
        expected = [0.0, 190 / 390, 90 / 390, 1.0]
        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(expected)

    def test_dips_fees_score_like_the_same_fees_already_in_the_snapshot(self):
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

    def test_one_indexer_without_stake_still_scores_as_before(self):
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

    def test_a_carried_neutral_score_is_recomputed_when_stake_is_there(self):
        """Stake decides, not the neutral column, so real stake is never ignored."""
        records = [{**r, "norm_stake_to_fees": 0.5} for r in self.RECORDS]

        data = _select(records, fees=self.FEES)

        expected = [0.0, 190 / 390, 90 / 390, 1.0]
        assert data.loc[[A, B, C, D], "norm_stake_to_fees"].tolist() == pytest.approx(expected)
