"""The score is uptime x freshness x quality x value.

Quality blends success rate and latency, and value blends stake-to-fees with the 2 prices. A 0
on uptime or freshness sinks the whole score, while a weak part inside quality or value only
pulls its factor down. Selection's 3 cut-offs are fractions of the best total among the
candidates a request can pick from, since products run lower than sums.
"""

import math

import pandas as pd
import pytest

from iisa.indexer_selection import (
    MIN_INDEXER_SCORE,
    MIN_SYNCED_THRESHOLD,
    REPLACEMENT_MARGIN,
    DeploymentId,
    IndexerSelector,
    _calculate_scores,
    _score_factors,
)

PARTS = {
    "norm_uptime_score": 0.9,
    "norm_freshness": 0.8,
    "norm_success_rate": 0.5,
    "norm_lat_lin_reg_coefficient": 0.7,
    "norm_stake_to_fees": 0.6,
    "norm_base_price_per_epoch": 0.4,
    "norm_price_per_entity": 1.0,
}
QUALITY = 0.2 * 0.5 + 0.8 * 0.7
VALUE = 0.45 * 0.6 + 0.45 * 0.4 + 0.10 * 1.0


def _total(**changes) -> float:
    return float(_calculate_scores(pd.DataFrame([{**PARTS, **changes}])).iloc[0])


class TestFormula:
    def test_the_total_is_the_product_of_the_4_factors(self):
        factors = _score_factors(pd.DataFrame([PARTS])).iloc[0].to_dict()

        assert factors == pytest.approx(
            {"uptime": 0.9, "freshness": 0.8, "quality": QUALITY, "value": VALUE}
        )
        assert _total() == pytest.approx(0.9 * 0.8 * QUALITY * VALUE)

    @pytest.mark.parametrize("part", ["norm_uptime_score", "norm_freshness"])
    def test_a_0_on_uptime_or_freshness_gives_0_overall(self, part):
        assert _total(**{part: 0.0}) == 0.0

    def test_a_0_on_latency_alone_does_not(self):
        total = _total(norm_lat_lin_reg_coefficient=0.0)

        assert total == pytest.approx(0.9 * 0.8 * (0.2 * 0.5) * VALUE)
        assert total > 0

    def test_freshness_left_out_counts_as_1(self):
        """Freshness is NaN on a chain whose checks don't yet cover 7 days, or are stale."""
        assert _total(norm_freshness=math.nan) == pytest.approx(0.9 * QUALITY * VALUE)

    def test_missing_uptime_scores_0(self):
        """Indexers must prove themselves, so no data is not a neutral value."""
        assert _total(norm_uptime_score=math.nan) == 0.0

    def test_a_part_missing_from_value_is_rescaled_over_the_others(self):
        """_normalize_metrics gives every part, so this only happens to a table built without
        one, which also logs a warning."""
        total = _total(norm_price_per_entity=math.nan)

        rescaled = (0.45 * 0.6 + 0.45 * 0.4) / 0.90
        assert total == pytest.approx(0.9 * 0.8 * QUALITY * rescaled)

    def test_scores_stay_within_0_and_1(self):
        ones = _total(**dict.fromkeys(PARTS, 1.0))
        zeros = _total(**dict.fromkeys(PARTS, 0.0))

        assert (ones, zeros) == (pytest.approx(1.0), 0.0)


def _selector(scores: dict[str, float], group: list[str], synced=None) -> IndexerSelector:
    """A selector over indexers with distinct orgs and locations, totals set by hand."""
    indexers = list(scores)
    processor = IndexerSelector(
        history=pd.DataFrame(
            {
                "indexer": indexers,
                "destination_loc": [f"loc{i}" for i in range(len(indexers))],
                "org": [f"org{i}" for i in range(len(indexers))],
            }
        ),
        deployment_id=DeploymentId("QmFormula"),
        synced_indexers=synced,
    )
    for indexer, score in scores.items():
        processor.data.loc[processor.data["indexer"] == indexer, "weighted_score"] = score
    processor.current_group = list(group)
    return processor


class TestRelativeCutOffs:
    """B is the best total among the candidates the request can pick from."""

    def test_the_cut_offs_are_fractions_of_the_best_total(self):
        assert (MIN_INDEXER_SCORE, REPLACEMENT_MARGIN, MIN_SYNCED_THRESHOLD) == (0.15, 0.50, 0.60)

    @pytest.mark.parametrize(("member", "replaced"), [(0.059, True), (0.061, False)])
    def test_a_member_is_replaceable_under_15_percent_of_the_best(self, member, replaced):
        """D, the only candidate, sets B at 0.40, so the cut-off is 0.06."""
        processor = _selector({"A": member, "B": 0.3, "C": 0.3, "D": 0.40}, ["A", "B", "C"])

        swap = processor._swap_for("A")

        assert (swap is not None) is replaced

    def test_a_low_best_total_still_lets_a_much_better_candidate_in(self):
        """D, the only candidate, sets B at 0.10. A at 0.01 is under 0.015 and D gains 0.09,
        more than half of B, where a fixed margin of 0.50 would keep A for ever."""
        processor = _selector({"A": 0.01, "B": 0.08, "C": 0.08, "D": 0.10}, ["A", "B", "C"])

        swap = processor._swap_for("A")

        assert swap is not None and swap[:2] == ("A", "D")

    @pytest.mark.parametrize(("synced_score", "replacement"), [(0.25, "C"), (0.24, "D")])
    def test_a_replacement_must_beat_the_member_by_more_than_half_the_best(
        self, synced_score, replacement
    ):
        """D sets B at 0.40, so a replacement must gain more than 0.20 on A's 0.04. C says it
        has synced, so it is tried first: at 0.25 it gains 0.21 and replaces A, at 0.24 it gains
        only 0.20, and D, the best candidate, replaces A instead."""
        processor = _selector(
            {"A": 0.04, "B": 0.3, "C": synced_score, "D": 0.40}, ["A", "B"], synced={"c"}
        )

        swap = processor._swap_for("A")

        assert swap is not None and swap[:2] == ("A", replacement)

    @pytest.mark.parametrize(("synced_score", "kept_ahead"), [(0.241, True), (0.239, False)])
    def test_a_synced_candidate_keeps_its_head_start_at_60_percent_of_the_best(
        self, synced_score, kept_ahead
    ):
        """B is in the group and says it has synced, so a synced candidate keeps its head
        start only at 60% of the best candidate's total or more: D's 0.40 puts that at 0.24."""
        processor = _selector(
            {"A": 0.3, "B": 0.3, "C": synced_score, "D": 0.40}, ["A", "B"], synced={"b", "c"}
        )

        candidates = processor.data[processor.data["indexer"].isin(["C", "D"])]
        ahead = processor._synced_first(candidates)

        assert ("C" in set(ahead["indexer"])) is kept_ahead


class TestReplacingTheGroupsSyncedMember:
    """A member being replaced has left the group for the replacement's sake, so its sync claim
    doesn't count: a synced challenger keeps the head start the first synced indexer gets."""

    def test_a_synced_challenger_keeps_its_head_start_when_the_only_synced_member_goes(self):
        """E, the group's only synced member, totals under 15% of A's 1.0. S says it has
        synced, totals under 60% of A's and gains 0.56 on E, more than half of A's total."""
        processor = _selector(
            {"E": 0.02, "M": 0.9, "S": 0.58, "A": 1.0}, ["E", "M"], synced={"e", "s"}
        )

        swap = processor._swap_for("E")

        assert swap is not None and swap[:2] == ("E", "S")

    def test_another_synced_member_still_holds_a_synced_challenger_to_60_percent(self):
        """T, also synced, stays in the group, so S needs 60% of A's total to stay ahead."""
        processor = _selector(
            {"E": 0.02, "T": 0.9, "S": 0.58, "A": 1.0}, ["E", "T"], synced={"e", "t", "s"}
        )

        swap = processor._swap_for("E")

        assert swap is not None and swap[:2] == ("E", "A")


class TestCutOffsWhileFillingWithStale:
    """Once no fresh candidate is left, the stale ones fill the remaining places but never
    replace a member. B is the best total the pick at hand can choose from: the best stale
    candidate for a fill-in, and the best fresh one, of which there is none, for a replacement.
    """

    @staticmethod
    def _filling(scores: dict[str, float], group: list[str], stale: list[str], synced=None):
        processor = _selector(scores, group, synced=synced)
        processor.stale_indexers = stale
        processor._filling_with_stale = True
        return processor

    @pytest.mark.parametrize(("synced_score", "picked"), [(0.241, "C"), (0.239, "D")])
    def test_a_synced_stale_candidate_needs_60_percent_of_the_best_stale_one(
        self, synced_score, picked
    ):
        """B in the group says it has synced, so C, which says so too, keeps its head start only
        at 60% of D's 0.40, the best stale candidate, or more."""
        processor = self._filling(
            {"A": 0.3, "B": 0.3, "C": synced_score, "D": 0.40},
            ["A", "B"],
            stale=["C", "D"],
            synced={"b", "c"},
        )

        assert processor._find_best_replacement_or_select_best_indexer() == picked

    def test_a_stale_candidate_never_replaces_a_member(self):
        """A totals far under 15% of D's, but D is stale, so nothing replaces A."""
        processor = self._filling(
            {"A": 0.01, "B": 0.3, "C": 0.3, "D": 0.40}, ["A", "B", "C"], stale=["D"]
        )

        assert processor._swap_for("A") is None
