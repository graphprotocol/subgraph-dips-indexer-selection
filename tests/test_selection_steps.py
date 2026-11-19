"""The selector notes each change it makes to a group, as it makes it.

A selection's record is read from these notes, so they must hold what the selector saw at the
moment of each pick: the pool the indexer was drawn from, the row it was ranked on, whether the
pick kept the group spread over 2 operators and 2 locations, and who was passed over for it.
"""

from typing import Any, Optional
from unittest.mock import patch

import pandas as pd
import pytest

from iisa.indexer_selection import DeploymentId, IndexerSelector, Step

DEPLOYMENT = DeploymentId("QmSteps")


def _selector(
    scores: dict[str, float],
    group: tuple[str, ...] = (),
    target: int = 3,
    orgs: Optional[dict[str, str]] = None,
    **settings: Any,
) -> IndexerSelector:
    """A selector over indexers whose totals are set by hand, each in an operator and a place
    of its own unless ``orgs`` puts some together, with ``group`` as its group and no step yet.
    """
    indexers = list(scores)
    places = [(orgs or {}).get(indexer, f"org-{indexer}") for indexer in indexers]
    selector = IndexerSelector(
        history=pd.DataFrame({"indexer": indexers, "destination_loc": places, "org": places}),
        deployment_id=DEPLOYMENT,
        target_size=target,
        **settings,
    )
    for indexer, score in scores.items():
        selector.data.loc[selector.data["indexer"] == indexer, "weighted_score"] = score
    selector.current_group = list(group)
    selector.steps.clear()
    return selector


def _summary(step: Step) -> tuple[Any, ...]:
    pick = step.pick
    assert pick is not None and pick.indexer == step.indexer
    return step.action, step.indexer, step.replaced, pick.pool, pick.kept_spread, pick.passed_over


class TestAdding:
    def test_each_indexer_added_is_a_step_with_the_search_that_found_it(self):
        selector = _selector({"A": 0.5, "B": 0.9, "C": 0.7}, target=2)

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["B", "C"]
        assert [_summary(step) for step in selector.steps] == [
            ("added", "B", None, "all", True, []),
            ("added", "C", None, "all", True, []),
        ]
        first, second = (step.pick for step in selector.steps)
        assert first.row["weighted_score"] == 0.9 and second.row["weighted_score"] == 0.7
        assert first.pools == [("B", "all"), ("C", "all"), ("A", "all")]
        assert second.pools == [("C", "all"), ("A", "all")]

    def test_a_candidate_skipped_to_keep_the_group_spread_is_noted_as_passed_over(self):
        """B2 ranks second but shares B's operator and place, so C takes the second place."""
        scores = {"A": 0.5, "B": 0.9, "B2": 0.8, "C": 0.7}
        selector = _selector(scores, target=2, orgs={"B2": "org-B"})

        selector._assign_indexers_to_subgraph()

        assert [_summary(step) for step in selector.steps] == [
            ("added", "B", None, "all", True, []),
            ("added", "C", None, "all", True, ["B2"]),
        ]
        assert selector.group_is_spread() is True

    def test_a_pick_that_cannot_keep_the_group_spread_is_noted_as_one(self):
        selector = _selector({"B": 0.9, "B2": 0.8}, target=2, orgs={"B2": "org-B"})

        selector._assign_indexers_to_subgraph()

        assert [_summary(step) for step in selector.steps] == [
            ("added", "B", None, "all", True, []),
            ("added", "B2", None, "all", False, []),
        ]
        assert selector.group_is_spread() is False

    def test_the_pool_is_the_one_the_indexer_was_drawn_from(self):
        """A says it has synced, so it is drawn from first, ahead of the better B."""
        selector = _selector({"A": 0.5, "B": 0.9, "C": 0.7}, target=2, synced_indexers={"a"})

        selector._assign_indexers_to_subgraph()

        assert [_summary(step)[:4] for step in selector.steps] == [
            ("added", "A", None, "synced"),
            ("added", "B", None, "unsynced"),
        ]
        assert selector.steps[0].pick.pools == [
            ("A", "synced"),
            ("B", "unsynced"),
            ("C", "unsynced"),
        ]

    def test_an_indexer_added_by_another_search_has_no_search_noted(self):
        """The step never carries the search for a different indexer."""
        selector = _selector({"A": 0.5, "B": 0.9}, target=1)
        search = (
            "iisa.indexer_selection.IndexerSelector._find_best_replacement_or_select_best_indexer"
        )

        with patch(search, return_value="Z"):
            selector._add_indexers_to_group()

        assert selector.steps == [Step("added", "Z")]


class TestLeaving:
    def test_a_member_removed_from_a_group_over_its_target_is_a_step_with_its_total(self):
        selector = _selector({"A": 0.5, "B": 0.9, "C": 0.7}, group=("A", "B", "C"), target=2)

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["B", "C"]
        assert selector.steps == [Step("removed", "A", score=0.5, kept_spread=True)]

    def test_a_removal_that_keeps_the_group_spread_is_preferred_over_a_lower_total(self):
        """Taking out A, the lowest, would leave B and C in 1 operator, so C goes instead."""
        orgs = {"B": "shared", "C": "shared"}
        selector = _selector(
            {"A": 0.5, "B": 0.9, "C": 0.7}, group=("A", "B", "C"), target=2, orgs=orgs
        )

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["A", "B"]
        assert selector.steps == [Step("removed", "C", score=0.7, kept_spread=True)]

    def test_the_lowest_total_is_removed_when_no_removal_keeps_the_group_spread(self):
        orgs = dict.fromkeys("ABC", "shared")
        selector = _selector(
            {"A": 0.5, "B": 0.9, "C": 0.7}, group=("A", "B", "C"), target=2, orgs=orgs
        )

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["B", "C"]
        assert selector.steps == [Step("removed", "A", score=0.5, kept_spread=False)]

    def test_a_group_shrinks_to_a_target_of_1(self):
        """A group of 1 can't be spread, so the spread can't decide which member stays."""
        selector = _selector({"A": 0.5, "B": 0.9}, group=("A", "B"), target=1)

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["B"]
        assert selector.steps == [Step("removed", "A", score=0.5, kept_spread=True)]

    def test_a_group_whose_totals_failed_is_left_as_it_is(self):
        """With scoring failed every total is NaN, so trimming would cancel members at random."""
        nan = float("nan")
        selector = _selector({"A": nan, "B": nan, "C": nan}, group=("A", "B", "C"), target=1)

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["A", "B", "C"]
        assert selector.steps == []

    def test_a_group_with_no_scored_member_is_left_as_it_is(self):
        """None of X, Y and Z has a score row, so there is nothing to rank them by."""
        selector = _selector({"A": 0.5}, group=("X", "Y", "Z"), target=1)

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["X", "Y", "Z"]
        assert selector.steps == []

    def test_trimming_to_a_target_of_1_keeps_the_best_scorer(self):
        """Only taking out H keeps the 3 spread, but a group of 1 needs no spread, so H stays."""
        selector = _selector({"H": 0.9, "X": 0.5, "Y": 0.1}, group=("H", "X", "Y"), target=1)
        selector.data["org"] = ["o1", "o1", "o2"]
        selector.data["destination_loc"] = ["l1", "l2", "l1"]

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["H"]
        assert [step.indexer for step in selector.steps] == ["Y", "X"]

    def test_once_no_removal_keeps_the_group_spread_the_rest_go_without_checking_again(self):
        """Removing members only narrows a group, so once 1 pass finds no removal that keeps it
        spread, none can, and a group of thousands isn't checked again for every member shed."""
        orgs = dict.fromkeys("ABCDE", "shared")
        scores = {"A": 0.1, "B": 0.5, "C": 0.3, "D": 0.9, "E": 0.7}
        selector = _selector(scores, group=tuple("ABCDE"), target=2, orgs=orgs)
        check = selector._meets_decentralization_requirements_indexer_removal

        with patch.object(
            selector, "_meets_decentralization_requirements_indexer_removal", wraps=check
        ) as spread_check:
            selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["D", "E"]
        assert [step.indexer for step in selector.steps] == ["A", "C", "B"]
        assert spread_check.call_count == 5

    def test_a_member_with_no_score_is_removed_on_a_total_of_0(self):
        selector = _selector({"B": 0.9, "C": 0.7}, group=("X", "B", "C"), target=2)

        selector._assign_indexers_to_subgraph()

        assert selector.steps == [Step("removed", "X", score=0.0, kept_spread=True)]

    def test_a_failing_member_swapped_out_is_a_step_naming_both(self):
        scores = {"A": 0.05, "B": 0.9, "C": 0.7, "D": 0.8}
        selector = _selector(scores, group=("A", "C"), target=2)

        selector._assign_indexers_to_subgraph()

        assert selector.current_group == ["C", "B"]
        assert [_summary(step) for step in selector.steps] == [
            ("replaced", "B", "A", "all", True, []),
        ]
        assert selector.steps[0].pick.row["weighted_score"] == 0.9


class TestWhoCouldNotBePicked:
    def test_the_reasons_cover_exactly_the_indexers_selection_could_not_pick(self):
        """They are read from the lists selection takes its unpickable indexers from, so the 2
        can't disagree.
        """
        selector = _selector(
            dict.fromkeys("ABCDEF", 0.5),
            group=("A",),
            target=1,
            pending_agreements={DeploymentId("QmOther"): ["B", "C"]},
            declined_indexers={DEPLOYMENT: ["C"], DeploymentId("QmOther"): ["F"]},
            indexer_denylist=["D", "C", "A"],
            stale_indexers=["E"],
        )

        reasons = selector.left_out_reasons()

        assert reasons == {
            "B": ["pending_agreement"],
            "C": ["blocklisted", "pending_agreement", "declined"],
            "D": ["blocklisted"],
            "E": ["stale"],
        }
        assert set(reasons) == selector._unpickable_indexers() - set(selector.current_group)

    @pytest.mark.parametrize(
        ("target", "group", "filled", "reasons"),
        [(1, ["A"], False, {"E": ["stale"]}), (2, ["A", "E"], True, {})],
    )
    def test_a_stale_indexer_has_no_reason_once_the_stale_may_fill_places(
        self, target, group, filled, reasons
    ):
        """With A filling the only place, E is held back as stale. With a second place and no
        fresh candidate for it, the stale may fill it, so being stale no longer rules E out.
        """
        selector = IndexerSelector(
            history=pd.DataFrame(
                {"indexer": ["A", "E"], "destination_loc": ["a", "e"], "org": ["a", "e"]}
            ),
            deployment_id=DEPLOYMENT,
            existing_agreements={DEPLOYMENT: ["A"]},
            stale_indexers=["E"],
            target_size=target,
        )

        assert selector.current_group == group and selector.filled_with_stale is filled
        assert selector.left_out_reasons() == reasons


@pytest.mark.parametrize(
    ("group", "spread"),
    [
        ((), None),
        (("A",), None),
        (("A", "B"), True),
        (("A", "A2"), False),
        (("A", "A2", "B"), True),
    ],
)
def test_a_group_is_spread_when_it_holds_2_operators_and_2_places(group, spread):
    selector = _selector(dict.fromkeys(["A", "A2", "B"], 0.5), group=group, orgs={"A2": "org-A"})

    assert selector.group_is_spread() is spread
