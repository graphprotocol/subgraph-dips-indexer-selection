import logging
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from iisa.indexer_selection import (
    DEFAULT_WEIGHTS,
    DeploymentId,
    IndexerId,
    IndexerSelector,
    _calculate_weighted_scores,
    _normalize_generic,
    _normalize_metrics,
    _normalize_uptime_and_success_rate,
)


def _row_wise_weighted_score(row: pd.Series, weights: dict) -> float:
    """Reference oracle: the obvious row-at-a-time weighted average.

    _calculate_weighted_scores does the same sum as one matrix multiply; the
    equivalence tests below check the vectorised version against this simple,
    correct-by-inspection version. Absent/NaN columns drop from the denominator.
    """
    weighted_sum = 0.0
    weight_total = 0.0
    for metric, weight in weights.items():
        column_name = f"norm_{metric}"
        if column_name not in row.index:
            continue
        value = row.get(column_name, np.nan)
        if not pd.isna(value):
            weighted_sum += value * weight
            weight_total += weight
    if weight_total == 0:
        raise ValueError("Total weight cannot be 0.")
    return weighted_sum / weight_total


@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "indexer": ["A", "B", "C"],
            "deployment_hash": ["hash1", "hash2", "hash3"],
            "score": [0.8, 0.6, 0.7],
        }
    )


@pytest.fixture
def mock__combined_query_results(faker):
    return pd.DataFrame(
        {
            "query_id": [faker.query_id() for _ in range(3)],
            "deployment_hash": [faker.deployment_id() for _ in range(3)],
            "indexer": [faker.indexer_id() for _ in range(3)],
            "indexer_network": ["net1", "net2", "net3"],
            "org": ["hetzner", "amazon aws", "google"],
            "fee": [0.1, 0.2, 0.3],
            "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "blocks_behind": [1, 2, 3],
            "response_time_ms": [100, 200, 300],
            "status": ["200 OK", "200 OK", "200 OK"],
            "day_partition": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "subgraph_network": ["network1", "network2", "network3"],
            "url": [faker.url() for _ in range(3)],
            "origin_loc": ["0,20", "40,40", "60,60"],
            "destination_loc": ["20,40", "40,60", "60,80"],
            "loc": ["0,20", "40,40", "60,60"],
            "distance_miles": [100, 200, 300],
            "sampled_query_id_hashed_mod_integer_root": [0, 1, 2],
        }
    )


@pytest.fixture
def mock__provider(faker, mock__combined_query_results):
    provider = MagicMock()
    provider.return_value.fetch_initial_query_results.return_value = pd.DataFrame(
        {
            "deployment_hash": [faker.deployment_id() for _ in range(3)],
            "indexer": [faker.indexer_id() for _ in range(3)],
            "num_rows": [1000, 2000, 3000],
        }
    )
    provider.return_value.fetch_combined_query_results.return_value = mock__combined_query_results
    provider.return_value.fetch_initial_stake_to_fees.return_value = pd.DataFrame(
        {
            "indexer": [faker.indexer_id() for _ in range(3)],
            "stake_to_fees": [1.0, 2.0, 3.0],
        }
    )
    return provider


class TestIndexerSelector:
    """
    Unit tests for the IndexerSelector class.
    """

    @pytest.fixture
    def sample_data(self):
        """
        Fixture to create a sample DataFrame for testing.
        """
        return pd.DataFrame(
            {
                "indexer": ["A", "B", "C"],
                "deployment_hash": ["hash1", "hash2", "hash3"],
                "score": [0.8, 0.6, 0.7],
                "destination_loc": ["loc1", "loc2", "loc3"],
                "org": ["org1", "org2", "org3"],
                "weighted_score": [0.9, 0.7, 0.8],
                "lat_lin_reg_coefficient": [0.1, 0.2, 0.3],
                "uptime_score": [0.9, 0.8, 0.7],
                "stake_to_fees": [0.1, 0.2, 0.3],
                "success_rate": [0.95, 0.90, 0.85],
                "base_price_per_epoch": [100, 200, 300],
                "price_per_entity": [0.1, 0.2, 0.3],
            }
        )

    @pytest.mark.parametrize(
        "initial_group, current_group, expected_added, expected_cancelled",
        [
            (
                ["A", "B"],  # initial_group
                ["A", "C"],  # current_group
                {"test_subgraph": ["C"]},  # expected_added
                {"test_subgraph": ["B"]},  # expected_cancelled
            ),
            (
                [],  # initial_group
                ["A", "B"],  # current_group
                {"test_subgraph": ["A", "B"]},  # expected_added
                {},  # expected_cancelled (no cancellations)
            ),
            (
                ["A", "B", "C"],  # initial_group
                [],  # current_group
                {},  # expected_added (no additions)
                {"test_subgraph": ["A", "B", "C"]},  # expected_cancelled
            ),
            (
                ["A", "B"],  # initial_group
                ["A", "B"],  # current_group
                {},  # expected_added (no additions)
                {},  # expected_cancelled (no cancellations)
            ),
            (
                ["A"],  # initial_group
                ["B"],  # current_group
                {"test_subgraph": ["B"]},  # expected_added
                {"test_subgraph": ["A"]},  # expected_cancelled
            ),
        ],
    )
    def test_get_indexer_selections(
        self,
        sample_data,
        initial_group,
        current_group,
        expected_added,
        expected_cancelled,
        mock__provider,
    ):
        """
        This test verifies the get_indexer_selections method correctly identifies the
        recent added and cancelled indexers.
        """
        with patch("iisa.indexer_selection.IndexerSelector._process_data"):
            # Create a IndexerSelector instance
            processor = IndexerSelector(
                history=sample_data,
                deployment_id=DeploymentId("test_subgraph"),
            )

        processor.initial_group = initial_group
        processor.current_group = current_group

        # Call the method under test
        added, cancelled = processor.get_indexer_selections()

        # Sort the lists within the dictionaries
        added_sorted = {k: sorted(v) for k, v in added.items()}
        cancelled_sorted = {k: sorted(v) for k, v in cancelled.items()}
        expected_added_sorted = {k: sorted(v) for k, v in expected_added.items()}
        expected_cancelled_sorted = {k: sorted(v) for k, v in expected_cancelled.items()}

        # Verify the results by comparing sorted dictionaries
        assert added_sorted == expected_added_sorted, (
            f"Expected added: {expected_added_sorted}, but got: {added_sorted}"
        )
        assert cancelled_sorted == expected_cancelled_sorted, (
            f"Expected cancelled: {expected_cancelled_sorted}, but got: {cancelled_sorted}"
        )

    def test_get_indexer_selections_empty_groups(self, sample_data, mock__provider):
        """get_indexer_selections returns empty added/cancelled when both groups are empty."""
        with patch("iisa.indexer_selection.IndexerSelector._process_data"):
            processor = IndexerSelector(
                history=sample_data,
                deployment_id=DeploymentId("test_subgraph"),
            )

        processor.initial_group = []
        processor.current_group = set()

        added, cancelled = processor.get_indexer_selections()

        # Verify that no indexers were added or cancelled.
        assert added == {}
        assert cancelled == {}

    @pytest.fixture
    def processor(self, sample_data, mock__provider):
        return IndexerSelector(
            history=sample_data,
            deployment_id=DeploymentId("test_subgraph"),
        )

    def test_get_current_group_normal_case(self, processor):
        """
        Test _get_current_group with multiple indexers assigned to the subgraph.
        """
        processor.existing_agreements = {
            "test_subgraph": ["A", "B", "D"],
            "other_subgraph": ["A", "C"],
            "another_subgraph": ["D"],
        }
        result = processor._get_current_group()
        expected = ["A", "B", "D"]
        assert set(result) == set(expected)

    def test_get_current_group_no_assigned_indexers(self, processor):
        """
        Test _get_current_group when no indexers are assigned to the subgraph.
        """
        processor.existing_agreements = {
            "A": ["other_subgraph"],
            "B": ["another_subgraph"],
            "C": ["yet_another_subgraph"],
        }
        result = processor._get_current_group()
        assert result == []

    def test_get_current_group_empty_agreements(self, processor):
        """
        Test _get_current_group with empty existing_agreements.
        """
        processor.existing_agreements = {}
        result = processor._get_current_group()
        assert result == []

    def test_get_current_group_subgraph_not_in_agreements(self, processor, mock__provider):
        """
        Test _get_current_group when the subgraph 'test_subgraph' is not in any agreement.
        """
        processor.existing_agreements = {
            "A": ["other_subgraph1", "other_subgraph2"],
            "B": ["other_subgraph3", "other_subgraph4"],
        }
        result = processor._get_current_group()
        assert result == []

    @patch("iisa.indexer_selection._normalize_metrics")
    @patch("iisa.indexer_selection._calculate_weighted_scores")
    def test_normalize_and_score(
        self, mock_calculate_scores, mock_normalize, sample_data, mock__provider
    ):
        """_normalize_and_score normalises, then scores every indexer in one call.

        Checks normalize_metrics gets the raw data, the vectorised scorer is
        called once with the full weights dict, and the result carries a
        weighted_score column.
        """
        # Create a IndexerSelector instance
        with patch("iisa.indexer_selection.IndexerSelector._process_data"):
            processor = IndexerSelector(
                history=sample_data,
                deployment_id=DeploymentId("test_subgraph"),
            )

        # Set up mock return values
        normalized_data = sample_data.copy()
        for metric in [
            "stake_to_fees",
            "base_price_per_epoch",
            "lat_lin_reg_coefficient",
            "uptime_score",
            "success_rate",
            "price_per_entity",
        ]:
            normalized_data[f"norm_{metric}"] = normalized_data.get(metric, 0.5)
        mock_normalize.return_value = normalized_data
        mock_calculate_scores.return_value = pd.Series(0.8, index=normalized_data.index)

        # Act
        result = processor._normalize_and_score()

        # normalize_metrics was called with the raw data
        mock_normalize.assert_called_once()
        pd.testing.assert_frame_equal(mock_normalize.call_args[0][0], sample_data)

        # The vectorised scorer is called once, with (normalized_frame, weights)
        mock_calculate_scores.assert_called_once()
        args, _ = mock_calculate_scores.call_args
        assert len(args) == 2
        weights = args[1]
        assert isinstance(weights, dict)
        expected_metrics = [
            "stake_to_fees",
            "base_price_per_epoch",
            "lat_lin_reg_coefficient",
            "uptime_score",
            "success_rate",
            "price_per_entity",
        ]
        assert all(metric in weights for metric in expected_metrics)
        assert pytest.approx(sum(weights.values())) == 1.0

        # weighted_score column exists with the scorer's values
        assert "weighted_score" in result.columns
        expected_scores = pd.Series(
            [0.8] * len(sample_data), name="weighted_score", index=result.index
        )
        pd.testing.assert_series_equal(result["weighted_score"], expected_scores)

    @pytest.mark.parametrize(
        "initial_group, expected_calls, expected_final_group",
        [
            (
                [],  # initial_group
                3,  # expected_calls
                ["B", "C", "D"],  # expected_final_group
            ),
            (
                ["A"],  # initial_group
                2,  # expected_calls
                ["A", "B", "C"],  # expected_final_group
            ),
            (
                ["A", "B"],  # initial_group
                1,  # expected_calls
                ["A", "B", "B"],  # expected_final_group
            ),
            (
                ["A", "B", "C"],  # initial_group
                0,  # expected_calls
                ["A", "B", "C"],  # expected_final_group
            ),
        ],
    )
    def test_add_indexers_to_group(
        self,
        sample_data,
        initial_group,
        expected_calls,
        expected_final_group,
        mock__provider,
    ):
        """_add_indexers_to_group fills the group to target_size.

        Adds until 3 indexers, stops when no suitable candidate remains, and
        handles different initial group sizes.
        """
        processor = IndexerSelector(
            history=sample_data,
            deployment_id=DeploymentId("test_subgraph"),
        )

        with patch(
            "iisa.indexer_selection.IndexerSelector._find_best_replacement_or_select_best_indexer"
        ) as mock_select:
            mock_select.side_effect = ["B", "C", "D", None]
            processor.current_group = initial_group.copy()

            processor._add_indexers_to_group()

            assert processor.current_group == expected_final_group
            assert mock_select.call_count == expected_calls

            # Check intermediate states
            for i in range(expected_calls):
                mock_select.assert_any_call()

        # Test when no suitable indexers are found
        with patch(
            "iisa.indexer_selection.IndexerSelector._find_best_replacement_or_select_best_indexer",
            return_value=None,
        ):
            processor.current_group = ["A"]
            processor._add_indexers_to_group()
            assert processor.current_group == ["A"]

    def test_meets_decentralization_requirements(self, mock__provider):
        """_meets_decentralization_requirements enforces the 2-orgs/2-locations floor.

        Groups under 2 indexers always pass; 2+ need 2+ unique locations and orgs;
        replacing_indexer simulates a swap.
        """
        processor = IndexerSelector(
            history=pd.DataFrame(
                {
                    "indexer": ["A", "B", "C", "D"],
                    "destination_loc": ["loc1", "loc1", "loc2", "loc3"],
                    "org": ["org1", "org1", "org2", "org3"],
                }
            ),
            deployment_id=DeploymentId("test_subgraph"),
        )

        # Test adding first indexer (resulting group has 1 indexer - no check needed)
        processor.current_group = []
        assert processor._meets_decentralization_requirements("A")

        # Test adding second indexer - same location and org (fails decentralization)
        processor.current_group = ["A"]
        assert not processor._meets_decentralization_requirements("B")  # A,B both loc1/org1

        # Test adding second indexer - different location and org (passes)
        processor.current_group = ["A"]
        assert processor._meets_decentralization_requirements("C")  # A=loc1/org1, C=loc2/org2

        # Test with 2 indexers, adding third with different location and org
        processor.current_group = ["A", "B"]
        assert processor._meets_decentralization_requirements("C")  # C adds loc2 and org2

        # Test with 2 indexers that already meet requirements, adding any third is fine
        processor.current_group = ["A", "C"]
        assert processor._meets_decentralization_requirements("D")  # Adds loc3 and org3
        assert processor._meets_decentralization_requirements("B")  # Already have 2 locs/orgs

        # Test replacement scenario: replacing A (loc1/org1) with C (loc2/org2) in group [A, B]
        # Results in [B, C] = loc1/org1 + loc2/org2 = 2 locs, 2 orgs (passes)
        processor.current_group = ["A", "B"]
        assert processor._meets_decentralization_requirements("C", replacing_indexer="A")

        # Test replacement scenario: replacing C (loc2/org2) with B (loc1/org1) in group [A, C]
        # Results in [A, B] = loc1/org1 + loc1/org1 = 1 loc, 1 org (fails)
        processor.current_group = ["A", "C"]
        assert not processor._meets_decentralization_requirements("B", replacing_indexer="C")

        # Test with duplicate indexer (edge case)
        processor.current_group = ["A", "A"]
        assert not processor._meets_decentralization_requirements("A")

    def test_meets_decentralization_requirements_edge_cases(self, mock__provider):
        """
        Test _meets_decentralization_requirements with various edge cases.
        """
        processor = IndexerSelector(
            history=pd.DataFrame(
                {
                    "indexer": ["A", "B", "C", "D", "E", "F"],
                    "destination_loc": ["loc1", "loc1", "loc2", "loc2", "loc3", "loc3"],
                    "org": ["org1", "org2", "org1", "org2", "org3", "org1"],
                }
            ),
            deployment_id=DeploymentId("test_subgraph"),
        )

        # Test with empty current group (adding first indexer)
        processor.current_group = []
        assert processor._meets_decentralization_requirements("A")

        # Test adding second indexer with different org (A=loc1/org1, B=loc1/org2)
        # Same location but different org - fails (needs 2 locs AND 2 orgs)
        processor.current_group = ["A"]
        assert not processor._meets_decentralization_requirements("B")  # Same loc

        # Test adding second indexer with different location and org
        processor.current_group = ["A"]
        assert processor._meets_decentralization_requirements("D")  # A=loc1/org1, D=loc2/org2

        # Test with indexer 'A' selected twice due to some error, adding diverse indexer
        processor.current_group = ["A", "A"]
        assert processor._meets_decentralization_requirements("D")  # D=loc2/org2 adds diversity

        # Test with many indexers already in group (decentralization already met)
        processor.current_group = ["A", "B", "C", "D", "E", "F"]
        assert processor._meets_decentralization_requirements("F")

        # Test adding same indexer that's already in group (fails - duplicates don't add diversity)
        processor.current_group = ["A", "B"]  # loc1/org1 + loc1/org2 = 1 loc, 2 orgs (fails)
        assert not processor._meets_decentralization_requirements("A")

        # Test that two diverse indexers pass
        processor.current_group = ["A", "D"]  # loc1/org1 + loc2/org2 = 2 locs, 2 orgs
        assert processor._meets_decentralization_requirements("E")  # Adds more diversity

    def test_replace_underperforming_indexers_replaces_low_scorer(self, mock__provider):
        """
        Test replacement when indexer scores below MIN_INDEXER_SCORE and
        candidate exceeds current + REPLACEMENT_MARGIN.
        """
        history = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D"],
                "destination_loc": ["loc1", "loc2", "loc3", "loc4"],
                "org": ["org1", "org2", "org3", "org4"],
            }
        )
        processor = IndexerSelector(
            history=history,
            deployment_id=DeploymentId("test_subgraph"),
        )

        # Manually set weighted_score after initialization
        # A=0.10 (below MIN_INDEXER_SCORE=0.15), D=0.70 (> 0.10 + 0.50 = 0.60)
        processor.data.loc[processor.data["indexer"] == "A", "weighted_score"] = 0.10
        processor.data.loc[processor.data["indexer"] == "B", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "C", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "D", "weighted_score"] = 0.70

        processor.current_group = ["A", "B", "C"]
        processor._replace_underperforming_indexers()

        # A (0.10) should be replaced with D (0.70) since 0.70 > 0.10 + 0.50
        assert "D" in processor.current_group
        assert "A" not in processor.current_group
        assert len(processor.current_group) == 3

    def test_replace_underperforming_indexers_keeps_adequate_performers(self, mock__provider):
        """
        Test that indexers scoring >= MIN_INDEXER_SCORE are not replaced,
        even if better candidates exist.
        """
        history = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D"],
                "destination_loc": ["loc1", "loc2", "loc3", "loc4"],
                "org": ["org1", "org2", "org3", "org4"],
            }
        )
        processor = IndexerSelector(
            history=history,
            deployment_id=DeploymentId("test_subgraph"),
        )

        # A=0.20 >= MIN_INDEXER_SCORE, so not eligible for replacement
        processor.data.loc[processor.data["indexer"] == "A", "weighted_score"] = 0.20
        processor.data.loc[processor.data["indexer"] == "B", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "C", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "D", "weighted_score"] = 0.90

        processor.current_group = ["A", "B", "C"]
        processor._replace_underperforming_indexers()

        # No replacement - all indexers are above MIN_INDEXER_SCORE (0.15)
        assert processor.current_group == ["A", "B", "C"]

    def test_replace_underperforming_indexers_margin_not_met(self, mock__provider):
        """
        Test that no replacement occurs when candidate doesn't exceed
        current + REPLACEMENT_MARGIN.
        """
        history = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D"],
                "destination_loc": ["loc1", "loc2", "loc3", "loc4"],
                "org": ["org1", "org2", "org3", "org4"],
            }
        )
        processor = IndexerSelector(
            history=history,
            deployment_id=DeploymentId("test_subgraph"),
        )

        # A=0.10 (below threshold), D=0.55 (< 0.10 + 0.50 = 0.60)
        processor.data.loc[processor.data["indexer"] == "A", "weighted_score"] = 0.10
        processor.data.loc[processor.data["indexer"] == "B", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "C", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "D", "weighted_score"] = 0.55

        processor.current_group = ["A", "B", "C"]
        processor._replace_underperforming_indexers()

        # No replacement - D (0.55) doesn't exceed A (0.10) + REPLACEMENT_MARGIN (0.50)
        assert processor.current_group == ["A", "B", "C"]

    def test_replace_underperforming_indexers_multiple_swaps(self, mock__provider):
        """
        Test iterative replacement when multiple indexers are below threshold.
        """
        # Need 5 indexers with diverse locations/orgs
        history = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "destination_loc": ["loc1", "loc2", "loc3", "loc4", "loc5"],
                "org": ["org1", "org2", "org3", "org4", "org5"],
            }
        )
        processor = IndexerSelector(
            history=history,
            deployment_id=DeploymentId("test_subgraph"),
        )

        # A=0.05, B=0.08 (both below MIN_INDEXER_SCORE)
        # D=0.80 > 0.05+0.50, E=0.75 > 0.08+0.50
        processor.data.loc[processor.data["indexer"] == "A", "weighted_score"] = 0.05
        processor.data.loc[processor.data["indexer"] == "B", "weighted_score"] = 0.08
        processor.data.loc[processor.data["indexer"] == "C", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "D", "weighted_score"] = 0.80
        processor.data.loc[processor.data["indexer"] == "E", "weighted_score"] = 0.75

        processor.current_group = ["A", "B", "C"]
        processor._replace_underperforming_indexers()

        # A and B should be replaced with D and E
        assert "A" not in processor.current_group
        assert "B" not in processor.current_group
        assert "C" in processor.current_group
        assert len(processor.current_group) == 3

    def test_replace_underperforming_indexers_skips_newly_added(self, mock__provider):
        """
        Test that newly added indexers are not eligible for replacement in the same call.
        """
        history = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "destination_loc": ["loc1", "loc2", "loc3", "loc4", "loc5"],
                "org": ["org1", "org2", "org3", "org4", "org5"],
            }
        )
        processor = IndexerSelector(
            history=history,
            deployment_id=DeploymentId("test_subgraph"),
        )

        # A=0.05 (below threshold), D and E are good replacements
        processor.data.loc[processor.data["indexer"] == "A", "weighted_score"] = 0.05
        processor.data.loc[processor.data["indexer"] == "B", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "C", "weighted_score"] = 0.50
        processor.data.loc[processor.data["indexer"] == "D", "weighted_score"] = 0.80
        processor.data.loc[processor.data["indexer"] == "E", "weighted_score"] = 0.85

        processor.current_group = ["A", "B", "C"]
        processor._replace_underperforming_indexers()

        # A replaced with best candidate (D or E), newly added indexer not re-evaluated
        assert "A" not in processor.current_group
        assert len(processor.current_group) == 3

    def test_find_best_replacement_or_select_best_indexer(self, mock__provider):
        """_find_best_replacement_or_select_best_indexer picks the best eligible candidate.

        Returns the best replacement meeting decentralisation, None when none fit,
        and skips denylisted indexers.
        """
        processor = IndexerSelector(
            history=pd.DataFrame(
                {
                    "indexer": ["A", "B", "C", "D", "E"],
                    "weighted_score": [0.9, 0.8, 0.7, 0.6, 0.5],
                    "destination_loc": ["loc1", "loc2", "loc3", "loc4", "loc5"],
                    "org": ["org1", "org2", "org3", "org4", "org5"],
                }
            ),
            deployment_id=DeploymentId("test_subgraph"),
        )

        processor.current_group = ["A", "B", "C"]
        processor.indexer_denylist = ["E"]

        with patch(
            "iisa.indexer_selection.IndexerSelector._meets_decentralization_requirements"
        ) as mock_decentralization:
            mock_decentralization.side_effect = [True]

            result = processor._find_best_replacement_or_select_best_indexer()

            # Verify the best replacement is D, not E, due to indexer_denylisting.
            assert result == "D"

            # Verify the number of decentralization requirement checks
            assert mock_decentralization.call_count == 1

    def test_update_indexer_denylist_cancel_indexing_agreements(self, sample_data, mock__provider):
        """update_indexer_denylist_cancel_indexing_agreements cancels denied agreements.

        Identifies agreements to cancel from the denylist, returns them, and
        updates the internal denylist.
        """
        # Initialize IndexerSelector
        processor = IndexerSelector(
            history=sample_data,
            deployment_id=DeploymentId("test_subgraph"),
            existing_agreements={
                "subgraph1": ["A"],
                "subgraph2": ["A", "B"],
                "subgraph3": ["B"],
                "subgraph4": ["A", "D"],
                "subgraph5": ["B"],
                "subgraph6": ["F"],
                "subgraph7": ["A"],
                "subgraph9": ["B", "F"],
                "subgraph10": ["A", "C"],
                "subgraph11": ["E"],
                "subgraph12": ["B", "E"],
                "subgraph14": ["E"],
                "subgraph15": ["E"],
                "subgraph16": ["F"],
                "subgraph20": ["C"],
                "subgraph23": ["F"],
                "subgraph40": ["C"],
                "subgraph41": ["F"],
                "subgraph45": ["F"],
                "subgraph70": ["C"],
                "subgraph100": ["C"],
            },
            indexer_denylist=[IndexerId("H")],
        )

        # update the indexer_denylist to cancel agreements
        new_indexer_denylist = [
            IndexerId("H"),
            IndexerId("B"),
            IndexerId("E"),
            IndexerId("NOT_IN_LIST"),
        ]

        # Call update_indexer_denylist_cancel_indexing_agreements with new new_indexer_denylist
        newly_cancelled_agreements = processor.update_indexer_denylist_cancel_indexing_agreements(
            new_indexer_denylist
        )
        expected_newly_cancelled_agreements = {
            "B": ["subgraph2", "subgraph3", "subgraph5", "subgraph9", "subgraph12"],
            "E": ["subgraph11", "subgraph12", "subgraph14", "subgraph15"],
        }

        # Check state after update
        print("Newly cancelled indexing agreements: ", newly_cancelled_agreements)
        assert newly_cancelled_agreements == expected_newly_cancelled_agreements

        # Verify that the indexer_denylist has been updated
        assert processor.indexer_denylist == new_indexer_denylist

        # Verify that 'H' and 'NOT_IN_LIST' don't appear in cancelled agreements
        assert "H" not in newly_cancelled_agreements
        assert "NOT_IN_LIST" not in newly_cancelled_agreements


def _selector_with_scores(scores, group, synced_indexers=None):
    """Selector over indexers with distinct orgs and locations, weighted_score set by hand."""
    indexers = list(scores)
    processor = IndexerSelector(
        history=pd.DataFrame(
            {
                "indexer": indexers,
                "destination_loc": [f"loc{i}" for i in range(len(indexers))],
                "org": [f"org{i}" for i in range(len(indexers))],
            }
        ),
        deployment_id=DeploymentId("test_subgraph"),
        synced_indexers=synced_indexers,
    )
    for indexer, score in scores.items():
        processor.data.loc[processor.data["indexer"] == indexer, "weighted_score"] = score
    processor.current_group = list(group)
    return processor


def _selection_log(caplog, *fragments):
    """Messages from the selection module containing any of ``fragments``, in order.

    The selector selects, and logs, on construction, so callers clear caplog first.
    """
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "iisa.indexer_selection" and any(f in r.getMessage() for f in fragments)
    ]


class TestReplaceUnderperformingIndexersOrder:
    """Pins the swap order, tie rule and log lines of _replace_underperforming_indexers."""

    LOOP_LOG = ("no replacement needed", "replaced indexer", "no more beneficial")

    def test_swaps_largest_improvement_first_and_logs_each_pass(self, caplog):
        # B comes first in the group but A gains more from the same candidate D.
        processor = _selector_with_scores(
            {"A": 0.05, "B": 0.08, "C": 0.50, "D": 0.80, "E": 0.75}, ["B", "A", "C"]
        )

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="iisa.indexer_selection"):
            processor._replace_underperforming_indexers()

        assert processor.current_group == ["C", "D", "E"]
        threshold_line = (
            "deployment=test_subgraph indexer C score=0.5000 >= threshold=0.15, "
            "no replacement needed"
        )
        assert _selection_log(caplog, *self.LOOP_LOG) == [
            threshold_line,
            "deployment=test_subgraph replaced indexer A with D (improvement=0.7500)",
            threshold_line,
            "deployment=test_subgraph replaced indexer B with E (improvement=0.6700)",
            threshold_line,
            "deployment=test_subgraph no more beneficial replacements found",
        ]

    def test_equal_improvement_keeps_the_earlier_group_member(self):
        processor = _selector_with_scores(
            {"A": 0.05, "B": 0.05, "C": 0.50, "D": 0.80, "E": 0.10}, ["A", "B", "C"]
        )

        processor._replace_underperforming_indexers()

        assert processor.current_group == ["B", "C", "D"]

    def test_group_member_without_a_row_is_left_alone(self):
        processor = _selector_with_scores({"A": 0.05, "C": 0.50, "D": 0.80}, ["X", "A", "C"])

        processor._replace_underperforming_indexers()

        assert processor.current_group == ["X", "C", "D"]


class TestCandidatePools:
    """Pins how _find_best_replacement_or_select_best_indexer orders synced candidates."""

    SCORES = {"A": 0.90, "B": 0.80, "C": 0.50, "D": 0.65, "E": 0.30}

    def test_threshold_applies_once_group_has_a_synced_indexer(self, caplog):
        processor = _selector_with_scores(self.SCORES, ["C"], synced_indexers={"c", "d", "e"})

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="iisa.indexer_selection"):
            result = processor._find_best_replacement_or_select_best_indexer()

        assert result == "D"
        assert _selection_log(caplog, "candidates:", "selected") == [
            "deployment=test_subgraph candidates: 1 synced eligible, 3 unsynced "
            "(group_has_synced=True)",
            "deployment=test_subgraph selected synced indexer D "
            "(score=0.6500, meets decentralization)",
        ]

    def test_first_synced_indexer_skips_the_threshold(self, caplog):
        processor = _selector_with_scores(self.SCORES, ["A"], synced_indexers={"c", "d", "e"})

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="iisa.indexer_selection"):
            result = processor._find_best_replacement_or_select_best_indexer()

        assert result == "D"
        assert _selection_log(caplog, "candidates:", "selected") == [
            "deployment=test_subgraph candidates: 3 synced eligible, 1 unsynced "
            "(group_has_synced=False)",
            "deployment=test_subgraph selected synced indexer D "
            "(score=0.6500, meets decentralization)",
        ]

    def test_without_synced_indexers_draws_from_one_pool(self, caplog):
        processor = _selector_with_scores(self.SCORES, ["A"])

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="iisa.indexer_selection"):
            result = processor._find_best_replacement_or_select_best_indexer()

        assert result == "B"
        assert _selection_log(caplog, "candidates:", "selected") == [
            "deployment=test_subgraph selected all indexer B "
            "(score=0.8000, meets decentralization)",
        ]


class TestNormalizeMetrics:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "Latency Coefficient + Error Confidence Interval": [
                    -5,
                    0,
                    5,
                    10,
                    12.121212,
                ],
                "% up_x": [0, 10, 50, 75.7575, 99.9],
                "stake_to_fees": [-5.15, 0, 1.125, 3, 120],
                "average_status": [0, 1, 50, 75.7575, 99.9],
                "base_price_per_epoch": [10, 200, 300, 400.457, 1000],
                "price_per_entity": [0.1, 0.2, 0.3, 0.4, 0.5],
                "other_column": ["A", 1, "B", 12.12, np.nan],
            }
        )

    def test_normalize_metrics_full_run_base_case(self, sample_df):
        # Compute the result
        result = _normalize_metrics(sample_df)

        # Check all expected columns are present.
        expected_columns = [
            # Original columns
            "Latency Coefficient + Error Confidence Interval",
            "% up_x",
            "stake_to_fees",
            "average_status",
            "base_price_per_epoch",
            "price_per_entity",
            "other_column",
            # New columns
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_stake_to_fees",
            "norm_success_rate",
            "norm_base_price_per_epoch",
            "norm_price_per_entity",
        ]
        for col in expected_columns:
            assert col in result.columns

        # Check all normalized values are between 0 and 1
        normalized_columns = [
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_stake_to_fees",
            "norm_success_rate",
            "norm_base_price_per_epoch",
            "norm_price_per_entity",
        ]
        for col in normalized_columns:
            assert result[col].between(0, 1).all()

    def test_normalize_generic(self):
        # Test the normalize_generic function
        series = pd.Series([-1000, 0, 345.234, 4, 5000])
        result = _normalize_generic(series)
        assert result.min() == 0
        assert result.max() == 1
        assert len(result) == len(series)

    def test_normalize_uptime_and_success_rate(self):
        # Test the normalize_uptime_and_success_rate function
        series = pd.Series([0, 12.121212, 98, 99, 100])
        result = _normalize_uptime_and_success_rate(series)
        assert result.max() == 1
        assert result.min() == 0
        assert len(result) == len(series)

    def test_empty_dataframe(self, sample_df):
        # Test with an empty DataFrame
        empty_df = pd.DataFrame(columns=sample_df.columns)
        result = _normalize_metrics(empty_df)
        assert result.empty
        expected_columns = list(empty_df.columns) + [
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_stake_to_fees",
            "norm_success_rate",
            "norm_base_price_per_epoch",
            "norm_price_per_entity",
        ]
        assert set(result.columns) == set(expected_columns)

    def test_all_same_values(self, sample_df):
        # Test with all values being the same
        sample_df.loc[:, :] = 1000

        # Call normalize_metrics function
        result = _normalize_metrics(sample_df)

        norm_columns = [
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_stake_to_fees",
            "norm_success_rate",
            "norm_base_price_per_epoch",
            "norm_price_per_entity",
        ]

        # Check for normalization results where input values are the same
        for column in norm_columns:
            if column in [
                "norm_stake_to_fees",
            ]:
                assert (result[column] == 0).all(), (
                    f"Column {column} is not 0 for identical input values"
                )

            elif column in [
                "norm_lat_lin_reg_coefficient",
            ]:
                assert (result[column] == 1).all(), (
                    f"Column {column} is not 1 for identical input values"
                )

    def test_negative_values(self, sample_df):
        # Test with negative values (7 columns)
        sample_df.loc[0] = [-1, -1, -1, -1, -1, -1, -1]
        sample_df.loc[1] = [-100, -50, -25, -10, -5, -1, -1]
        sample_df.loc[2] = [0, 0, 0, 0, 0, 0, 0]
        sample_df.loc[3] = [1, 1, 1, 1, 1, 1, 1]
        sample_df.loc[4] = [-1000, 0, -500, 500, -250, 250, 250]

        # Compute result
        result = _normalize_metrics(sample_df)

        # Check negative numbers don't create np.nan's in the result
        assert not result.isnull().any().any()

        norm_columns = result.columns[result.columns.str.startswith("norm_")]
        for col in norm_columns:
            min_val = result[col].min()
            max_val = result[col].max()

            # Make sure results are normalized correctly.
            assert min_val >= 0 and max_val <= 1
            assert not result[col].isin([np.inf, -np.inf]).any()

    def test_all_negative_values(self, sample_df):
        # Test with all negative values
        sample_df.loc[:, :] = -1
        result = _normalize_metrics(sample_df)

        # Check that the function handles all negative values as expected
        norm_columns = [col for col in result.columns if col.startswith("norm_")]

        for col in norm_columns:
            assert result[col].between(0, 1).all(), (
                f"Column {col} contains values outside [0, 1] range"
            )

        assert not result[norm_columns].isnull().any().any(), (
            "Result contains unexpected NaN values"
        )

    def test_nan_values(self, sample_df):
        # Test with NaN values
        sample_df.loc[0] = [np.nan] * len(sample_df.columns)
        result = _normalize_metrics(sample_df)

        # Check that NaN values are not present in other rows of normalized columns
        assert not result.iloc[1:, result.columns.str.startswith("norm_")].isnull().any().any()

    def test_price_normalization(self):
        """Test that price columns normalize correctly (lower is better)."""
        df = pd.DataFrame(
            {
                "Latency Coefficient + Error Confidence Interval": [1, 2, 3],
                "% up_x": [99, 100, 98],
                "stake_to_fees": [0.1, 0.2, 0.3],
                "average_status": [99, 100, 98],
                "base_price_per_epoch": [100, 500, 1000],
                "price_per_entity": [0.1, 0.5, 1.0],
            }
        )
        result = _normalize_metrics(df)
        # Cheapest indexer should have highest score
        assert (
            result["norm_base_price_per_epoch"].iloc[0] == result["norm_base_price_per_epoch"].max()
        )
        assert result["norm_price_per_entity"].iloc[0] == result["norm_price_per_entity"].max()

    def test_price_ceiling_clamps_outliers(self):
        """When price_ceiling is provided, prices above it score 0."""
        df = pd.DataFrame(
            {
                "base_price_per_epoch": [10, 50, 200],
                "Latency Coefficient + Error Confidence Interval": [1, 1, 1],
                "% up_x": [99, 99, 99],
                "stake_to_fees": [1.0, 1.0, 1.0],
                "average_status": [0.99, 0.99, 0.99],
                "price_per_entity": [0.1, 0.1, 0.1],
            }
        )
        result = _normalize_metrics(df, price_ceiling=100)

        scores = result["norm_base_price_per_epoch"]
        # 10 GRT: 1 - (10/100) = 0.9
        assert scores.iloc[0] == pytest.approx(0.9)
        # 50 GRT: 1 - (50/100) = 0.5
        assert scores.iloc[1] == pytest.approx(0.5)
        # 200 GRT: 1 - (200/100) = -1.0, clipped to 0.0
        assert scores.iloc[2] == pytest.approx(0.0)

    def test_price_ceiling_none_falls_back_to_observed_max(self):
        """Without price_ceiling, observed max is the ceiling."""
        df = pd.DataFrame(
            {
                "base_price_per_epoch": [10, 50, 200],
                "Latency Coefficient + Error Confidence Interval": [1, 1, 1],
                "% up_x": [99, 99, 99],
                "stake_to_fees": [1.0, 1.0, 1.0],
                "average_status": [0.99, 0.99, 0.99],
                "price_per_entity": [0.1, 0.1, 0.1],
            }
        )
        result = _normalize_metrics(df, price_ceiling=None)

        scores = result["norm_base_price_per_epoch"]
        # ceiling = 200 (observed max)
        # 10 GRT: 1 - (10/200) = 0.95
        assert scores.iloc[0] == pytest.approx(0.95)
        # 200 GRT: 1 - (200/200) = 0.0
        assert scores.iloc[2] == pytest.approx(0.0)


class TestTargetSize:
    """Tests for variable target_size parameter in IndexerSelector."""

    @pytest.fixture
    def sample_data_with_scores(self):
        """Sample data with all required fields for IndexerSelector."""
        return pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "deployment_hash": ["hash1"] * 5,
                "destination_loc": ["loc1", "loc2", "loc3", "loc4", "loc5"],
                "org": ["org1", "org2", "org3", "org4", "org5"],
                "weighted_score": [0.9, 0.8, 0.7, 0.6, 0.5],
                "lat_lin_reg_coefficient": [0.1, 0.2, 0.3, 0.4, 0.5],
                "uptime_score": [0.9, 0.8, 0.7, 0.6, 0.5],
                "stake_to_fees": [0.1, 0.2, 0.3, 0.4, 0.5],
                "success_rate": [0.95, 0.90, 0.85, 0.80, 0.75],
                "base_price_per_epoch": [100, 200, 300, 400, 500],
                "price_per_entity": [0.1, 0.2, 0.3, 0.4, 0.5],
            }
        )

    def test_target_size_defaults_to_three(self, sample_data_with_scores):
        """Default target_size is 3."""
        # Arrange & Act
        with patch("iisa.indexer_selection.IndexerSelector._process_data"):
            processor = IndexerSelector(
                history=sample_data_with_scores,
                deployment_id=DeploymentId("test_subgraph"),
            )

        # Assert
        assert processor.target_size == 3

    def test_target_size_custom_value(self, sample_data_with_scores):
        """Custom target_size is stored correctly."""
        # Arrange & Act
        with patch("iisa.indexer_selection.IndexerSelector._process_data"):
            processor = IndexerSelector(
                history=sample_data_with_scores,
                deployment_id=DeploymentId("test_subgraph"),
                target_size=5,
            )

        # Assert
        assert processor.target_size == 5

    def test_target_size_one_selects_single_indexer(self, sample_data_with_scores):
        """With target_size=1, only one indexer is selected."""
        # Arrange & Act
        processor = IndexerSelector(
            history=sample_data_with_scores,
            deployment_id=DeploymentId("test_subgraph"),
            target_size=1,
        )

        # Assert
        assert len(processor.current_group) == 1

    def test_target_size_five_selects_five_indexers(self, sample_data_with_scores):
        """With target_size=5, five indexers are selected."""
        # Arrange & Act
        processor = IndexerSelector(
            history=sample_data_with_scores,
            deployment_id=DeploymentId("test_subgraph"),
            target_size=5,
        )

        # Assert
        assert len(processor.current_group) == 5

    def test_target_size_respects_available_indexers(self, sample_data_with_scores):
        """target_size > available indexers returns all available."""
        # Arrange & Act
        processor = IndexerSelector(
            history=sample_data_with_scores,
            deployment_id=DeploymentId("test_subgraph"),
            target_size=10,  # More than available
        )

        # Assert - Should have at most 5 (all available indexers)
        assert len(processor.current_group) <= 5

    def test_target_size_removes_excess_indexers(self, sample_data_with_scores):
        """Existing group larger than target_size gets trimmed."""
        # Arrange & Act
        processor = IndexerSelector(
            history=sample_data_with_scores,
            deployment_id=DeploymentId("test_subgraph"),
            existing_agreements={
                DeploymentId("test_subgraph"): [
                    IndexerId("A"),
                    IndexerId("B"),
                    IndexerId("C"),
                    IndexerId("D"),
                ]
            },
            target_size=2,
        )

        # Assert - Should have reduced to 2
        assert len(processor.current_group) == 2

    def test_target_size_adds_to_small_group(self, sample_data_with_scores):
        """Existing group smaller than target_size gets expanded."""
        # Arrange & Act
        processor = IndexerSelector(
            history=sample_data_with_scores,
            deployment_id=DeploymentId("test_subgraph"),
            existing_agreements={DeploymentId("test_subgraph"): [IndexerId("A")]},
            target_size=4,
        )

        # Assert - Should have expanded to 4
        assert len(processor.current_group) == 4


class TestSyncedIndexerPreference:
    """Tests for two-pool selection: prefer synced indexers."""

    @pytest.fixture
    def five_indexers(self):
        """Five indexers with distinct orgs/locations and descending scores.

        Raw metrics are varied so normalisation produces different
        weighted_scores. A is best, E is worst, all above 0.6.
        """
        return pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "deployment_hash": ["hash1"] * 5,
                "destination_loc": ["loc1", "loc2", "loc3", "loc4", "loc5"],
                "org": ["org1", "org2", "org3", "org4", "org5"],
                "Latency Coefficient + Error Confidence Interval": [
                    1.0,
                    1.1,
                    1.2,
                    1.3,
                    2.0,
                ],
                "% up_x": [100.0] * 5,
                "stake_to_fees": [10.0, 9.5, 9.0, 8.5, 5.0],
                "average_status": [1.0] * 5,
                "base_price_per_epoch": [50, 55, 60, 65, 100],
                "price_per_entity": [0.1, 0.11, 0.12, 0.13, 0.2],
            }
        )

    def test_prefers_synced_over_higher_scored_unsynced(self, five_indexers):
        """Synced indexer C (score 0.7) chosen over unsynced A (0.9)."""
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=1,
            synced_indexers={"C", "D"},
        )
        # C is the highest-scored synced indexer
        assert "C" in processor.current_group

    def test_falls_back_to_unsynced_when_synced_pool_empty(self, five_indexers):
        """No synced indexers provided — behaves as before."""
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=1,
            synced_indexers=set(),
        )
        assert "A" in processor.current_group

    def test_falls_back_when_synced_exhausted(self, five_indexers):
        """Need 3, only 1 synced — remaining 2 from unsynced."""
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=3,
            synced_indexers={"C"},
        )
        assert len(processor.current_group) == 3
        assert "C" in processor.current_group

    def test_synced_still_respects_denylist(self, five_indexers):
        """Synced but denylisted indexer is skipped."""
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=1,
            synced_indexers={"C", "D"},
            indexer_denylist=["C"],
        )
        assert "D" in processor.current_group

    def test_synced_still_respects_decentralization(self, five_indexers):
        """Synced indexers with same org — draws unsynced for diversity."""
        # Make C and D share org/location
        five_indexers.loc[five_indexers["indexer"] == "D", "org"] = "org3"
        five_indexers.loc[five_indexers["indexer"] == "D", "destination_loc"] = "loc3"
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=2,
            synced_indexers={"C", "D"},
        )
        assert len(processor.current_group) == 2
        # C selected from synced, but D skipped for decentralisation
        # — second slot filled from unsynced pool
        assert "C" in processor.current_group
        unsynced_in_group = [i for i in processor.current_group if i != "C"]
        assert unsynced_in_group[0] in {"A", "B", "E"}

    def test_empty_synced_set_backward_compatible(self, five_indexers):
        """synced_indexers=set() identical to synced_indexers=None."""
        result_empty = IndexerSelector(
            history=five_indexers.copy(),
            deployment_id="hash1",
            target_size=3,
            synced_indexers=set(),
        ).current_group

        result_none = IndexerSelector(
            history=five_indexers.copy(),
            deployment_id="hash1",
            target_size=3,
            synced_indexers=None,
        ).current_group

        assert set(result_empty) == set(result_none)

    def test_replacement_prefers_synced(self):
        """Replacement path also draws from synced pool first."""
        # A has terrible raw metrics so it scores below MIN_INDEXER_SCORE
        # after normalisation against the other 4
        df = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "deployment_hash": ["hash1"] * 5,
                "destination_loc": [
                    "loc1",
                    "loc2",
                    "loc3",
                    "loc4",
                    "loc5",
                ],
                "org": ["org1", "org2", "org3", "org4", "org5"],
                "Latency Coefficient + Error Confidence Interval": [
                    100,
                    1,
                    1,
                    1,
                    1,
                ],
                "% up_x": [10, 99, 99, 99, 99],
                "stake_to_fees": [0.001, 1.0, 1.0, 1.0, 1.0],
                "average_status": [0.1, 0.99, 0.99, 0.99, 0.99],
                "base_price_per_epoch": [900, 100, 100, 100, 100],
                "price_per_entity": [9.0, 0.1, 0.1, 0.1, 0.1],
            }
        )
        processor = IndexerSelector(
            history=df,
            deployment_id="hash1",
            existing_agreements={"hash1": ["A"]},
            target_size=1,
            synced_indexers={"D", "E"},
        )
        # A replaced — D or E selected from synced pool
        assert "A" not in processor.current_group
        assert processor.current_group[0] in {"D", "E"}

    def test_first_synced_preferred_regardless_of_score(self, five_indexers):
        """First synced pick ignores threshold to guarantee availability."""
        # E scores 0.20 (below 0.6) but is the only synced option
        # and no synced indexer is in the group yet — gets preferred
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=1,
            synced_indexers={"E"},
        )
        assert "E" in processor.current_group

    def test_second_synced_below_threshold_not_preferred(self, five_indexers):
        """After first synced pick, threshold applies to subsequent ones."""
        # Need 3 slots. C (0.72) and E (0.20) are synced. C takes the first
        # synced slot; E is below 0.6 so the threshold applies and E competes
        # on merit in the unsynced pool, losing to A and B.
        processor = IndexerSelector(
            history=five_indexers,
            deployment_id="hash1",
            target_size=3,
            synced_indexers={"C", "E"},
        )
        assert len(processor.current_group) == 3
        assert "C" in processor.current_group
        assert "E" not in processor.current_group


class TestDecentralizationBestEffort:
    """Tests for best-effort decentralization behavior."""

    def test_fallback_when_decentralization_not_possible(self):
        """When no indexer meets decentralization, still return best candidate."""
        # All indexers have the same org and location - decentralization impossible
        data = pd.DataFrame(
            {
                "indexer": ["A", "B", "C"],
                "deployment_hash": ["hash1"] * 3,
                "destination_loc": ["loc1", "loc1", "loc1"],  # Same location
                "org": ["org1", "org1", "org1"],  # Same org
                "weighted_score": [0.9, 0.8, 0.7],
                "lat_lin_reg_coefficient": [0.1, 0.2, 0.3],
                "uptime_score": [0.9, 0.8, 0.7],
                "stake_to_fees": [0.1, 0.2, 0.3],
                "success_rate": [0.95, 0.90, 0.85],
                "base_price_per_epoch": [100, 200, 300],
                "price_per_entity": [0.1, 0.2, 0.3],
            }
        )

        # Arrange & Act
        processor = IndexerSelector(
            history=data,
            deployment_id=DeploymentId("test_subgraph"),
            target_size=3,
        )

        # Assert - Should still select 3 indexers even though decentralization not met
        assert len(processor.current_group) == 3


class TestRowWiseWeightedScoreReference:
    """Pins the reference oracle's semantics, the spec the vectorised version meets."""

    @pytest.fixture
    def sample_weights(self):
        return {"metric1": 0.5, "metric2": 0.3, "metric3": 0.2}

    def test_basic_calculation(self, sample_weights):
        # Test the function with all metrics present
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": 0.6, "norm_metric3": 0.4})
        result = _row_wise_weighted_score(row, sample_weights)
        expected = (0.8 * 0.5 + 0.6 * 0.3 + 0.4 * 0.2) / 1.0
        assert np.isclose(result, expected)

    def test_missing_metric(self, sample_weights):
        # Test the function when one metric is missing (NaN)
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": np.nan, "norm_metric3": 0.4})
        result = _row_wise_weighted_score(row, sample_weights)
        expected = ((0.8 * 0.5) + (0 * 0.3) + (0.4 * 0.2)) / (0.5 + 0.2)
        assert np.isclose(result, expected)

    def test_all_metrics_missing(self, sample_weights):
        # Test the function when all metrics are missing (NaN)
        row = pd.Series({"norm_metric1": np.nan, "norm_metric2": np.nan, "norm_metric3": np.nan})
        with pytest.raises(ValueError, match="Total weight cannot be 0."):
            _row_wise_weighted_score(row, sample_weights)

    def test_zero_weights(self):
        # Test the function when all weights are zero
        weights = {"metric1": 0, "metric2": 0, "metric3": 0}
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": 0.6, "norm_metric3": 0.4})
        with pytest.raises(ValueError, match="Total weight cannot be 0."):
            _row_wise_weighted_score(row, weights)

    def test_partial_weights(self):
        # Test the function when some weights are zero
        weights = {"metric1": 0.5, "metric2": 0, "metric3": 0.5}
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": 0.6, "norm_metric3": 0.4})
        result = _row_wise_weighted_score(row, weights)
        expected = (0.8 * 0.5 + 0.4 * 0.5) / 1.0
        assert np.isclose(result, expected)

    def test_extra_metrics_in_row(self, sample_weights):
        # Test the function when the row contains extra metrics not in weights
        row = pd.Series(
            {
                "norm_metric1": 0.8,
                "norm_metric2": 0.6,
                "norm_metric3": 0.4,
                "norm_metric4": 1.0,
                "other_column": "value",
            }
        )
        result = _row_wise_weighted_score(row, sample_weights)
        expected = (0.8 * 0.5 + 0.6 * 0.3 + 0.4 * 0.2) / 1.0
        assert np.isclose(result, expected)

    @pytest.mark.parametrize(
        "row_data, weights, expected",
        [
            (
                {"norm_metric1": 1.0, "norm_metric2": 1.0},
                {"metric1": 1, "metric2": 1},
                1.0,
            ),
            (
                {"norm_metric1": 0.0, "norm_metric2": 0.0},
                {"metric1": 1, "metric2": 1},
                0.0,
            ),
            (
                {"norm_metric1": 0.5, "norm_metric2": 0.5},
                {"metric1": 1, "metric2": 1},
                0.5,
            ),
        ],
    )
    def test_edge_cases(self, row_data, weights, expected):
        # Test various edge cases
        row = pd.Series(row_data)
        result = _row_wise_weighted_score(row, weights)
        assert np.isclose(result, expected)


class TestCalculateWeightedScoresVectorized:
    """Vectorised _calculate_weighted_scores matches the row-wise scalar version."""

    @staticmethod
    def _row_wise(df, weights):
        return pd.Series(
            [_row_wise_weighted_score(df.iloc[i], weights) for i in range(len(df))],
            index=df.index,
        )

    def test_matches_scalar_on_full_clean_frame(self):
        """All norm_* columns present and finite: identical to the scalar version."""
        # Arrange
        df = pd.DataFrame(
            {
                "norm_stake_to_fees": [0.1, 0.9, 0.5],
                "norm_base_price_per_epoch": [0.8, 0.2, 0.4],
                "norm_lat_lin_reg_coefficient": [0.3, 0.7, 0.6],
                "norm_uptime_score": [0.95, 0.6, 0.99],
                "norm_success_rate": [0.9, 0.5, 0.85],
                "norm_price_per_entity": [0.2, 0.4, 0.7],
            }
        )

        # Act
        vectorised = _calculate_weighted_scores(df, DEFAULT_WEIGHTS)

        # Assert
        pd.testing.assert_series_equal(vectorised, self._row_wise(df, DEFAULT_WEIGHTS))

    def test_nan_cells_drop_from_denominator_like_scalar(self):
        """A NaN metric per row renormalises over the rest, matching the scalar."""
        df = pd.DataFrame(
            {
                "norm_a": [0.4, np.nan, 1.0],
                "norm_b": [np.nan, 0.5, 0.0],
                "norm_c": [0.8, 0.2, np.nan],
            }
        )
        weights = {"a": 0.5, "b": 0.3, "c": 0.2}

        vectorised = _calculate_weighted_scores(df, weights)

        pd.testing.assert_series_equal(vectorised, self._row_wise(df, weights))

    def test_missing_column_dropped_like_scalar(self):
        """A weight whose norm_ column is absent is skipped on both paths."""
        df = pd.DataFrame({"norm_a": [0.6, 0.2], "norm_b": [0.4, 0.8]})
        weights = {"a": 0.5, "b": 0.3, "c": 0.2}  # norm_c is absent

        vectorised = _calculate_weighted_scores(df, weights)

        pd.testing.assert_series_equal(vectorised, self._row_wise(df, weights))

    def test_row_with_no_usable_column_raises(self):
        """A row whose every weighted column is NaN raises ValueError like the scalar."""
        df = pd.DataFrame({"norm_a": [0.5, np.nan], "norm_b": [0.5, np.nan]})
        weights = {"a": 0.5, "b": 0.5}

        with pytest.raises(ValueError, match="Total weight cannot be 0"):
            _calculate_weighted_scores(df, weights)
