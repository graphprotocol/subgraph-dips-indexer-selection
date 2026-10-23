"""Tests for the DataManager class."""

import dataclasses
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from iisa import DataManager
from iisa.score_loader import ScoresSnapshot


class TestDataManager:
    """Tests for DataManager."""

    @pytest.fixture
    def mock_scores_df(self):
        """Create a mock DataFrame simulating the indexer_scores table."""
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x123"],
                "url": ["https://a.com/", "https://b.com/", "https://c.com/"],
                "lat_lin_reg_coefficient": [0.5, 0.3, 0.7],
                "lat_coefficient_std_error": [0.05, 0.03, 0.07],
                "lat_coefficient_upper_bound": [0.55, 0.33, 0.77],
                "lat_normalized_score": [0.8, 0.9, 0.6],
                "uptime_score": [0.98, 0.95, 0.99],
                "success_rate": [0.95, 0.90, 0.97],
                "stake_to_fees": [0.5, 1.0, 0.75],
                "norm_uptime_score": [0.9, 0.7, 0.95],
                "norm_success_rate": [0.85, 0.6, 0.9],
                "norm_stake_to_fees": [0.5, 0.8, 0.65],
                "org": ["hetzner", "amazon", "google"],
                "dst_lat": [50.0, 40.0, 35.0],
                "dst_lon": [10.0, -74.0, 139.0],
                "computed_at": [datetime(2024, 1, 15, 12, 0)] * 3,
                "query_count": [10000, 8000, 12000],
                "num_days": [28, 28, 28],
            }
        )

    @pytest.fixture
    def mock_provider_with_scores(self, mock_scores_df):
        """Create a mock provider that returns scores."""
        mock_provider = MagicMock()
        mock_provider.fetch_indexer_scores.return_value = (
            mock_scores_df,
            datetime(2024, 1, 15, 12, 0),
        )
        return mock_provider

    @pytest.fixture
    def mock_provider_empty_scores(self):
        """Create a mock provider that returns empty scores."""
        mock_provider = MagicMock()
        mock_provider.fetch_indexer_scores.return_value = (pd.DataFrame(), None)
        return mock_provider

    def test_load_scores_success(self, mock_provider_with_scores):
        """Test that load_scores() successfully loads and transforms data."""
        data_manager = DataManager(mock_provider_with_scores)

        result = data_manager.load_scores()

        assert result is True
        data = data_manager.get_data()
        assert data is not None
        assert len(data) == 3

    def test_load_scores_column_transformation(self, mock_provider_with_scores):
        """Test that load_scores() correctly transforms column names."""
        data_manager = DataManager(mock_provider_with_scores)
        data_manager.load_scores()

        data = data_manager.get_data()

        assert "Latency Coefficient + Error Confidence Interval" in data.columns
        assert "average_status" in data.columns
        assert "% up_x" in data.columns
        assert "destination_loc" in data.columns
        assert "norm_lat_lin_reg_coefficient" in data.columns
        assert "norm_stake_to_fees" in data.columns

    def test_load_scores_uptime_conversion(self, mock_provider_with_scores):
        """Test that uptime is converted from 0-1 to percentage."""
        data_manager = DataManager(mock_provider_with_scores)
        data_manager.load_scores()

        data = data_manager.get_data()

        # Original was 0.98, 0.95, 0.99 -> should become 98, 95, 99
        assert data["% up_x"].iloc[0] == pytest.approx(98.0)
        assert data["% up_x"].iloc[1] == pytest.approx(95.0)
        assert data["% up_x"].iloc[2] == pytest.approx(99.0)

    def test_transform_reads_uptime_as_a_number_before_scaling(self, mock_scores_df, caplog):
        """An uptime that does not read as a number becomes missing, so it scores 0, and is
        logged, rather than failing the push (an object) or repeating as text (a string).
        """
        data_manager = DataManager(MagicMock())
        mock_scores_df["uptime_score"] = pd.Series([{"a": 1}, "x", "0.5"], dtype=object)

        with caplog.at_level(logging.WARNING, logger="iisa.indexer_selection"):
            data = data_manager.transform_scores_df(mock_scores_df)

        assert data["% up_x"].tolist()[2] == pytest.approx(50.0)
        assert data["% up_x"].iloc[:2].isna().all()
        assert any("'uptime_score'" in r.getMessage() for r in caplog.records)

    def test_load_scores_destination_loc_creation(self, mock_provider_with_scores):
        """Test that destination_loc is created from lat/lon."""
        data_manager = DataManager(mock_provider_with_scores)
        data_manager.load_scores()

        data = data_manager.get_data()

        assert data["destination_loc"].iloc[0] == "50.0,10.0"
        assert data["destination_loc"].iloc[1] == "40.0,-74.0"
        assert data["destination_loc"].iloc[2] == "35.0,139.0"

    def test_transform_without_org_or_location_adds_them_empty(self, mock_scores_df, caplog):
        """Scores without the org or location columns, which selection reads, get them empty,
        and a warning names each one, so a selection can still run on them.
        """
        data_manager = DataManager(MagicMock())

        with caplog.at_level(logging.WARNING, logger="iisa.score_loader"):
            data = data_manager.transform_scores_df(mock_scores_df.drop(columns=["org", "dst_lon"]))

        assert data["org"].isna().all()
        assert data["destination_loc"].isna().all()
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("'org'" in message for message in warnings)
        assert any("'dst_lon'" in message for message in warnings)

    def test_load_scores_empty_table(self, mock_provider_empty_scores):
        """Test that load_scores() returns False when table is empty."""
        data_manager = DataManager(mock_provider_empty_scores)

        result = data_manager.load_scores()

        assert result is False
        assert data_manager.get_data() is None

    def test_load_scores_staleness_check(self, mock_provider_with_scores, caplog):
        """Test that staleness warnings are logged for old scores."""
        old_timestamp = datetime.now(timezone.utc) - timedelta(hours=100)
        mock_provider_with_scores.fetch_indexer_scores.return_value = (
            mock_provider_with_scores.fetch_indexer_scores.return_value[0],
            old_timestamp,
        )
        data_manager = DataManager(mock_provider_with_scores)

        import logging

        with caplog.at_level(logging.WARNING):
            data_manager.load_scores()

        assert any("stale" in record.message.lower() for record in caplog.records)

    def test_get_scores_age(self, mock_provider_with_scores):
        """Test that get_scores_age() returns correct age."""
        data_manager = DataManager(mock_provider_with_scores)

        assert data_manager.get_scores_age() is None

        data_manager.load_scores()

        age = data_manager.get_scores_age()
        assert age is not None
        assert isinstance(age, timedelta)

    def test_load_scores_preserves_precomputed_normalized_values(self, mock_provider_with_scores):
        """Test that pre-computed normalized values are preserved."""
        data_manager = DataManager(mock_provider_with_scores)
        data_manager.load_scores()

        data = data_manager.get_data()

        assert "norm_uptime_score" in data.columns
        assert "norm_success_rate" in data.columns
        assert data["norm_uptime_score"].iloc[0] == pytest.approx(0.9)
        assert data["norm_success_rate"].iloc[0] == pytest.approx(0.85)

    def test_load_scores_from_df_success(self, mock_scores_df):
        """load_scores_from_df accepts an in-memory DataFrame and runs the full transform."""
        data_manager = DataManager(MagicMock())

        computed_at = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        result = data_manager.load_scores_from_df(mock_scores_df, computed_at)

        assert result is True
        data = data_manager.get_data()
        assert data is not None
        assert len(data) == 3
        # Same transform as the file path
        assert "Latency Coefficient + Error Confidence Interval" in data.columns
        assert "% up_x" in data.columns
        assert data["% up_x"].iloc[0] == pytest.approx(98.0)
        assert data_manager.get_scores_age() is not None

    def test_load_scores_from_df_empty_df(self):
        """Empty DataFrame should return False and clear state."""
        data_manager = DataManager(MagicMock())
        result = data_manager.load_scores_from_df(pd.DataFrame(), datetime.now(timezone.utc))

        assert result is False
        assert data_manager.get_data() is None

    def test_transform_scores_df_empty_raises(self):
        """transform_scores_df must raise loudly on empty input.

        The push handler relies on this: an empty frame fails fast rather than
        silently zeroing the cache. The endpoint's 422 on empty bodies is a
        separate guard; this covers the method invariant itself.
        """
        data_manager = DataManager(MagicMock())

        with pytest.raises(ValueError, match="empty DataFrame"):
            data_manager.transform_scores_df(pd.DataFrame())

    def test_transform_scores_df_rows_without_fields_raise_payload_error(self):
        """Records that hold no fields make a frame with rows but no columns. That is a payload
        missing every required column, not an empty one, so the push can answer 422.
        """
        from iisa.score_loader import ScoresPayloadError

        data_manager = DataManager(MagicMock())

        with pytest.raises(ScoresPayloadError, match="indexer"):
            data_manager.transform_scores_df(pd.DataFrame([{}, {}]))

    def test_transform_scores_df_missing_required_column_raises(self, mock_scores_df):
        """A frame missing a required column raises ScoresPayloadError, naming it."""
        from iisa.score_loader import ScoresPayloadError

        data_manager = DataManager(MagicMock())
        incomplete = mock_scores_df.drop(columns=["success_rate"])

        with pytest.raises(ScoresPayloadError, match="success_rate"):
            data_manager.transform_scores_df(incomplete)

    def test_missing_required_columns_lists_only_absent(self, mock_scores_df):
        """missing_required_columns reports absent columns and nothing else."""
        from iisa.score_loader import missing_required_columns

        assert missing_required_columns(mock_scores_df) == []
        dropped = mock_scores_df.drop(columns=["indexer", "stake_to_fees"])
        assert sorted(missing_required_columns(dropped)) == ["indexer", "stake_to_fees"]

    def test_commit_scores_twice_keeps_most_recent(self, mock_scores_df):
        """Back-to-back commits must leave the most recent frame in memory."""
        data_manager = DataManager(MagicMock())

        df1 = data_manager.transform_scores_df(mock_scores_df)
        ts1 = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        data_manager.commit_scores(df1, ts1)

        df2_raw = mock_scores_df.copy()
        df2_raw["uptime_score"] = [0.50, 0.60, 0.70]  # distinguishable values
        df2 = data_manager.transform_scores_df(df2_raw)
        ts2 = datetime(2024, 1, 16, 12, 0, tzinfo=timezone.utc)
        data_manager.commit_scores(df2, ts2)

        current = data_manager.get_data()
        assert current is not None
        # Second commit's values should be what's in memory, not the first.
        assert list(current["% up_x"]) == pytest.approx([50.0, 60.0, 70.0])
        assert data_manager.snapshot.computed_at == ts2

    def test_snapshot_is_consistent_pair_across_commits(self, mock_scores_df):
        """Each snapshot observation carries data and computed_at from one push.

        The atomic-swap design closes the window where the two could disagree.
        A reader that captured a snapshot before a later commit keeps seeing
        the earlier push's values, detached from subsequent writes.
        """
        data_manager = DataManager(MagicMock())

        initial = data_manager.snapshot
        assert initial.data is None
        assert initial.computed_at is None

        ts1 = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        df1 = data_manager.transform_scores_df(mock_scores_df)
        data_manager.commit_scores(df1, ts1)
        snap1 = data_manager.snapshot

        assert snap1.computed_at == ts1
        assert snap1.data is not None
        assert list(snap1.data["% up_x"]) == pytest.approx([98.0, 95.0, 99.0])

        df2_raw = mock_scores_df.copy()
        df2_raw["uptime_score"] = [0.50, 0.60, 0.70]
        df2 = data_manager.transform_scores_df(df2_raw)
        ts2 = datetime(2024, 1, 16, 12, 0, tzinfo=timezone.utc)
        data_manager.commit_scores(df2, ts2)
        snap2 = data_manager.snapshot

        # Both fields advance together — never a mix.
        assert snap2.computed_at == ts2
        assert list(snap2.data["% up_x"]) == pytest.approx([50.0, 60.0, 70.0])

        # snap1 is still the first push's consistent pair, unaffected by the
        # second commit. Without the frozen dataclass, the second commit
        # could have mutated what a reader was observing.
        assert snap1.computed_at == ts1
        assert list(snap1.data["% up_x"]) == pytest.approx([98.0, 95.0, 99.0])

    def test_snapshot_is_frozen(self):
        """ScoresSnapshot must reject field mutation — the whole point of
        bundling is that a reader's reference cannot be tampered with after
        capture."""
        snap = ScoresSnapshot(data=None, computed_at=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            snap.data = pd.DataFrame()  # type: ignore[misc]
