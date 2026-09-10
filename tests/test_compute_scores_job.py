"""Test suite for the score computation CronJob.

Covers the new functions: normalization, schema transformation,
GeoIP utilities, and other helpers introduced for the CronJob.
"""

# Import from the jobs package - we need to add it to the path
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import processing  # noqa: E402
from processing import (  # noqa: E402
    adjust_rows,
    aggregate_indexer_info,
    calculate_indexer_success_rate,
    calculate_indexer_uptime,
    compute_all_scores,
    diagnose_geoip_failure,
    filter_successful_queries,
    hash_sampled_queries,
    haversine_vectorized,
    is_private_ip,
    iterative_filter,
    merge_and_prepare_dataframes,
    normalize_to_0_1,
    normalize_to_0_1_inverted,
    perform_latency_linear_regression,
    strategic_sample,
    transform_to_scores_schema,
)


class TestNormalizeToZeroOne:
    """Tests for the normalize_to_0_1 function."""

    def test_normalize_basic(self):
        # Arrange
        series = pd.Series([0, 50, 100])

        # Act
        result = normalize_to_0_1(series)

        # Assert
        expected = pd.Series([0.0, 0.5, 1.0])
        pd.testing.assert_series_equal(result, expected)

    def test_normalize_negative_values(self):
        # Arrange
        series = pd.Series([-100, 0, 100])

        # Act
        result = normalize_to_0_1(series)

        # Assert
        expected = pd.Series([0.0, 0.5, 1.0])
        pd.testing.assert_series_equal(result, expected)

    def test_normalize_constant_values(self):
        # Arrange
        series = pd.Series([5, 5, 5])

        # Act
        result = normalize_to_0_1(series)

        # Assert - all values should be 0.5 when constant
        expected = pd.Series([0.5, 0.5, 0.5])
        pd.testing.assert_series_equal(result, expected)

    def test_normalize_empty_series(self):
        # Arrange
        series = pd.Series([], dtype=float)

        # Act
        result = normalize_to_0_1(series)

        # Assert
        assert result.empty

    def test_normalize_single_value(self):
        # Arrange
        series = pd.Series([42])

        # Act
        result = normalize_to_0_1(series)

        # Assert - single value normalizes to 0.5
        expected = pd.Series([0.5])
        pd.testing.assert_series_equal(result, expected)

    def test_normalize_with_nan(self):
        # Arrange
        series = pd.Series([0, np.nan, 100])

        # Act
        result = normalize_to_0_1(series)

        # Assert - NaN should be preserved
        assert result.iloc[0] == 0.0
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 1.0


class TestNormalizeToZeroOneInverted:
    """Tests for the normalize_to_0_1_inverted function."""

    def test_normalize_inverted_basic(self):
        # Arrange
        series = pd.Series([0, 50, 100])

        # Act
        result = normalize_to_0_1_inverted(series)

        # Assert - inverted so 0 -> 1.0, 100 -> 0.0
        expected = pd.Series([1.0, 0.5, 0.0])
        pd.testing.assert_series_equal(result, expected)

    def test_normalize_inverted_latency_use_case(self):
        # Arrange - lower latency should result in higher score
        latency_ms = pd.Series([100, 200, 300])

        # Act
        result = normalize_to_0_1_inverted(latency_ms)

        # Assert - lowest latency (100) gets highest score (1.0)
        assert result.iloc[0] == 1.0
        assert result.iloc[2] == 0.0

    def test_normalize_inverted_empty(self):
        # Arrange
        series = pd.Series([], dtype=float)

        # Act
        result = normalize_to_0_1_inverted(series)

        # Assert
        assert result.empty


class TestTransformToScoresSchema:
    """Tests for the transform_to_scores_schema function."""

    @pytest.fixture
    def sample_merged_df(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xDEF", "0x123"],
                "url": ["https://a.com", "https://b.com", "https://c.com"],
                "Latency Coefficient": [0.5, 1.0, 1.5],
                "Standard Error": [0.1, 0.2, 0.3],
                "Latency Coefficient + Error Confidence Interval": [0.65, 1.3, 1.95],
                "% up_x": [99.5, 95.0, 90.0],
                "observed_duration_restricted": [1000, 2000, 3000],
                "uptime_duration_restricted": [995, 1900, 2700],
                "average_status": [0.98, 0.95, 0.90],
                "stake_to_fees": [100.0, 200.0, 300.0],
                "org": ["AWS", "GCP", "Hetzner"],
                "dst_lat": [40.0, 35.0, 50.0],
                "dst_lon": [-74.0, -120.0, 8.0],
                "query_count": [1000, 2000, 1500],
            }
        )

    def test_transform_basic(self, sample_merged_df):
        # Act
        result = transform_to_scores_schema(sample_merged_df)

        # Assert - check required columns exist
        required_columns = [
            "indexer",
            "url",
            "lat_lin_reg_coefficient",
            "lat_coefficient_std_error",
            "lat_coefficient_upper_bound",
            "lat_normalized_score",
            "uptime_score",
            "observed_duration_seconds",
            "uptime_duration_seconds",
            "success_rate",
            "stake_to_fees",
            "norm_uptime_score",
            "norm_success_rate",
            "org",
            "dst_lat",
            "dst_lon",
            "computed_at",
            "query_count",
        ]
        for col in required_columns:
            assert col in result.columns, f"Missing column: {col}"

    def test_transform_uptime_conversion(self, sample_merged_df):
        # Act
        result = transform_to_scores_schema(sample_merged_df)

        # Assert - uptime should be converted from percentage to 0-1 scale
        assert result["uptime_score"].iloc[0] == pytest.approx(0.995)
        assert result["uptime_score"].iloc[1] == pytest.approx(0.95)

    def test_transform_computed_at_is_recent(self, sample_merged_df):
        # Act
        result = transform_to_scores_schema(sample_merged_df)

        # Assert - computed_at should be recent
        now = datetime.now(timezone.utc)
        computed_at = result["computed_at"].iloc[0]
        delta = (now - computed_at).total_seconds()
        assert delta < 60  # Within 60 seconds

    def test_transform_normalized_scores_bounded(self, sample_merged_df):
        # Act
        result = transform_to_scores_schema(sample_merged_df)

        # Assert - all normalized scores should be between 0 and 1
        for col in [
            "norm_uptime_score",
            "norm_success_rate",
            "lat_normalized_score",
        ]:
            values = result[col].dropna()
            assert all(0 <= v <= 1 for v in values), f"{col} has values outside [0, 1]"

    def test_transform_query_count_preserved(self, sample_merged_df):
        # Act
        result = transform_to_scores_schema(sample_merged_df)

        # Assert - query_count should be preserved from input
        assert list(result["query_count"]) == [1000, 2000, 1500]


class TestIsPrivateIp:
    """Tests for the is_private_ip function."""

    def test_localhost(self):
        assert is_private_ip("127.0.0.1") is True
        assert is_private_ip("127.255.255.255") is True

    def test_class_a_private(self):
        assert is_private_ip("10.0.0.1") is True
        assert is_private_ip("10.255.255.255") is True

    def test_class_b_private(self):
        assert is_private_ip("172.16.0.1") is True
        assert is_private_ip("172.31.255.255") is True
        assert is_private_ip("172.15.0.1") is False  # Just outside range
        assert is_private_ip("172.32.0.1") is False  # Just outside range

    def test_class_c_private(self):
        assert is_private_ip("192.168.0.1") is True
        assert is_private_ip("192.168.255.255") is True
        assert is_private_ip("192.167.0.1") is False  # Just outside range

    def test_public_ips(self):
        assert is_private_ip("8.8.8.8") is False  # Google DNS
        assert is_private_ip("1.1.1.1") is False  # Cloudflare DNS
        assert is_private_ip("142.250.80.46") is False  # Google

    def test_ipv6_private(self):
        assert is_private_ip("fc00::1") is True  # ULA
        assert is_private_ip("fd00::1") is True  # ULA
        assert is_private_ip("fe80::1") is True  # link-local
        assert is_private_ip("::1") is True  # loopback

    def test_ipv6_public(self):
        assert is_private_ip("2001:4860:4860::8888") is False  # Google IPv6 DNS
        assert is_private_ip("2606:4700:4700::1111") is False  # Cloudflare IPv6 DNS

    def test_malformed_input(self):
        assert is_private_ip("not an ip") is False
        assert is_private_ip("") is False
        assert is_private_ip("999.999.999.999") is False


class TestDiagnoseGeoipFailure:
    """Tests for the diagnose_geoip_failure helper.

    The helper categorises why every indexer's GeoIP resolution returned
    NaN: ``private`` (RFC 1918 / loopback / Docker bridge), ``unresolved``
    (DNS gaierror), or ``public`` (resolved but missing from GeoLite2).
    """

    def _build_df(self, ip_addrs):
        return pd.DataFrame(
            {
                "indexer": [f"0x{i:040x}" for i in range(len(ip_addrs))],
                "ip_addr": ip_addrs,
            }
        )

    def test_all_private_ips_picks_local_network_diagnosis(self):
        df = self._build_df(["172.18.0.5", "172.18.0.6", "172.18.0.7"])
        result = diagnose_geoip_failure(df)
        assert "private" in result.lower()
        assert "private=3" in result
        assert "public=0" in result
        assert "unresolved=0" in result

    def test_all_unresolved_picks_dns_diagnosis(self):
        df = self._build_df([None, None, np.nan])
        result = diagnose_geoip_failure(df)
        assert "DNS" in result or "resolve" in result.lower()
        assert "unresolved=3" in result

    def test_all_public_picks_database_diagnosis(self):
        df = self._build_df(["8.8.8.8", "1.1.1.1", "142.250.80.46"])
        result = diagnose_geoip_failure(df)
        assert "public IPs" in result
        assert "GeoLite2-City" in result or "stale" in result
        assert "public=3" in result

    def test_mixed_majority_private_picks_private_diagnosis(self):
        df = self._build_df(["172.18.0.5", "172.18.0.6", "8.8.8.8"])
        result = diagnose_geoip_failure(df)
        assert "private" in result.lower()
        assert "private=2" in result
        assert "public=1" in result

    def test_counts_reflect_full_set_but_sample_capped_at_five(self):
        df = self._build_df([f"172.18.0.{i}" for i in range(10)])
        result = diagnose_geoip_failure(df)
        # Counts reflect all 10 indexers, not just the 5 displayed.
        assert "private=10" in result
        assert "Counts across 10 unique" in result
        # Sample line states it shows the first 5.
        assert "Sample (first 5)" in result
        # Confirm the rendered sample really is 5 rows.
        indexer_rows = [line for line in result.split("\n") if "0x" in line and "->" in line]
        assert len(indexer_rows) == 5

    def test_mixed_ipv4_and_ipv6_private_addresses(self):
        df = self._build_df(["172.18.0.5", "fc00::1", "fe80::1", "::1"])
        result = diagnose_geoip_failure(df)
        # All four are private (IPv4 RFC 1918 + IPv6 ULA + link-local + loopback).
        assert "private=4" in result
        assert "public=0" in result
        assert "unresolved=0" in result


class TestHaversineVectorized:
    """Tests for the haversine_vectorized function."""

    def test_same_location(self):
        # Arrange
        lon1 = pd.Series([0.0])
        lat1 = pd.Series([0.0])
        lon2 = pd.Series([0.0])
        lat2 = pd.Series([0.0])

        # Act
        result = haversine_vectorized(lon1, lat1, lon2, lat2)

        # Assert
        assert result[0] == 0.0

    def test_known_distance(self):
        # Arrange - NYC to LA approx 2450 miles
        nyc_lon, nyc_lat = -74.006, 40.7128
        la_lon, la_lat = -118.2437, 34.0522

        # Act
        result = haversine_vectorized(
            pd.Series([nyc_lon]), pd.Series([nyc_lat]), pd.Series([la_lon]), pd.Series([la_lat])
        )

        # Assert - should be approximately 2450 miles
        assert 2400 < result[0] < 2500

    def test_vectorized_multiple_points(self):
        # Arrange
        lon1 = pd.Series([0.0, 0.0])
        lat1 = pd.Series([0.0, 0.0])
        lon2 = pd.Series([0.0, 30.0])
        lat2 = pd.Series([0.0, 0.0])

        # Act
        result = haversine_vectorized(lon1, lat1, lon2, lat2)

        # Assert
        assert len(result) == 2
        assert result[0] == 0.0
        assert result[1] > 0


class TestFilterSuccessfulQueries:
    """Tests for filter_successful_queries function."""

    def test_filter_200_ok(self):
        # Arrange
        df = pd.DataFrame(
            {
                "status": ["200 OK", "404 Not Found", "200 OK", "500 Error"],
                "data": ["a", "b", "c", "d"],
            }
        )

        # Act
        result = filter_successful_queries(df)

        # Assert
        assert len(result) == 2
        assert list(result["data"]) == ["a", "c"]

    def test_filter_empty_df(self):
        # Arrange
        df = pd.DataFrame(columns=["status", "data"])

        # Act
        result = filter_successful_queries(df)

        # Assert
        assert result.empty


class TestCalculateIndexerSuccessRate:
    """Tests for calculate_indexer_success_rate function."""

    def test_success_rate_basic(self):
        # Arrange
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "A", "B", "B"],
                "status": ["200 OK", "200 OK", "Error", "200 OK", "200 OK"],
            }
        )

        # Act
        result = calculate_indexer_success_rate(df)

        # Assert
        assert len(result) == 2
        a_rate = result[result["indexer"] == "A"]["average_status"].iloc[0]
        b_rate = result[result["indexer"] == "B"]["average_status"].iloc[0]
        assert a_rate == pytest.approx(2 / 3)
        assert b_rate == pytest.approx(1.0)

    def test_success_rate_includes_missing_block(self):
        # Arrange - Unavailable(MissingBlock) counts as success
        df = pd.DataFrame(
            {
                "indexer": ["A", "A"],
                "status": ["200 OK", "Unavailable(MissingBlock)"],
            }
        )

        # Act
        result = calculate_indexer_success_rate(df)

        # Assert
        assert result["average_status"].iloc[0] == 1.0


class TestIterativeFilter:
    """Tests for iterative_filter function."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "deployment_hash": ["A"] * 10 + ["B"] * 10,
                "indexer": ["X", "Y"] * 10,
                "query_id": list(range(20)),
            }
        )

    def test_iterative_filter_no_change(self, sample_df):
        # Act
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=1,
            min_deployments_per_indexer=1,
            min_queries_per_indexer=1,
            min_queries_per_deployment=1,
        )

        # Assert
        assert len(result) == len(sample_df)

    def test_iterative_filter_removes_sparse_data(self, sample_df):
        # Act - require 3 indexers per deployment (we only have 2)
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=3,
            min_deployments_per_indexer=1,
            min_queries_per_indexer=1,
            min_queries_per_deployment=1,
        )

        # Assert
        assert len(result) == 0

    def test_iterative_filter_empty_df(self):
        # Arrange
        df = pd.DataFrame(columns=["deployment_hash", "indexer", "query_id"])

        # Act
        result = iterative_filter(df, 0, 0, 0, 0)

        # Assert
        assert len(result) == 0


# Ported from test_data_manager.py: cover the processing functions
# that will be deleted from IISA once the CronJob migration completes.


class TestAdjustRows:
    """Tests for the adjust_rows function."""

    def test_adjust_rows_normal_case(self):
        # Arrange
        sample_data = pd.DataFrame(
            {
                "deployment_hash": ["hash1", "hash2", "hash3", "hash1"],
                "indexer": ["index1", "index2", "index3", "indexer4"],
                "num_rows": [50, 10000, 600, 50],
            }
        )

        # Act
        target_rows = 600
        result = adjust_rows(sample_data, target_rows)

        # Assert - result should be close to target
        assert result > 0

    def test_adjust_rows_empty_dataframe(self):
        # Arrange
        df = pd.DataFrame({"deployment_hash": [], "indexer": [], "num_rows": []})

        # Act
        target_rows = 100
        result = adjust_rows(df, target_rows)

        # Assert - should handle empty gracefully
        assert result == 0 or df.empty

    def test_adjust_rows_zero_target(self):
        # Arrange
        sample_data = pd.DataFrame(
            {
                "deployment_hash": ["hash1", "hash2"],
                "indexer": ["index1", "index2"],
                "num_rows": [50, 100],
            }
        )

        # Act
        target_rows = 0
        result = adjust_rows(sample_data, target_rows)

        # Assert
        assert result == 0

    def test_adjust_rows_negative_case(self):
        # Arrange
        df = pd.DataFrame(
            {
                "deployment_hash": ["hash1"],
                "indexer": ["index1"],
                "num_rows": [100],
            }
        )

        # Act & Assert
        with pytest.raises(ValueError, match="non-negative"):
            adjust_rows(df, -300)


class TestStrategicSample:
    """Tests for the strategic_sample function."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "deployment_hash": ["A", "A", "A", "B", "B", "C"] * 10000,
                "indexer": ["X", "Y", "Z", "X", "Y", "X"] * 10000,
                "query_id": range(60000),
            }
        )

    def test_strategic_sample_basic(self, sample_df):
        # Act
        result_df, integer_root = strategic_sample(sample_df, target_rows_per_subgraph=30)

        # Assert
        # Check output df length unchanged
        assert len(result_df) == len(sample_df)

        # Check sampled count is reasonable
        assert result_df["sampled_query_id"].notna().sum() == 90

        # Verify integer_root calculation
        assert isinstance(integer_root, int)
        assert integer_root == int(np.sqrt(result_df["sampled_query_id"].notna().sum()))

    def test_strategic_sample_empty_df(self):
        # Arrange
        empty_df = pd.DataFrame(columns=["deployment_hash", "indexer", "query_id"])

        # Act
        result_df, integer_root = strategic_sample(empty_df, target_rows_per_subgraph=10)

        # Assert
        assert result_df.empty
        assert "sampled_query_id" in result_df.columns
        assert integer_root == 0

    def test_strategic_sample_target_rows_per_subgraph_greater_than_df(self, sample_df):
        # Act - target larger than data
        result_df, integer_root = strategic_sample(sample_df, target_rows_per_subgraph=10_000_000)

        # Assert - should sample all unique queries
        assert len(result_df) == len(sample_df)
        assert result_df["sampled_query_id"].notna().sum() == sample_df["query_id"].nunique()


class TestHashSampledQueries:
    """Tests for the hash_sampled_queries function."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "sampled_query_id": [1, 2, 3, None, 5, 6, np.nan, 8, 9, 10] * 1000,
                "other_column": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"] * 1000,
            }
        )

    def test_hash_sampled_queries_basic(self, sample_df):
        # Act
        integer_root = 33
        result = hash_sampled_queries(sample_df, integer_root)

        # Assert
        assert "sampled_query_id_hashed_mod_integer_root" in result.columns
        assert result["sampled_query_id_hashed_mod_integer_root"].notna().sum() == 8000
        assert result["sampled_query_id_hashed_mod_integer_root"].isna().sum() == 2000

        # All hashed values within range
        assert all(
            0 <= x < integer_root
            for x in result["sampled_query_id_hashed_mod_integer_root"].dropna()
        )

    def test_hash_sampled_queries_empty_df(self):
        # Arrange
        empty_df = pd.DataFrame(columns=["sampled_query_id"])

        # Act
        result = hash_sampled_queries(empty_df, 5)

        # Assert
        assert "sampled_query_id_hashed_mod_integer_root" in result.columns
        assert result.empty

    def test_hash_sampled_queries_all_null(self):
        # Arrange
        df = pd.DataFrame({"sampled_query_id": [None, None, None]})

        # Act
        result = hash_sampled_queries(df, 5)

        # Assert
        assert result["sampled_query_id_hashed_mod_integer_root"].isna().all()

    def test_hash_sampled_queries_consistency(self, sample_df):
        # Act - hash twice with same params
        integer_root = 7
        result1 = hash_sampled_queries(sample_df, integer_root)
        result2 = hash_sampled_queries(sample_df, integer_root)

        # Assert - results should be identical
        pd.testing.assert_frame_equal(result1, result2)

    def test_hash_sampled_queries_different_integer_roots(self, sample_df):
        # Act
        result1 = hash_sampled_queries(sample_df.copy(), 5)
        result2 = hash_sampled_queries(sample_df.copy(), 10)

        # Assert - different roots should produce different results
        assert not result1["sampled_query_id_hashed_mod_integer_root"].equals(
            result2["sampled_query_id_hashed_mod_integer_root"]
        )

    def test_hash_sampled_queries_original_df_unchanged(self, sample_df):
        # Arrange
        original_df = sample_df.copy()

        # Act
        _ = hash_sampled_queries(sample_df, 5)

        # Assert - original should be unchanged
        pd.testing.assert_frame_equal(original_df, sample_df)

    def test_hash_sampled_queries_large_integer_root(self):
        # Arrange
        df = pd.DataFrame({"sampled_query_id": range(1000)})
        large_root = 10_000_000_000

        # Act
        result = hash_sampled_queries(df, large_root)

        # Assert - all values within range
        assert all(0 <= x < large_root for x in result["sampled_query_id_hashed_mod_integer_root"])


class TestDeterminism:
    """Tests that scoring produces identical results given the same seed."""

    def test_hash_produces_known_output(self):
        """SHA256 bucketing produces a stable known value, guarding against
        accidental reversion to hash()."""
        df = pd.DataFrame({"sampled_query_id": ["query-abc-JFK"]})
        result = hash_sampled_queries(df, 100)
        value = result["sampled_query_id_hashed_mod_integer_root"].iloc[0]
        # Re-running this test in a different process must produce the same value.
        # hash() would fail this; sha256 will not.
        assert (
            value
            == int.from_bytes(hashlib.sha256(b"query-abc-JFK").digest()[:8], byteorder="big") % 100
        )

    def test_default_scoring_seed_is_derived_from_the_start_date(self):
        """Without an explicit seed, sampling must still be reproducible for a given window."""
        from datetime import date

        assert processing.default_scoring_seed(date(2026, 5, 1)) == 20260501
        assert processing.default_scoring_seed(date(2026, 5, 1)) == processing.default_scoring_seed(
            date(2026, 5, 1)
        )
        assert processing.default_scoring_seed(date(2026, 5, 2)) != processing.default_scoring_seed(
            date(2026, 5, 1)
        )

    def test_strategic_sample_deterministic_with_seed(self):
        """strategic_sample with the same seed produces identical output."""
        df = pd.DataFrame(
            {
                "deployment_hash": ["QmA"] * 50 + ["QmB"] * 50,
                "indexer": ["0x01"] * 25 + ["0x02"] * 25 + ["0x03"] * 25 + ["0x04"] * 25,
                "query_id": [f"q{i}" for i in range(100)],
            }
        )

        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)
        result1, root1 = strategic_sample(df.copy(), 10, rng=rng1)
        result2, root2 = strategic_sample(df.copy(), 10, rng=rng2)

        assert root1 == root2
        sampled1 = set(result1["sampled_query_id"].dropna())
        sampled2 = set(result2["sampled_query_id"].dropna())
        assert sampled1 == sampled2

    def test_strategic_sample_different_seeds_differ(self):
        """Different seeds produce different samples."""
        df = pd.DataFrame(
            {
                "deployment_hash": ["QmA"] * 100,
                "indexer": ["0x01"] * 50 + ["0x02"] * 50,
                "query_id": [f"q{i}" for i in range(100)],
            }
        )

        rng1 = np.random.default_rng(1)
        rng2 = np.random.default_rng(999)
        result1, _ = strategic_sample(df.copy(), 5, rng=rng1)
        result2, _ = strategic_sample(df.copy(), 5, rng=rng2)

        sampled1 = set(result1["sampled_query_id"].dropna())
        sampled2 = set(result2["sampled_query_id"].dropna())
        assert sampled1 != sampled2


class TestPerformLinearRegression:
    """
    Tests for the latency linear regression pipeline.
    This is the CORE algorithm of the score computation.
    """

    @pytest.fixture
    def sample_df(self):
        np.random.seed(42)
        return pd.DataFrame(
            {
                "sampled_query_id": range(10000),
                "indexer": np.random.choice(["0xABC", "0xXYZ", "0x123", "0x789"], 10000),
                "indexer_network": np.random.choice(["net1", "net2"], 10000),
                "deployment_hash": np.random.choice(
                    ["deployment_1", "deployment_2", "deployment_3"], 10000
                ),
                "response_time_ms": np.random.randint(10, 20000, 10000),
                "fee": np.random.uniform(0.000001, 0.01, 10000),
                "distance_miles": np.random.uniform(0, 1000, 10000),
            }
        )

    def test_perform_latency_linear_regression(self, sample_df):
        # Arrange
        integer_root = 100
        hashed_df = hash_sampled_queries(sample_df, integer_root)

        predictor = ["response_time_ms"]
        categorical = [
            "indexer",
            "deployment_hash",
            "indexer_network",
            "sampled_query_id_hashed_mod_integer_root",
        ]
        numeric = ["distance_miles", "fee"]

        # Act
        rankings, results = perform_latency_linear_regression(
            hashed_df, predictor, categorical, numeric
        )

        # Assert - rankings should have expected columns
        expected_columns = [
            "indexer",
            "Latency Coefficient",
            "Standard Error",
            "p-value",
            "Latency Coefficient + Error Confidence Interval",
        ]
        assert all(col in rankings.columns for col in expected_columns)

        # Only indexer values in the indexer column
        assert all(rankings["indexer"].isin(["0xABC", "0xXYZ", "0x123", "0x789"]))

        # Coefficients should be numeric and non-null
        assert rankings["Latency Coefficient"].notna().all()

        # p-values between 0 and 1
        assert rankings["p-value"].between(0, 1).all()

    def test_perform_latency_linear_regression_with_empty_df(self):
        # Arrange
        empty_df = pd.DataFrame(
            columns=[
                "indexer",
                "deployment_hash",
                "indexer_network",
                "response_time_ms",
                "fee",
                "distance_miles",
            ]
        )

        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Act & Assert
        with pytest.raises(RuntimeError):
            perform_latency_linear_regression(empty_df, predictor, categorical, numeric)

    def test_perform_latency_linear_regression_with_missing_columns(self, sample_df):
        # Arrange - remove required columns
        df_missing = sample_df.drop(columns=["indexer", "fee"])

        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Act & Assert
        with pytest.raises(KeyError):
            perform_latency_linear_regression(df_missing, predictor, categorical, numeric)

    def test_perform_latency_linear_regression_deterministic_verification(self, sample_df):
        # Arrange
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Act - run twice
        rankings1, _ = perform_latency_linear_regression(sample_df, predictor, categorical, numeric)
        rankings2, _ = perform_latency_linear_regression(sample_df, predictor, categorical, numeric)

        # Assert - results should be identical
        pd.testing.assert_frame_equal(rankings1, rankings2)

    def test_perform_latency_linear_regression_original_df_unchanged(self, sample_df):
        # Arrange
        original = sample_df.copy()
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Act
        perform_latency_linear_regression(sample_df, predictor, categorical, numeric)

        # Assert
        pd.testing.assert_frame_equal(original, sample_df)


class TestCalculateIndexerUptime:
    """
    Tests for the indexer uptime calculation.
    This is critical for measuring indexer reliability.
    """

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "indexer": ["A", "A", "A", "A", "B", "B", "C"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                    datetime(2024, 1, 1, 12, 5),
                    datetime(2024, 1, 1, 12, 7),
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 3),
                    datetime(2024, 1, 1, 12, 0),
                ],
                "status": [
                    "200 OK",
                    "200 OK",
                    "Error",
                    "200 OK",
                    "200 OK",
                    "Unavailable(MissingBlock)",
                    "200 OK",
                ],
            }
        )

    def test_calculate_indexer_uptime_base_case(self, sample_df):
        # Act
        result = calculate_indexer_uptime(sample_df)

        # Assert - expected columns present
        expected_columns = [
            "indexer",
            "observed_duration_restricted",
            "uptime_duration_restricted",
            "observed_duration_full",
            "uptime_duration_full",
            "% up_y",
            "% up_x",
        ]
        assert all(col in result.columns for col in expected_columns)

        # All indexers present
        assert set(result["indexer"]) == set(sample_df["indexer"])

        # Percentages valid (0-100 or NaN for single-query indexers)
        assert all((0 <= p <= 100) or np.isnan(p) for p in result["% up_x"])

    def test_calculate_indexer_uptime_all_up(self):
        # Arrange
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                ],
                "status": ["200 OK", "Unavailable(MissingBlock)", "200 OK", "200 OK"],
            }
        )

        # Act
        result = calculate_indexer_uptime(df)

        # Assert - all should be 100%
        assert all(result["% up_x"] == 100)
        assert all(result["% up_y"] == 100)

    def test_calculate_indexer_uptime_all_down(self):
        # Arrange
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                ],
                "status": ["Error", "Bad", "504 Error", "Timeout"],
            }
        )

        # Act
        result = calculate_indexer_uptime(df)

        # Assert - all should be 0%
        assert all(result["% up_x"] == 0)
        assert all(result["% up_y"] == 0)

    def test_calculate_indexer_uptime_threshold(self):
        # Arrange - queries spaced 5 minutes apart
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "A"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 5),
                    datetime(2024, 1, 1, 12, 10),
                ],
                "status": ["200 OK", "200 OK", "200 OK"],
            }
        )

        # Act - compare default (120s) vs lower threshold (60s)
        result_default = calculate_indexer_uptime(df, threshold_seconds=120)
        result_low = calculate_indexer_uptime(df, threshold_seconds=60)

        # Assert - lower threshold should show lower restricted uptime
        assert (
            result_low["uptime_duration_restricted"].iloc[0]
            < result_default["uptime_duration_restricted"].iloc[0]
        )

    def test_calculate_indexer_uptime_empty_df(self):
        # Arrange
        df = pd.DataFrame(columns=["indexer", "timestamp", "status"])

        # Act
        result = calculate_indexer_uptime(df)

        # Assert
        assert result.empty

    def test_calculate_indexer_uptime_single_entry_for_indexers(self):
        # Arrange - single query can't compute uptime
        df = pd.DataFrame(
            {
                "indexer": ["A", "B"],
                "timestamp": [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 12, 0)],
                "status": ["200 OK", "Error"],
            }
        )

        # Act
        result = calculate_indexer_uptime(df)

        # Assert - should have NaN uptime percentages
        assert len(result) == 2
        assert np.isnan(result["% up_x"].iloc[0])
        assert np.isnan(result["% up_y"].iloc[0])

    def test_calculate_indexer_uptime_rounding(self):
        # Arrange
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "A"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 1),
                    datetime(2024, 1, 1, 12, 3),
                ],
                "status": ["200 OK", "Error", "200 OK"],
            }
        )

        # Act
        result = calculate_indexer_uptime(df)

        # Assert - percentages rounded to 3 decimals
        assert all(round(p, 3) == p for p in result["% up_x"].dropna())

    def test_calculate_indexer_uptime_sorting(self):
        # Arrange - test if the result is sorted by '% up' in descending order
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B", "C", "C"],
                "timestamp": [datetime(2024, 1, 1, 12, i) for i in range(6)],
                "status": ["200 OK", "Error", "200 OK", "200 OK", "200 OK", "200 OK"],
            }
        )

        # Act
        result = calculate_indexer_uptime(df)

        # Assert - '% up_x' column is sorted in descending order
        assert list(result["% up_x"]) == sorted(result["% up_x"], reverse=True)


class TestAggregateIndexerInfo:
    """Tests for the aggregate_indexer_info function."""

    def test_aggregate_indexer_info_base_case(self):
        # Arrange
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B", "C", "C", "C"],
                "url": [
                    "https://a.com",
                    "https://a.com",
                    "https://b.com",
                    "https://b.com",
                    "https://c.com",
                    "https://c.com",
                    "https://c.com",
                ],
                "org": ["X", "X", "Y", "Z", "W", "W", "W"],
                "dst_lat": [10.1, 13.123445, 35, 31, 55, 45, 50],
                "dst_lon": [22, 25.123445, 44, 41, 65, 60, 60],
            }
        )

        # Act
        result = aggregate_indexer_info(df)

        # Assert - url preserved (first non-null)
        assert list(result["url"]) == ["https://a.com", "https://b.com", "https://c.com"]

        # Assert - mode org selected
        assert list(result["org"]) == ["X", "Y", "W"]

        # Lat/lon rounded to nearest 20
        assert list(result["dst_lat"]) == [20, 40, 40]
        assert list(result["dst_lon"]) == [20, 40, 60]

    def test_aggregate_indexer_info_empty_df(self):
        # Arrange
        df = pd.DataFrame(columns=["indexer", "url", "org", "dst_lat", "dst_lon"])

        # Act
        result = aggregate_indexer_info(df)

        # Assert
        assert result.empty
        assert list(result.columns) == ["indexer", "url", "org", "dst_lat", "dst_lon"]

    def test_aggregate_indexer_info_with_nans(self):
        # Arrange
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B", "B"],
                "url": [np.nan, "https://a.com", "https://b.com", np.nan, np.nan],
                "org": [np.nan, "X", "Y", np.nan, np.nan],
                "dst_lat": [10, np.nan, np.nan, np.nan, np.nan],
                "dst_lon": [20, np.nan, np.nan, np.nan, np.nan],
            }
        )

        # Act
        result = aggregate_indexer_info(df)

        # Assert
        assert list(result["indexer"]) == ["A", "B"]
        assert list(result["url"]) == ["https://a.com", "https://b.com"]
        assert list(result["org"]) == ["X", "Y"]


class TestMergeAndPrepareDataframes:
    """Tests for the final merge and prepare function."""

    @pytest.fixture
    def indexer_uptime(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x123"],
                "uptime": [99.5, 98.7, 97.0],
                "observed_duration_full": [100, 200, 300],
                "uptime_duration_full": [99, 197, 291],
            }
        )

    @pytest.fixture
    def indexer_rankings(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x789"],
                "Latency Coefficient": [0.5, 0.8, 1.2],
                "Standard Error": [0.05, 0.08, 0.12],
                "p-value": [0.01, 0.02, 0.03],
                "% up_y": [95, 96, 97],
            }
        )

    @pytest.fixture
    def agg_df(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x456"],
                "url": ["https://abc.com", "https://xyz.com", "https://456.com"],
                "org": ["AWS", "GCP", "Hetzner"],
                "dst_lat": [40, 35, 50],
                "dst_lon": [-74, -120, 8],
            }
        )

    @pytest.fixture
    def indexer_success_rate(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0xDEF"],
                "average_status": [0.95, 0.92, 0.88],
            }
        )

    @pytest.fixture
    def stake_to_fees(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0xGHI"],
                "stake_to_fees": [100, 200, 300],
            }
        )

    @pytest.fixture
    def indexer_query_count(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x123"],
                "query_count": [1000, 2000, 1500],
            }
        )

    def test_merge_base_case(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
        indexer_query_count,
    ):
        # Act
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
            indexer_query_count,
        )

        # Assert - core columns present
        assert "indexer" in result.columns
        assert "uptime" in result.columns
        assert "Latency Coefficient" in result.columns
        assert "query_count" in result.columns

        # Dropped columns not present
        assert "% up_y" not in result.columns
        assert "observed_duration_full" not in result.columns

    def test_merge__missing_indexer(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
        indexer_query_count,
    ):
        # Arrange - remove an indexer
        indexer_uptime_modified = indexer_uptime[indexer_uptime["indexer"] != "0xABC"]

        # Act
        result = merge_and_prepare_dataframes(
            indexer_uptime_modified,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
            indexer_query_count,
        )

        # Assert - removed indexer not in result
        assert "0xABC" not in result["indexer"].values

    def test_merge_no_common_indexers(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
        indexer_query_count,
    ):
        # Arrange - different indexer sets (no overlap with rankings)
        indexer_uptime["indexer"] = ["0xAAA", "0xBBB", "0xCCC"]

        # Act
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
            indexer_query_count,
        )

        # Assert - result is empty because:
        # 1. Left merge with rankings creates NaN for latency columns
        # 2. dropna(subset=latency_columns) removes all rows
        assert result.empty

    def test_merge_additional_columns(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
        indexer_query_count,
    ):
        # Arrange - add extra column
        indexer_uptime["extra_col"] = [1, 2, 3]

        # Act
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
            indexer_query_count,
        )

        # Assert - extra column preserved
        assert "extra_col" in result.columns


# ----------------------------------------------------------------------
# main.py: run_scoring() push-failure accounting and validate_configuration()
# ----------------------------------------------------------------------

# main is in cronjobs/compute_scores which jobs_path already added to sys.path
# at the top of this file.
import main  # noqa: E402
from iisa_client import IISAPushError  # noqa: E402


class TestRunScoringPushFailure:
    """run_scoring() must return False when IISAPushError escapes write_scores."""

    def test_push_failure_marks_run_as_failed(self, monkeypatch, caplog):
        # Arrange — pipeline returns a non-empty df, but provider.write_scores raises
        # IISAPushError. Without the try/except in run_scoring(), the exception would
        # propagate and crash the cronjob instead of exiting with a non-zero code.
        from unittest.mock import MagicMock

        scores_df = pd.DataFrame(
            {
                "indexer": ["0xAAA"],
                "url": ["https://a.example"],
                "lat_lin_reg_coefficient": [0.5],
            }
        )

        mock_provider = MagicMock()
        mock_provider.write_scores.side_effect = IISAPushError("retries exhausted")

        monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
        monkeypatch.setattr(
            main,
            "compute_all_scores",
            lambda **kwargs: scores_df,
        )

        # Act
        with caplog.at_level("INFO", logger=main.logger.name):
            result = main.run_scoring()

        # Assert — run is marked failed and the summary log reports mode=failed,
        # so the CronJob's failedJobsHistoryLimit and any log-based alerting
        # can see the failure mode without needing in-process counters.
        assert result is False
        assert any("mode=failed" in rec.getMessage() for rec in caplog.records)
        mock_provider.write_scores.assert_called_once()


class TestValidateConfigurationIISAURL:
    """validate_configuration() must fail fast when IISA_API_URL is unset."""

    def test_missing_iisa_api_url_raises_configuration_error(self, monkeypatch):
        # Arrange — satisfy REDPANDA_BOOTSTRAP_SERVERS so only IISA_API_URL trips.
        monkeypatch.setenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setattr(main, "IISA_API_URL", "")

        # Act / Assert
        with pytest.raises(main.ConfigurationError) as exc_info:
            main.validate_configuration()

        assert "configuration error" in str(exc_info.value).lower()

    def test_present_iisa_api_url_passes(self, monkeypatch):
        # Arrange — all required config set.
        monkeypatch.setenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setattr(main, "IISA_API_URL", "http://iisa.example:8080")

        # Act / Assert — should not raise
        main.validate_configuration()


class TestComputeAllScoresGeoipDemotion:
    """Demote full to partial when all indexers resolve to private IPs.

    Without this path, the pipeline raised at all-dst_lat-NaN and the
    cronjob fell to degraded mode (every quality score flattened to 0.5).
    Demoted runs keep neutral latency but real success / uptime / stake.
    """

    def _build_combined_queries(self, indexer_ids, rows_per_indexer=20):
        from datetime import datetime

        rows = []
        base = datetime(2026, 5, 1)
        for idx_id in indexer_ids:
            short = idx_id[2:8]
            for i in range(rows_per_indexer):
                rows.append(
                    {
                        "query_id": f"q-{short}-{i:03d}-LAX",
                        "deployment_hash": "QmTestDeployment",
                        "fee": 1.0,
                        "timestamp": base + pd.Timedelta(minutes=i),
                        "blocks_behind": 0,
                        "response_time_ms": 100.0,
                        "indexer": idx_id,
                        "status": 200,
                        "day_partition": "2026-05-01",
                        "subgraph_network": "arbitrum",
                        "url": f"http://{short}-svc:7601",
                    }
                )
        return pd.DataFrame(rows)

    def test_demotes_to_partial_when_all_indexers_have_private_ips(self, monkeypatch, caplog):
        from datetime import date
        from unittest.mock import MagicMock

        indexer_ids = ["0x" + c * 40 for c in "abc"]
        combined_queries = self._build_combined_queries(indexer_ids)

        mock_provider = MagicMock()
        mock_provider.fetch_initial_query_results.return_value = pd.DataFrame(
            {
                "deployment_hash": ["QmTestDeployment"] * 3,
                "indexer": indexer_ids,
                "num_rows": [20, 20, 20],
            }
        )
        mock_provider.fetch_combined_query_results.return_value = combined_queries
        mock_provider.fetch_stake_to_fees.return_value = pd.DataFrame(
            {
                "indexer": indexer_ids,
                "stake_to_fees": [1.0, 2.0, 3.0],
                "total_query_fees": [10.0, 20.0, 30.0],
                "last_known_slashable_stake": [100.0, 200.0, 300.0],
            }
        )
        mock_provider.graph_network_url = "http://graph-network:8000"

        def fake_resolve_geoip(_combined_queries):
            # Simulate the local-network case: every URL resolves to a
            # private IP and the GeoLite2 lookup returns no coordinates.
            return pd.DataFrame(
                {
                    "indexer": indexer_ids,
                    "url": [f"http://{i[2:8]}-svc:7601" for i in indexer_ids],
                    "ip_addr": ["172.18.0.5", "172.18.0.6", "172.18.0.7"],
                    "org": [None] * 3,
                    "dst_country": [None] * 3,
                    "dst_lat": [np.nan] * 3,
                    "dst_lon": [np.nan] * 3,
                    "indexer_network": ["arbitrum"] * 3,
                }
            )

        monkeypatch.setattr(processing, "resolve_indexer_geoip", fake_resolve_geoip)
        monkeypatch.setattr(processing, "discover_indexers_from_network_subgraph", lambda url: {})
        monkeypatch.setattr(
            processing,
            "fetch_and_filter_graph_node_versions",
            lambda merged, urls: merged,
        )

        with caplog.at_level("WARNING", logger="processing"):
            result = compute_all_scores(
                provider=mock_provider,
                start_date=date(2026, 5, 1),
                start_ts="2026-05-01T00:00:00Z",
                num_days=28,
                target_rows=20_000_000,
                geoip_available=True,
            )

        assert any("demoting to partial mode" in r.getMessage() for r in caplog.records), (
            "expected the demotion warning to be logged"
        )
        assert "scoring_mode" in result.columns
        assert (result["scoring_mode"] == "partial_no_geoip").all(), (
            "every row should reflect partial mode after demotion"
        )
        assert len(result) == 3, "all three indexers should appear in the output"

    def test_stays_in_full_mode_when_at_least_one_indexer_has_public_ip(self, monkeypatch, caplog):
        from datetime import date
        from unittest.mock import MagicMock

        indexer_ids = ["0x" + c * 40 for c in "abc"]
        combined_queries = self._build_combined_queries(indexer_ids)

        mock_provider = MagicMock()
        mock_provider.fetch_initial_query_results.return_value = pd.DataFrame(
            {
                "deployment_hash": ["QmTestDeployment"] * 3,
                "indexer": indexer_ids,
                "num_rows": [20, 20, 20],
            }
        )
        mock_provider.fetch_combined_query_results.return_value = combined_queries
        mock_provider.fetch_stake_to_fees.return_value = pd.DataFrame(
            {
                "indexer": indexer_ids,
                "stake_to_fees": [1.0, 2.0, 3.0],
                "total_query_fees": [10.0, 20.0, 30.0],
                "last_known_slashable_stake": [100.0, 200.0, 300.0],
            }
        )
        mock_provider.graph_network_url = "http://graph-network:8000"

        def fake_resolve_geoip(_combined_queries):
            # One public resolution → full mode must stay engaged.
            return pd.DataFrame(
                {
                    "indexer": indexer_ids,
                    "url": [f"http://{i[2:8]}-svc:7601" for i in indexer_ids],
                    "ip_addr": ["172.18.0.5", "8.8.8.8", "172.18.0.7"],
                    "org": [None, "Google LLC", None],
                    "dst_country": [None, "US", None],
                    "dst_lat": [np.nan, 37.751, np.nan],
                    "dst_lon": [np.nan, -97.822, np.nan],
                    "indexer_network": ["arbitrum"] * 3,
                }
            )

        monkeypatch.setattr(processing, "resolve_indexer_geoip", fake_resolve_geoip)
        monkeypatch.setattr(processing, "discover_indexers_from_network_subgraph", lambda url: {})
        monkeypatch.setattr(
            processing,
            "fetch_and_filter_graph_node_versions",
            lambda merged, urls: merged,
        )

        with caplog.at_level("WARNING", logger="processing"):
            try:
                compute_all_scores(
                    provider=mock_provider,
                    start_date=date(2026, 5, 1),
                    start_ts="2026-05-01T00:00:00Z",
                    num_days=28,
                    target_rows=20_000_000,
                    geoip_available=True,
                )
            except Exception:
                # Full mode may still raise downstream (e.g. iterative
                # filter wipes everything) — we only assert that the
                # demotion path was *not* taken.
                pass

        assert not any("demoting to partial mode" in r.getMessage() for r in caplog.records), (
            "demotion warning should not fire when any indexer has a public IP"
        )

    def test_partial_mode_scores_every_indexer_with_neutral_latency(self, monkeypatch):
        from datetime import date
        from unittest.mock import MagicMock

        indexer_ids = ["0x" + c * 40 for c in "abc"]
        combined_queries = self._build_combined_queries(indexer_ids)

        mock_provider = MagicMock()
        mock_provider.fetch_initial_query_results.return_value = pd.DataFrame(
            {
                "deployment_hash": ["QmTestDeployment"] * 3,
                "indexer": indexer_ids,
                "num_rows": [20, 20, 20],
            }
        )
        mock_provider.fetch_combined_query_results.return_value = combined_queries
        mock_provider.fetch_stake_to_fees.return_value = pd.DataFrame(
            {
                "indexer": indexer_ids,
                "stake_to_fees": [1.0, 2.0, 3.0],
                "total_query_fees": [10.0, 20.0, 30.0],
                "last_known_slashable_stake": [100.0, 200.0, 300.0],
            }
        )
        mock_provider.graph_network_url = "http://graph-network:8000"

        def fail_if_called(*_args, **_kwargs):
            raise AssertionError("GeoIP resolution must not run in partial mode")

        monkeypatch.setattr(processing, "resolve_indexer_geoip", fail_if_called)
        monkeypatch.setattr(processing, "discover_indexers_from_network_subgraph", lambda url: {})
        monkeypatch.setattr(
            processing,
            "fetch_and_filter_graph_node_versions",
            lambda merged, urls: merged,
        )

        result = compute_all_scores(
            provider=mock_provider,
            start_date=date(2026, 5, 1),
            start_ts="2026-05-01T00:00:00Z",
            num_days=28,
            target_rows=20_000_000,
            geoip_available=False,
        )

        assert len(result) == 3, "all three indexers should appear in the output"
        assert (result["scoring_mode"] == "partial_no_geoip").all()
        assert (result["lat_lin_reg_coefficient"] == 0.0).all()
        assert (result["query_count"] == 0).all()
        assert (result["dips_info_available"] == False).all()  # noqa: E712


class TestRunScoringModeReporting:
    """The summary log must reflect the published scoring_mode, not the input flag.

    Before this was fixed, when compute_all_scores demoted to partial internally
    the summary line still said `mode=full`, misleading operators reading the log.
    """

    def _df(self, mode_value):
        return pd.DataFrame(
            {
                "indexer": ["0xAAA"],
                "url": ["https://a.example"],
                "scoring_mode": [mode_value],
            }
        )

    def test_summary_reports_partial_when_compute_demoted(self, monkeypatch, caplog):
        from unittest.mock import MagicMock

        mock_provider = MagicMock()
        scores_df = self._df("partial_no_geoip")

        monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
        monkeypatch.setattr(main, "compute_all_scores", lambda **kwargs: scores_df)

        with caplog.at_level("INFO", logger=main.logger.name):
            result = main.run_scoring()

        assert result is True
        assert any("mode=partial" in r.getMessage() for r in caplog.records), (
            "summary must report mode=partial when compute_all_scores demoted"
        )
        assert not any("mode=full" in r.getMessage() for r in caplog.records), (
            "must not report mode=full when partial_no_geoip was published"
        )

    def test_summary_reports_full_when_compute_stayed_full(self, monkeypatch, caplog):
        from unittest.mock import MagicMock

        mock_provider = MagicMock()
        scores_df = self._df("full")

        monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
        monkeypatch.setattr(main, "compute_all_scores", lambda **kwargs: scores_df)

        with caplog.at_level("INFO", logger=main.logger.name):
            result = main.run_scoring()

        assert result is True
        assert any("mode=full" in r.getMessage() for r in caplog.records), (
            "summary must report mode=full when scoring_mode is full"
        )

    def test_falls_back_to_degraded_when_full_pipeline_raises(self, monkeypatch, caplog):
        from unittest.mock import MagicMock

        mock_provider = MagicMock()
        degraded_df = self._df("degraded")

        def raise_pipeline_error(**_kwargs):
            raise RuntimeError("no rows remain after iterative filtering")

        monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
        monkeypatch.setattr(main, "compute_all_scores", raise_pipeline_error)
        monkeypatch.setattr(main, "compute_degraded_scores", lambda url: degraded_df)

        with caplog.at_level("INFO", logger=main.logger.name):
            result = main.run_scoring()

        assert result is True
        mock_provider.write_scores.assert_called_once_with(degraded_df)
        assert any("mode=degraded" in r.getMessage() for r in caplog.records), (
            "summary must report mode=degraded when the fallback produced the scores"
        )

    def test_reports_failed_when_both_pipelines_produce_nothing(self, monkeypatch, caplog):
        from unittest.mock import MagicMock

        mock_provider = MagicMock()

        monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
        monkeypatch.setattr(main, "compute_all_scores", lambda **kwargs: pd.DataFrame())
        monkeypatch.setattr(main, "compute_degraded_scores", lambda url: None)

        with caplog.at_level("INFO", logger=main.logger.name):
            result = main.run_scoring()

        assert result is False
        mock_provider.write_scores.assert_not_called()
        assert any("mode=failed" in r.getMessage() for r in caplog.records)

    def test_falls_back_to_degraded_when_pipeline_result_is_unusable(self, monkeypatch, caplog):
        """A pipeline result that breaks the empty check must fall back, not crash the job."""
        from unittest.mock import MagicMock

        mock_provider = MagicMock()
        degraded_df = self._df("degraded")

        monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
        monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
        monkeypatch.setattr(main, "compute_all_scores", lambda **kwargs: None)
        monkeypatch.setattr(main, "compute_degraded_scores", lambda url: degraded_df)

        with caplog.at_level("INFO", logger=main.logger.name):
            result = main.run_scoring()

        assert result is True
        mock_provider.write_scores.assert_called_once_with(degraded_df)
        assert any("mode=degraded" in r.getMessage() for r in caplog.records)
