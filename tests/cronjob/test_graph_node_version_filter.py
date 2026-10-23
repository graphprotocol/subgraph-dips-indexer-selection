"""Tests for the graph-node minimum-version filter applied during scoring.

Covers the semver comparison helper, the DataFrame filter (strict +
fail-open modes), and the env-var getter. Network fetches against /status
live in test_graph_node_version_fetcher.py.
"""

import sys
from pathlib import Path

import pandas as pd

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from processing import (  # noqa: E402
    _get_min_graph_node_version_strict,
    _meets_min_graph_node_version,
    filter_by_min_graph_node_version,
)


def test_meets_min_version_above():
    assert _meets_min_graph_node_version("0.40.1", "0.40.0") is True


def test_meets_min_version_equal():
    assert _meets_min_graph_node_version("0.40.0", "0.40.0") is True


def test_meets_min_version_below():
    assert _meets_min_graph_node_version("0.39.9", "0.40.0") is False


def test_meets_min_version_double_digit_patch():
    # Naive string compare would say "0.40.2" > "0.40.10". PEP 440 semantics
    # gets this right.
    assert _meets_min_graph_node_version("0.40.10", "0.40.2") is True


def test_meets_min_version_prerelease_below_stable():
    # Pre-release versions sort below the matching stable, so a 0.40.0-rc1
    # build does not meet a 0.40.0 minimum.
    assert _meets_min_graph_node_version("0.40.0-rc1", "0.40.0") is False


def test_meets_min_version_unknown_reported():
    assert _meets_min_graph_node_version(None, "0.40.0") is False


def test_meets_min_version_empty_reported():
    assert _meets_min_graph_node_version("", "0.40.0") is False


def test_meets_min_version_unparseable_reported():
    # Any version string that fails PEP 440 parsing is treated as not-met
    # so strict mode can reject malformed responses.
    assert _meets_min_graph_node_version("not-a-version", "0.40.0") is False


def test_meets_min_version_empty_minimum_returns_false():
    # The helper is a pure comparison; the caller turns the filter off
    # when MIN_GRAPH_NODE_VERSION is unset. Returning False on empty
    # minimum prevents an accidental call from silently fail-opening.
    assert _meets_min_graph_node_version("0.40.0", "") is False


def _scores_df_with_versions(rows):
    """Build a scores-like DataFrame with the columns the filter touches."""
    return pd.DataFrame(rows, columns=["indexer", "graph_node_version"])


def test_filter_empty_minimum_is_noop():
    # An empty MIN_GRAPH_NODE_VERSION should pass every row through, even
    # ones with no reported version, so operators can leave the filter
    # disabled while keeping the column plumbing in place.
    df = _scores_df_with_versions(
        [
            ("0xaaa", "0.39.0"),
            ("0xbbb", None),
            ("0xccc", "0.40.0"),
        ]
    )
    out = filter_by_min_graph_node_version(df, "", strict=False)
    assert list(out["indexer"]) == ["0xaaa", "0xbbb", "0xccc"]


def test_filter_fail_open_keeps_unknown_versions():
    # Default rollout posture: an indexer whose /status was unreachable
    # appears with None and should stay in the candidate pool. Only the
    # known-too-old indexer is dropped.
    df = _scores_df_with_versions(
        [
            ("0xaaa", "0.39.0"),
            ("0xbbb", None),
            ("0xccc", "0.40.0"),
        ]
    )
    out = filter_by_min_graph_node_version(df, "0.40.0", strict=False)
    assert list(out["indexer"]) == ["0xbbb", "0xccc"]


def test_filter_strict_drops_unknown_versions():
    # Strict mode: unknown == not-eligible. After the rollout window has
    # closed, we expect every indexer to expose a version, so failing the
    # probe is a real reason to exclude.
    df = _scores_df_with_versions(
        [
            ("0xaaa", "0.39.0"),
            ("0xbbb", None),
            ("0xccc", "0.40.0"),
        ]
    )
    out = filter_by_min_graph_node_version(df, "0.40.0", strict=True)
    assert list(out["indexer"]) == ["0xccc"]


def test_filter_missing_column_skips_with_warning(caplog):
    # Defense: if the version column never made it onto the DataFrame
    # (e.g. an earlier merge step was skipped), don't crash — log and
    # pass everything through.
    df = pd.DataFrame([("0xaaa",), ("0xbbb",)], columns=["indexer"])
    out = filter_by_min_graph_node_version(df, "0.40.0", strict=True)
    assert list(out["indexer"]) == ["0xaaa", "0xbbb"]


def test_filter_handles_nan_from_pandas_string_dtype():
    # Regression: pandas's arrow-backed string dtype renders missing values
    # as float NaN (truthy), which once slipped past the helper into
    # Version() and crashed. Filter must treat NaN the same as None.
    import numpy as np

    df = pd.DataFrame(
        {
            "indexer": ["0xaaa", "0xbbb", "0xccc"],
            "graph_node_version": ["0.39.0", np.nan, "0.40.0"],
        }
    )
    out_open = filter_by_min_graph_node_version(df, "0.40.0", strict=False)
    assert list(out_open["indexer"]) == ["0xbbb", "0xccc"]

    out_strict = filter_by_min_graph_node_version(df, "0.40.0", strict=True)
    assert list(out_strict["indexer"]) == ["0xccc"]


def test_meets_min_version_nan_input():
    # Direct sanity-check on the comparison helper: a NaN float (the shape
    # pandas hands us) must short-circuit to False rather than crash.
    import math

    assert _meets_min_graph_node_version(math.nan, "0.40.0") is False


def test_filter_resets_index_after_drop():
    # A dropped row in the middle should leave the surviving rows with a
    # clean 0..N-1 index, not a gap (which downstream pd.merge can
    # tolerate but pd.concat can stumble on).
    df = _scores_df_with_versions(
        [
            ("0xaaa", "0.40.0"),
            ("0xbbb", "0.39.0"),
            ("0xccc", "0.40.1"),
        ]
    )
    out = filter_by_min_graph_node_version(df, "0.40.0", strict=False)
    assert list(out.index) == [0, 1]
    assert list(out["indexer"]) == ["0xaaa", "0xccc"]


def test_strict_default_is_true_when_env_unset(monkeypatch):
    monkeypatch.delenv("MIN_GRAPH_NODE_VERSION_STRICT", raising=False)
    assert _get_min_graph_node_version_strict() is True


def test_strict_false_disables(monkeypatch):
    monkeypatch.setenv("MIN_GRAPH_NODE_VERSION_STRICT", "false")
    assert _get_min_graph_node_version_strict() is False


def test_strict_accepts_other_falsy_spellings(monkeypatch):
    for value in ("0", "no", "off", "False", "OFF"):
        monkeypatch.setenv("MIN_GRAPH_NODE_VERSION_STRICT", value)
        assert _get_min_graph_node_version_strict() is False, f"falsy={value!r}"


def test_strict_truthy_values_stay_strict(monkeypatch):
    for value in ("true", "1", "yes", "on", "TRUE"):
        monkeypatch.setenv("MIN_GRAPH_NODE_VERSION_STRICT", value)
        assert _get_min_graph_node_version_strict() is True, f"truthy={value!r}"


def test_strict_fails_safe_on_typo(monkeypatch):
    # Operator types "fasle" instead of "false" — the getter must default
    # to strict rather than silently fail-open.
    monkeypatch.setenv("MIN_GRAPH_NODE_VERSION_STRICT", "fasle")
    assert _get_min_graph_node_version_strict() is True


def test_strict_whitespace_only_value_defaults_strict(monkeypatch):
    # Some k8s tooling renders an unset value as whitespace. `.strip()`
    # collapses it to empty, which must take the same default-strict path.
    monkeypatch.setenv("MIN_GRAPH_NODE_VERSION_STRICT", "   ")
    assert _get_min_graph_node_version_strict() is True


def test_strict_explicit_empty_string_defaults_strict(monkeypatch):
    # Explicit empty string (vs unset) should also default to strict — the
    # falsy-allowlist only excludes "false"/"0"/"no"/"off".
    monkeypatch.setenv("MIN_GRAPH_NODE_VERSION_STRICT", "")
    assert _get_min_graph_node_version_strict() is True
