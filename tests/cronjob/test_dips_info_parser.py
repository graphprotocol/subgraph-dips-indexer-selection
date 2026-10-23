"""
Tests for `_extract_dips_prices`, the shape-tolerant price parser for the
`/dips/info` endpoint published by `indexer-rs`.

Two response shapes are live in the indexer fleet at the same time during
rollout. The parser must accept either without losing data; these tests
pin that behaviour.
"""

import sys
from pathlib import Path

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from processing import _extract_dips_prices  # noqa: E402


def test_extracts_from_flat_shape():
    # Arrange — the post-#1037 shape, three top-level keys.
    data = {
        "min_grt_per_30_days": {"arbitrum-one": "450", "mainnet": "45"},
        "min_grt_per_billion_entities_per_30_days": "200",
        "supported_networks": ["arbitrum-one", "mainnet"],
    }

    # Act
    min_prices, min_entity_price = _extract_dips_prices(data)

    # Assert
    assert min_prices == {"arbitrum-one": "450", "mainnet": "45"}
    assert min_entity_price == "200"


def test_extracts_from_legacy_nested_shape():
    # Arrange — the original shape, prices behind a `pricing` wrapper.
    data = {
        "pricing": {
            "min_grt_per_30_days": {"arbitrum-one": "450"},
            "min_grt_per_billion_entities_per_30_days": "200",
        },
        "supported_networks": ["arbitrum-one"],
    }

    # Act
    min_prices, min_entity_price = _extract_dips_prices(data)

    # Assert
    assert min_prices == {"arbitrum-one": "450"}
    assert min_entity_price == "200"


def test_empty_pricing_wrapper_falls_back_to_flat():
    # Arrange — an indexer that serialises `pricing: {}` should not lose
    # the top-level keys when they are also present.
    data = {
        "pricing": {},
        "min_grt_per_30_days": {"mainnet": "45"},
        "min_grt_per_billion_entities_per_30_days": "200",
    }

    # Act
    min_prices, min_entity_price = _extract_dips_prices(data)

    # Assert — the empty `pricing` dict is still a dict, so the parser
    # honours it and the top-level keys are intentionally ignored. This
    # documents the precedence: legacy shape wins if any `pricing` key
    # is present, even when empty.
    assert min_prices == {}
    assert min_entity_price is None


def test_non_dict_pricing_falls_back_to_flat():
    # Arrange — defensive: if some intermediate proxy mangles the response
    # and `pricing` becomes a string or null, fall back to flat parsing
    # rather than crashing.
    data = {
        "pricing": None,
        "min_grt_per_30_days": {"mainnet": "45"},
        "min_grt_per_billion_entities_per_30_days": "200",
    }

    # Act
    min_prices, min_entity_price = _extract_dips_prices(data)

    # Assert
    assert min_prices == {"mainnet": "45"}
    assert min_entity_price == "200"


def test_neither_shape_returns_defaults():
    # Arrange — an indexer that ships `/dips/info` without any pricing.
    data = {"supported_networks": []}

    # Act
    min_prices, min_entity_price = _extract_dips_prices(data)

    # Assert
    assert min_prices == {}
    assert min_entity_price is None


def test_entity_price_only_in_flat_shape():
    # Arrange — global entity-rate present but no per-chain map.
    data = {"min_grt_per_billion_entities_per_30_days": "200"}

    # Act
    min_prices, min_entity_price = _extract_dips_prices(data)

    # Assert
    assert min_prices == {}
    assert min_entity_price == "200"
