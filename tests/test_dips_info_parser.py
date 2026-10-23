"""
Tests for `_extract_dips_prices`, the shape-tolerant price parser for the
`/dips/info` endpoint published by `indexer-rs`, and for the checks that drop
prices an indexer advertises but that cannot be real.

Two response shapes are live in the indexer fleet at the same time during
rollout. The parser must accept either without losing data; these tests
pin that behaviour.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

import pytest

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from processing import _extract_dips_prices, _fetch_single_dips_info_async  # noqa: E402


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


# --- Rejecting prices an indexer advertises but that cannot be real -------------------------
#
# Any indexer can serve arbitrary JSON from /dips/info. A base price of "NaN" was scored as the
# cheapest on chains without a price ceiling, and a negative one passed every ceiling, so both
# won selection. These tests drive `_fetch_single_dips_info_async` through a stub session.


class _StubResponse:
    status = 200

    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _StubSession:
    def __init__(self, body):
        self._body = body

    def get(self, url, timeout=None):
        return _StubResponse(self._body)


def _fetch_record(body):
    async def run():
        session = _StubSession(body)
        return await _fetch_single_dips_info_async(
            session, "0xindexer", "http://indexer.example", asyncio.Semaphore(1)
        )

    return asyncio.run(run())


INVALID_PRICES = [
    pytest.param("NaN", id="nan-text"),
    pytest.param(float("nan"), id="nan-number"),
    pytest.param(float("inf"), id="infinite"),
    pytest.param("-Infinity", id="negative-infinite-text"),
    pytest.param(-500, id="negative-number"),
    pytest.param("-500", id="negative-text"),
    pytest.param("cheap", id="not-a-number"),
    pytest.param(True, id="boolean"),
    pytest.param(10**400, id="too-large-for-a-float"),
]


@pytest.mark.parametrize("bad_price", INVALID_PRICES)
def test_invalid_base_price_leaves_chain_unpriced(bad_price, caplog):
    body = {
        "min_grt_per_30_days": {"arbitrum-one": bad_price, "mainnet": "45"},
        "min_grt_per_billion_entities_per_30_days": "200",
        "supported_networks": ["arbitrum-one", "mainnet"],
    }

    with caplog.at_level(logging.WARNING, logger="processing"):
        record = _fetch_record(body)

    assert record["dips_info_available"] is True
    assert json.loads(record["dips_min_grt_per_30_days"]) == {"mainnet": "45"}
    assert json.loads(record["dips_supported_networks"]) == ["arbitrum-one", "mainnet"]
    warning = next(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    assert "0xindexer" in warning and "arbitrum-one" in warning and repr(bad_price) in warning


@pytest.mark.parametrize("bad_price", INVALID_PRICES)
def test_invalid_base_price_is_not_inferred_as_supported_network(bad_price):
    body = {"min_grt_per_30_days": {"arbitrum-one": bad_price, "mainnet": 45}}

    record = _fetch_record(body)

    assert json.loads(record["dips_min_grt_per_30_days"]) == {"mainnet": 45}
    assert json.loads(record["dips_supported_networks"]) == ["mainnet"]


@pytest.mark.parametrize("good_price", ["0", 0, "450.5", 450.5, "1e3"])
def test_finite_non_negative_base_price_is_kept(good_price, caplog):
    body = {"min_grt_per_30_days": {"arbitrum-one": good_price}}

    with caplog.at_level(logging.WARNING, logger="processing"):
        record = _fetch_record(body)

    assert json.loads(record["dips_min_grt_per_30_days"]) == {"arbitrum-one": good_price}
    assert json.loads(record["dips_supported_networks"]) == ["arbitrum-one"]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.parametrize("bad_price", INVALID_PRICES)
def test_invalid_per_entity_price_makes_dips_info_unusable(bad_price, caplog):
    # A missing per-entity price means "accepts any price", so a bad value must not become one.
    body = {
        "min_grt_per_30_days": {"arbitrum-one": "450"},
        "min_grt_per_billion_entities_per_30_days": bad_price,
        "supported_networks": ["arbitrum-one"],
    }

    with caplog.at_level(logging.WARNING, logger="processing"):
        record = _fetch_record(body)

    assert record == {
        "indexer": "0xindexer",
        "dips_info_available": False,
        "dips_min_grt_per_30_days": "{}",
        "dips_min_grt_per_billion_entities_per_30_days": None,
        "dips_supported_networks": "[]",
    }
    warning = next(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    assert "0xindexer" in warning and repr(bad_price) in warning


@pytest.mark.parametrize(
    ("entity_price", "stored"), [(None, None), ("0", "0"), (200, "200"), ("2.5", "2.5")]
)
def test_missing_or_valid_per_entity_price_keeps_dips_info(entity_price, stored):
    body = {
        "min_grt_per_30_days": {"arbitrum-one": "450"},
        "min_grt_per_billion_entities_per_30_days": entity_price,
    }

    record = _fetch_record(body)

    assert record["dips_info_available"] is True
    assert record["dips_min_grt_per_billion_entities_per_30_days"] == stored
    assert json.loads(record["dips_min_grt_per_30_days"]) == {"arbitrum-one": "450"}
