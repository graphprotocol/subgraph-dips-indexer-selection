"""GET /scores/weighted and POST /get-score score each indexer per chain, and only per chain.

An indexer's total belongs to a chain: the prices and freshness that go into it only exist
there. Each chain's entry carries the indexer's total, its 4 factors and their parts, and the
indexer's queries on that chain, from the score job's queries_by_chain, so a reader who wants
1 number per indexer can take the query-weighted average of its chain totals.
"""

import logging
import math
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints
from iisa.iisa_http_endpoints import Settings, app
from iisa.score_loader import ScoresSnapshot

ABC, XYZ, ODD = "0xabc", "0xxyz", "0x123"


@pytest.fixture
def client() -> TestClient:
    """A service holding 3 indexers priced on arbitrum-one, 1 of them on base too. ABC reports
    its queries on each chain, XYZ none, and ODD a count that isn't a number."""
    iisa_http_endpoints.get_settings.cache_clear()
    iisa_http_endpoints._state.initialize(Settings())
    data = pd.DataFrame(
        {
            "indexer": [ABC, XYZ, ODD],
            "norm_lat_lin_reg_coefficient": [0.8, 0.6, 0.4],
            "norm_stake_to_fees": [0.5, 0.65, 0.3],
            "% up_x": [98.5, 100.0, 90.0],
            "average_status": [0.985, 1.0, 0.9],
            "dips_info_available": [True, True, True],
            "dips_supported_networks": [
                '["arbitrum-one", "base"]',
                '["arbitrum-one"]',
                '["arbitrum-one"]',
            ],
            "dips_min_grt_per_30_days": [
                '{"arbitrum-one": "100", "base": "40"}',
                '{"arbitrum-one": "50"}',
                '{"arbitrum-one": "70"}',
            ],
            "dips_min_grt_per_billion_entities_per_30_days": ["0.4", "0.2", "0.3"],
            "queries_by_chain": [
                {"arbitrum-one": 1_200, "base": 40},
                None,
                {"arbitrum-one": "lots"},
            ],
        }
    )
    manager = MagicMock()
    manager.snapshot = ScoresSnapshot(
        data=data, computed_at=datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
    )
    iisa_http_endpoints._state.data_manager = manager
    iisa_http_endpoints._state._history = data
    iisa_http_endpoints._state._initialized = True
    return TestClient(app, raise_server_exceptions=False)


def _weighted(client: TestClient) -> dict[str, dict]:
    response = client.get("/scores/weighted")
    assert response.status_code == 200
    return {entry["indexer"]: entry for entry in response.json()["scores"]}


def test_scores_weighted_gives_no_network_wide_total(client):
    entries = _weighted(client)

    assert set(entries) == {ABC, XYZ, ODD}
    assert {key for entry in entries.values() for key in entry} == {"indexer", "chains"}


def test_get_score_gives_no_network_wide_total(client):
    body = client.post("/get-score", json={"indexer_id": ABC}).json()

    assert set(body) == {"indexer_id", "found", "chains"}
    assert body["found"] is True
    assert set(body["chains"]) == {"arbitrum-one", "base"}


def test_each_chains_entry_keeps_its_total_factors_and_components(client):
    entries = _weighted(client)
    arbitrum = entries[ABC]["chains"]["arbitrum-one"]

    assert set(arbitrum) == {"weighted_score", "components", "factors", "queries"}
    assert set(arbitrum["factors"]) == {"uptime", "freshness", "quality", "value"}
    assert arbitrum["weighted_score"] == pytest.approx(math.prod(arbitrum["factors"].values()))
    assert arbitrum["components"]["base_price"] == pytest.approx(0.0)
    single = client.post("/get-score", json={"indexer_id": ABC}).json()
    assert single["chains"] == entries[ABC]["chains"]


def test_each_chain_carries_the_indexers_queries_there(client):
    entries = _weighted(client)

    assert entries[ABC]["chains"]["arbitrum-one"]["queries"] == 1_200
    assert entries[ABC]["chains"]["base"]["queries"] == 40


@pytest.mark.parametrize("indexer", [XYZ, ODD], ids=["not-sent", "not-a-number"])
def test_queries_are_null_when_the_job_sent_no_usable_count(client, indexer):
    assert _weighted(client)[indexer]["chains"]["arbitrum-one"]["queries"] is None


def test_a_count_that_cant_be_read_is_logged_and_one_not_sent_is_not(client, caplog):
    with caplog.at_level(logging.WARNING):
        _weighted(client)

    warnings = [r.getMessage() for r in caplog.records if "queries_by_chain" in r.getMessage()]
    assert len(warnings) == 1
    assert "1 queries_by_chain" in warnings[0] and "arbitrum-one" in warnings[0]
    assert "'lots'" in warnings[0]


def test_an_indexer_not_loaded_is_not_found(client):
    body = client.post("/get-score", json={"indexer_id": "0xnobody"}).json()

    assert body == {"indexer_id": "0xnobody", "found": False, "chains": None}
