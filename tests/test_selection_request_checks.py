"""How strictly POST /select-indexers checks the request it is sent.

Dipper keeps its own copy of the request's fields in step with this service by hand, so a field
it renames must be refused rather than dropped without a word: a dropped blocklist or price
ceiling would select blocked or over-priced indexers.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import app

CHAIN = "arbitrum-one"
INDEXERS = ["0x" + "a1" * 20, "0x" + "b2" * 20, "0x" + "c3" * 20]


def _score_record(indexer: str) -> dict:
    """1 indexer's scores in the shape the daily score job pushes."""
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_std_error": 0.002,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "observed_duration_seconds": 2_419_200.0,
        "uptime_duration_seconds": 2_395_000.0,
        "success_rate": 0.98,
        "stake_to_fees": 1_500.0,
        "total_query_fees": 2_000.0,
        "last_known_slashable_stake": 3_000_000.0,
        "norm_uptime_score": 0.9,
        "norm_success_rate": 0.85,
        "org": f"Hosting {indexer[2:4]}",
        "dst_lat": 52.4,
        "dst_lon": 4.9,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": json.dumps({CHAIN: "450"}),
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": json.dumps([CHAIN]),
        "graph_node_version": "0.36.0",
        "graph_node_commit": "abc1234",
        "computed_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "query_count": 12_000,
    }


@pytest.fixture(autouse=True)
def loaded_scores(monkeypatch, tmp_path):
    """Fresh app state holding 3 priced indexers, put back afterwards."""
    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    iisa_http_endpoints.get_settings.cache_clear()
    state = iisa_http_endpoints.IISAState()
    state.initialize(iisa_http_endpoints.get_settings())
    state.load_scores_from_records([_score_record(indexer) for indexer in INDEXERS])
    iisa_http_endpoints._state = state
    yield
    iisa_http_endpoints._state = iisa_http_endpoints.IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _post(body, **kwargs):
    return TestClient(app, raise_server_exceptions=False).post(
        "/select-indexers", json=body, **kwargs
    )


def _request(**fields):
    return {"deployment_id": "QmDeployment", "chain_id": CHAIN, "num_candidates": 2, **fields}


# --- Unknown fields -------------------------------------------------------------------------


# Every field dipper sends today, so none of them is refused by mistake.
DIPPER_REQUEST = {
    "deployment_id": "QmDeployment",
    "existing_indexers": [INDEXERS[0]],
    "pending_agreements": {"QmOther": [INDEXERS[1]]},
    "num_candidates": 2,
    "blocklist": ["0x" + "ff" * 20],
    "declined_indexers": {"QmDeployment": ["0x" + "ee" * 20]},
    "chain_id": CHAIN,
    "max_grt_per_30_days": 5_000.0,
    "max_grt_per_billion_entities_per_30_days": 1_000.0,
    "optimistic_dips_fees": {INDEXERS[2]: 50.0},
}


def test_every_field_dipper_sends_is_accepted():
    response = _post(DIPPER_REQUEST)

    assert response.status_code == 200, response.text


@pytest.mark.parametrize("field", ["block_list", "declined", "max_grt_per_month"])
def test_an_unknown_field_is_refused_and_named(field):
    response = _post({**DIPPER_REQUEST, field: []})

    assert response.status_code == 422
    errors = response.json()["detail"]
    assert [(e["type"], e["loc"]) for e in errors] == [("extra_forbidden", ["body", field])]


def test_the_published_schema_says_unknown_fields_are_refused():
    schema = app.openapi()["components"]["schemas"]["SelectionRequest"]

    assert schema["additionalProperties"] is False
