"""A second bearer token that reads but cannot push.

Dipper only reads from this service, but held the 1 token that also pushes the scores and sync
status deciding who is paid. With IISA_READ_TOKEN set, the endpoints that only read accept it as
well as the push token, and the 2 that push accept only the push token. Unset, nothing changes.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import app

PUSH_TOKEN = "push-token-for-tests"
READ_TOKEN = "read-token-for-tests"
CHAIN = "arbitrum-one"
INDEXER = "0x" + "a1" * 20


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
        "org": "Example Hosting",
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


READS = [
    pytest.param("GET", "/scores/status", None, id="scores-status"),
    pytest.param("GET", "/scores", None, id="scores"),
    pytest.param("GET", "/scores/weighted", None, id="scores-weighted"),
    pytest.param("GET", f"/dips-indexers?chain={CHAIN}", None, id="dips-indexers"),
    pytest.param("POST", "/get-score", {"indexer_id": INDEXER}, id="get-score"),
]
WRITES = [
    pytest.param("POST", "/scores", [_score_record(INDEXER)], id="push-scores"),
    pytest.param(
        "POST",
        "/sync-status",
        {INDEXER: {"deployments": ["QmOne"], "fetched_at": datetime.now(timezone.utc).isoformat()}},
        id="push-sync-status",
    ),
]


@pytest.fixture
def tokens(monkeypatch, tmp_path):
    """Set the given tokens, unsetting any not given, and load 1 indexer's scores."""

    def configure(push=None, read=None):
        for name, value in (("IISA_PUSH_TOKEN", push), ("IISA_READ_TOKEN", read)):
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)
        monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
        monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "scores.json"))
        iisa_http_endpoints.get_settings.cache_clear()
        state = iisa_http_endpoints.IISAState()
        state.initialize(iisa_http_endpoints.get_settings())
        state.load_scores_from_records([_score_record(INDEXER)])
        iisa_http_endpoints._state = state

    yield configure
    iisa_http_endpoints._state = iisa_http_endpoints.IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _call(method, path, body, token):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    client = TestClient(app, raise_server_exceptions=False)
    return client.request(method, path, json=body, headers=headers)


# --- With both tokens set -------------------------------------------------------------------


@pytest.mark.parametrize(("method", "path", "body"), READS)
def test_the_read_token_reads(tokens, method, path, body):
    tokens(push=PUSH_TOKEN, read=READ_TOKEN)

    assert _call(method, path, body, READ_TOKEN).status_code == 200


@pytest.mark.parametrize(("method", "path", "body"), WRITES)
def test_the_read_token_cannot_push(tokens, method, path, body):
    tokens(push=PUSH_TOKEN, read=READ_TOKEN)

    response = _call(method, path, body, READ_TOKEN)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid bearer token"}


@pytest.mark.parametrize(("method", "path", "body"), READS + WRITES)
def test_the_push_token_still_reads_and_pushes(tokens, method, path, body):
    tokens(push=PUSH_TOKEN, read=READ_TOKEN)

    assert _call(method, path, body, PUSH_TOKEN).status_code == 200


@pytest.mark.parametrize(("method", "path", "body"), READS + WRITES)
def test_any_other_token_is_refused(tokens, method, path, body):
    tokens(push=PUSH_TOKEN, read=READ_TOKEN)

    assert _call(method, path, body, "not-either-token").status_code == 401


# --- With the read token unset, as before ---------------------------------------------------


@pytest.mark.parametrize(("method", "path", "body"), READS + WRITES)
def test_without_a_read_token_the_push_token_does_everything(tokens, method, path, body):
    tokens(push=PUSH_TOKEN)

    assert _call(method, path, body, PUSH_TOKEN).status_code == 200
    assert _call(method, path, body, READ_TOKEN).status_code == 401
    assert _call(method, path, body, None).status_code == 401


@pytest.mark.parametrize(("method", "path", "body"), READS + WRITES)
def test_without_any_token_nothing_is_checked(tokens, method, path, body):
    # Local development only; the service warns about it at startup.
    tokens()

    assert _call(method, path, body, None).status_code == 200


# --- Startup --------------------------------------------------------------------------------


def test_a_read_token_equal_to_the_push_token_is_warned_about(tokens, monkeypatch, caplog):
    # It would let whoever holds the "read" token push as well.
    tokens(push=PUSH_TOKEN, read=PUSH_TOKEN)

    with caplog.at_level("WARNING"), TestClient(app):
        pass

    assert any("IISA_READ_TOKEN" in r.getMessage() for r in caplog.records)
