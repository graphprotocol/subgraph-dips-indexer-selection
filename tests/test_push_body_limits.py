"""How much body POST /scores, /sync-status, /freshness and /get-score read before refusing it.

FastAPI reads and parses a whole body before a route checks its token, so without a limit a caller
with no token could make the service hold a body of any size. Each route has its own limit, sized
from what its real sender pushes.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import (
    MAX_FRESHNESS_BODY_BYTES,
    MAX_GET_SCORE_BODY_BYTES,
    MAX_SCORES_BODY_BYTES,
    MAX_SYNC_STATUS_BODY_BYTES,
    IISAState,
    Settings,
    _BodyLimit,
    app,
)

TOKEN = "push-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
JSON = {"Content-Type": "application/json"}

LIMITS = {
    "/scores": MAX_SCORES_BODY_BYTES,
    "/sync-status": MAX_SYNC_STATUS_BODY_BYTES,
    "/freshness": MAX_FRESHNESS_BODY_BYTES,
    "/get-score": MAX_GET_SCORE_BODY_BYTES,
}

# Around each route's padding, a body the route accepts as far as its token check.
_SHAPES = {
    "/scores": (b'[{"padding": "', b'"}]'),
    "/sync-status": (b'{"padding": "', b'"}'),
    "/freshness": (b'{"minute": "2026-01-01T00:00:00Z", "outcomes": ["', b'"]}'),
    "/get-score": (b'{"indexer_id": "', b'"}'),
}


@pytest.fixture(autouse=True)
def service(monkeypatch, tmp_path):
    """An initialised service whose pushes need TOKEN, with its files in a temporary folder."""
    monkeypatch.setenv("IISA_PUSH_TOKEN", TOKEN)
    monkeypatch.delenv("IISA_READ_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(tmp_path / "freshness.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    iisa_http_endpoints.get_settings.cache_clear()
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints._state.initialize(Settings())
    yield
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _post(path, content, headers=None):
    return TestClient(app, raise_server_exceptions=False).post(
        path, content=content, headers={**JSON, **(headers or {})}
    )


def _body(path, size):
    """A body of exactly ``size`` bytes that the route reads as far as its token check."""
    before, after = _SHAPES[path]
    return before + b"x" * (size - len(before) - len(after)) + after


def _in_pieces(body, piece=64 * 1024):
    """The body as a stream, so it is sent with no declared length."""
    for start in range(0, len(body), piece):
        yield body[start : start + piece]


# --- Over the limit ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", LIMITS)
def test_a_body_declared_over_the_limit_is_refused_before_the_token_check(path):
    response = _post(path, _body(path, LIMITS[path] + 1))

    assert response.status_code == 413
    assert f"{LIMITS[path]:,} byte limit" in response.json()["detail"]


@pytest.mark.parametrize("path", LIMITS)
def test_a_body_with_no_declared_length_is_refused_once_over_the_limit(path):
    response = _post(path, _in_pieces(_body(path, LIMITS[path] + 1)))

    assert response.status_code == 413
    assert f"{LIMITS[path]:,} byte limit" in response.json()["detail"]


@pytest.mark.parametrize("stream", [False, True], ids=["declared-length", "no-declared-length"])
@pytest.mark.parametrize("path", LIMITS)
def test_a_body_at_the_limit_reaches_the_route(path, stream):
    body = _body(path, LIMITS[path])

    response = _post(path, _in_pieces(body) if stream else body)

    # Refused by the route's own token check, so the limit let the whole body through.
    assert response.status_code == 401, response.text


# --- What the limit applies to ------------------------------------------------------------------


async def _through_the_limit(method, path, body, headers=()):
    """Send ``body`` in 2 pieces through the limit to a stand-in route.

    Returns the answer's status and the body the route read, None if it never ran.
    """
    half = len(body) // 2
    messages = [
        {"type": "http.request", "body": body[:half], "more_body": True},
        {"type": "http.request", "body": body[half:], "more_body": False},
    ]
    sent = []
    read_by_route = []

    async def receive():
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    async def route(scope, receive, send):
        read = b""
        while True:
            message = await receive()
            read += message.get("body", b"")
            if not message.get("more_body", False):
                break
        read_by_route.append(read)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    scope = {"type": "http", "method": method, "path": path, "headers": list(headers)}
    await _BodyLimit(route)(scope, receive, send)
    return sent[0]["status"], (read_by_route[0] if read_by_route else None)


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
@pytest.mark.parametrize("path", ["/scores", "/freshness"])
async def test_a_method_that_carries_no_body_passes_through_unread(method, path):
    body = b"x" * (LIMITS[path] + 1)

    status, read = await _through_the_limit(method, path, body)

    assert status == 200
    assert read == body


# --- Real pushes --------------------------------------------------------------------------------
#
# Each at about today's size and at the larger size its limit is sized to hold.


def _chains(count: int) -> list[str]:
    return [f"chain-name-{i:03d}" for i in range(count)]


def _score_record(n: int, chains: list[str]) -> dict:
    """1 indexer's scores in the shape the daily score job pushes, on each of ``chains``."""
    return {
        "indexer": f"0x{n:040x}",
        "url": f"https://indexer-{n}.some-operator.example.com/",
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_std_error": 0.002,
        "lat_coefficient_upper_bound": 0.016,
        "latency_ms": 180.5,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "observed_duration_seconds": 2_419_200.0,
        "uptime_duration_seconds": 2_395_000.0,
        "success_rate": 0.98,
        "uptime_by_chain": dict.fromkeys(chains, 0.987654321),
        "success_rate_by_chain": dict.fromkeys(chains, 0.976543210),
        "queries_by_chain": dict.fromkeys(chains, 1_234_567),
        "stake_to_fees": 1_500.0,
        "total_query_fees": 2_000.0,
        "last_known_slashable_stake": 3_000_000.0,
        "stake_read_at": datetime.now(timezone.utc).isoformat(),
        "org": "Some Hosting Provider GmbH",
        "dst_lat": 52.4,
        "dst_lon": 4.9,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": json.dumps(dict.fromkeys(chains, 450.0)),
        "dips_min_grt_per_billion_entities_per_30_days": "200.0",
        "dips_supported_networks": json.dumps(chains),
        "graph_node_version": "0.36.1",
        "graph_node_commit": "0123456789abcdef0123456789abcdef01234567",
        "computed_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "query_count": 12_000,
    }


@pytest.mark.parametrize("indexers, chains", [(300, 50), (1_000, 100)])
def test_a_real_scores_push_is_accepted(indexers, chains):
    records = [_score_record(n, _chains(chains)) for n in range(indexers)]

    response = _post("/scores", json.dumps(records), AUTH)

    assert response.status_code == 200, response.text
    assert response.json() == {"status": "success", "rows": indexers}


@pytest.mark.parametrize("indexers, deployments", [(500, 100), (1_000, 330)])
def test_a_real_sync_status_push_is_accepted(indexers, deployments):
    fetched_at = datetime.now(timezone.utc).isoformat()
    snapshot = {
        f"0x{n:040x}": {
            "deployments": [f"Qm{n:022d}{d:022d}" for d in range(deployments)],
            "fetched_at": fetched_at,
        }
        for n in range(indexers)
    }

    response = _post("/sync-status", json.dumps(snapshot), AUTH)

    assert response.status_code == 200, response.text
    assert response.json() == {"status": "success", "indexers": indexers}


@pytest.mark.parametrize("indexers, chains", [(100, 50), (500, 100)])
def test_a_real_freshness_push_is_accepted(indexers, chains):
    minute = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
    outcomes = [
        {
            "indexer": f"0x{n:040x}",
            "chain": chain,
            "outcome": "pass",
            "reason": None,
            "detail": None,
            "deployment": f"Qm{n:044d}",
            "age_seconds": 12.345678,
        }
        for n in range(indexers)
        for chain in _chains(chains)
    ]
    push = {"minute": minute.isoformat().replace("+00:00", "Z"), "outcomes": outcomes}

    response = _post("/freshness", json.dumps(push), AUTH)

    assert response.status_code == 200, response.text
    assert response.json()["pairs"] == indexers * chains


def test_a_real_score_request_is_answered():
    iisa_http_endpoints._state.load_scores_from_records(
        [_score_record(n, _chains(50)) for n in range(3)]
    )

    response = _post("/get-score", json.dumps({"indexer_id": f"0x{1:040x}"}), AUTH)

    assert response.status_code == 200, response.text
    assert response.json()["found"] is True
