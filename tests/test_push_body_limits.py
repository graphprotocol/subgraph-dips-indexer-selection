"""What POST /scores, /sync-status, /freshness and /get-score check before parsing a body.

FastAPI reads and parses a whole body before a route runs, so the token of a route that takes 1
is checked first, before any of the body is read, and each route's body has a limit sized from
what its real sender pushes.
"""

import json
import random
import re
import tracemalloc
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import (
    _BODY_ROUTES,
    DEFAULT_BODY_LIMIT,
    MAX_FRESHNESS_BODY_BYTES,
    MAX_GET_SCORE_BODY_BYTES,
    MAX_SCORES_BODY_BYTES,
    MAX_SYNC_STATUS_BODY_BYTES,
    IISAState,
    Settings,
    _BodyLimit,
    app,
)

REPO = Path(__file__).resolve().parents[1]
TOKEN = "push-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
AUTH_HEADER = [(b"authorization", f"Bearer {TOKEN}".encode())]
JSON = {"Content-Type": "application/json"}

LIMITS = {
    "/scores": MAX_SCORES_BODY_BYTES,
    "/sync-status": MAX_SYNC_STATUS_BODY_BYTES,
    "/freshness": MAX_FRESHNESS_BODY_BYTES,
    "/get-score": MAX_GET_SCORE_BODY_BYTES,
}
PUSH_ROUTES = ["/scores", "/sync-status", "/freshness"]


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


def _padded(size):
    """A JSON body of exactly ``size`` bytes."""
    return b'{"padding": "' + b"x" * (size - 15) + b'"}'


def _in_pieces(body, piece=64 * 1024):
    """The body as a stream, so it is sent with no declared length."""
    for start in range(0, len(body), piece):
        yield body[start : start + piece]


async def _through_the_limit(method, path, body, headers=(), root_path=""):
    """Send ``body`` in 2 pieces through the limit to a stand-in route.

    Returns the answer's status, the body the route read (None if it never ran), and how many
    times the limit or the route asked the server for more of the body.
    """
    half = len(body) // 2
    messages = [
        {"type": "http.request", "body": body[:half], "more_body": True},
        {"type": "http.request", "body": body[half:], "more_body": False},
    ]
    asked = 0
    sent = []
    read_by_route = []

    async def receive():
        nonlocal asked
        asked += 1
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

    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "root_path": root_path,
        "headers": list(headers),
    }
    await _BodyLimit(route)(scope, receive, send)
    return sent[0]["status"], (read_by_route[0] if read_by_route else None), asked


# --- The token, checked before the body -------------------------------------------------------


@pytest.mark.parametrize(
    "authorization",
    [None, "Bearer wrong", f"Basic {TOKEN}", TOKEN],
    ids=["none", "wrong-token", "not-bearer", "no-scheme"],
)
@pytest.mark.parametrize("path", LIMITS)
async def test_a_request_without_the_token_is_refused_before_any_body_is_read(path, authorization):
    headers = [] if authorization is None else [(b"authorization", authorization.encode())]

    status, read, asked = await _through_the_limit("POST", path, _padded(1_000), headers)

    assert status == 401
    assert (read, asked) == (None, 0)


@pytest.mark.parametrize("authorization", [None, "Bearer wrong"], ids=["none", "wrong-token"])
@pytest.mark.parametrize("path", LIMITS)
def test_the_refusal_is_the_one_the_routes_give(path, authorization):
    headers = {} if authorization is None else {"Authorization": authorization}
    client = TestClient(app, raise_server_exceptions=False)

    early = client.post(path, content=_padded(1_000), headers={**JSON, **headers})
    # GET /scores has no body, so its token is checked by the route itself.
    by_route = client.get("/scores", headers=headers)

    assert early.status_code == by_route.status_code == 401
    assert early.content == by_route.content
    assert early.headers["content-type"] == by_route.headers["content-type"]


async def test_the_read_token_is_let_through_to_get_score_but_to_no_push(monkeypatch):
    monkeypatch.setenv("IISA_READ_TOKEN", "read-token")
    iisa_http_endpoints.get_settings.cache_clear()
    headers = [(b"authorization", b"Bearer read-token")]

    to_get_score = await _through_the_limit("POST", "/get-score", _padded(100), headers)
    to_pushes = [await _through_the_limit("POST", p, _padded(100), headers) for p in PUSH_ROUTES]

    assert to_get_score[0] == 200
    assert [status for status, _, _ in to_pushes] == [401] * len(PUSH_ROUTES)


@pytest.mark.parametrize("path", LIMITS)
async def test_the_push_token_with_bearer_in_any_letter_case_is_let_through(path):
    headers = [(b"authorization", f"bEaReR {TOKEN}".encode())]

    status, read, _ = await _through_the_limit("POST", path, _padded(100), headers)

    assert (status, read) == (200, _padded(100))


@pytest.mark.parametrize("path", LIMITS)
async def test_with_no_token_set_a_request_without_one_is_let_through(monkeypatch, path):
    monkeypatch.delenv("IISA_PUSH_TOKEN")
    iisa_http_endpoints.get_settings.cache_clear()

    status, read, _ = await _through_the_limit("POST", path, _padded(100))

    assert (status, read) == (200, _padded(100))


# --- Over the limit ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", LIMITS)
def test_a_body_declared_over_the_limit_is_refused(path):
    response = _post(path, _padded(LIMITS[path] + 1), AUTH)

    assert response.status_code == 413
    assert f"{LIMITS[path]:,} byte limit" in response.json()["detail"]


@pytest.mark.parametrize("path", LIMITS)
def test_a_body_with_no_declared_length_is_refused_once_over_the_limit(path):
    response = _post(path, _in_pieces(_padded(LIMITS[path] + 1)), AUTH)

    assert response.status_code == 413
    assert f"{LIMITS[path]:,} byte limit" in response.json()["detail"]


@pytest.mark.parametrize("declare", [False, True], ids=["no-declared-length", "declared-length"])
@pytest.mark.parametrize("path", LIMITS)
async def test_a_body_at_the_limit_reaches_the_route_whole(path, declare):
    body = _padded(LIMITS[path])
    length = [(b"content-length", str(len(body)).encode())] if declare else []

    status, read, _ = await _through_the_limit("POST", path, body, AUTH_HEADER + length)

    assert (status, read) == (200, body)


# --- What the limit applies to ------------------------------------------------------------------


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
@pytest.mark.parametrize("path", ["/scores", "/freshness"])
async def test_a_method_that_carries_no_body_passes_through_unread(method, path):
    body = b"x" * (LIMITS[path] + 1)

    status, read, _ = await _through_the_limit(method, path, body)

    assert status == 200
    assert read == body


@pytest.mark.parametrize(
    "method, path", [("POST", "/no-such-route"), ("PUT", "/scores"), ("POST", "/scores/")]
)
async def test_a_route_with_no_limit_of_its_own_is_held_to_the_default(method, path):
    status, _, _ = await _through_the_limit(method, path, b"x" * (DEFAULT_BODY_LIMIT + 1))
    status_at_limit, _, _ = await _through_the_limit(method, path, b"x" * DEFAULT_BODY_LIMIT)

    assert status == 413
    assert status_at_limit == 200


async def test_a_route_under_a_root_path_keeps_its_own_limit_and_token():
    within, over = b"x" * (DEFAULT_BODY_LIMIT + 1), b"x" * (MAX_SCORES_BODY_BYTES + 1)

    status_within, read, _ = await _through_the_limit(
        "POST", "/iisa/scores", within, AUTH_HEADER, root_path="/iisa"
    )
    status_over, _, _ = await _through_the_limit(
        "POST", "/iisa/scores", over, AUTH_HEADER, root_path="/iisa"
    )
    no_token, _, _ = await _through_the_limit("POST", "/iisa/scores", within, root_path="/iisa")

    assert (status_within, read) == (200, within)
    assert status_over == 413
    assert no_token == 401


def test_every_route_that_takes_a_body_has_a_limit_of_its_own():
    unlisted = [
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods & {"POST", "PUT", "PATCH", "DELETE"}
        if (method, route.path) not in _BODY_ROUTES
    ]

    assert unlisted == []


# --- Real pushes --------------------------------------------------------------------------------
#
# Each at about today's size and at the larger size its limit is sized to hold.


def _chains(count: int) -> list[str]:
    return [f"chain-name-{i:03d}" for i in range(count)]


def _score_record(n: int, chains: list[str]) -> dict:
    """1 indexer's scores in the shape the daily score job pushes, on each of ``chains``.

    Its measures are random, so each is written with all the digits a float can take; the job
    rounds to 10 decimal places, so its pushes are no longer than these.
    """
    rng = random.Random(n)
    return {
        "indexer": f"0x{n:040x}",
        "url": f"https://indexer-{n}.some-operator.example.com/",
        "lat_lin_reg_coefficient": rng.random(),
        "lat_coefficient_std_error": rng.random(),
        "lat_coefficient_upper_bound": rng.random(),
        "latency_ms": rng.uniform(10, 2_000),
        "lat_normalized_score": rng.random(),
        "uptime_score": rng.random(),
        "observed_duration_seconds": 2_419_200.0,
        "uptime_duration_seconds": rng.uniform(0, 2_419_200),
        "success_rate": rng.random(),
        "uptime_by_chain": {chain: rng.random() for chain in chains},
        "success_rate_by_chain": {chain: rng.random() for chain in chains},
        "queries_by_chain": {chain: rng.randrange(10_000_000) for chain in chains},
        "stake_to_fees": rng.uniform(0, 1e6),
        "total_query_fees": rng.uniform(0, 1e5),
        "last_known_slashable_stake": rng.uniform(1e5, 1e8),
        "stake_read_at": datetime.now(timezone.utc).isoformat(),
        "org": "Some Hosting Provider GmbH",
        "dst_lat": rng.uniform(-90, 90),
        "dst_lon": rng.uniform(-180, 180),
        "dips_info_available": True,
        "dips_min_grt_per_30_days": json.dumps(
            {chain: rng.uniform(100, 5_000) for chain in chains}
        ),
        "dips_min_grt_per_billion_entities_per_30_days": str(rng.uniform(100, 5_000)),
        "dips_supported_networks": json.dumps(chains),
        "graph_node_version": "0.36.1",
        "graph_node_commit": "0123456789abcdef0123456789abcdef01234567",
        "computed_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "query_count": 12_000,
    }


@pytest.mark.parametrize("indexers, chains", [(300, 50), (1_000, 100), (1_900, 100)])
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


def _fault_minute(count: int, detail: str) -> bytes:
    """A minute of ``count`` void outcomes, each carrying ``detail``, as the prober sends it."""
    minute = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
    outcomes = [
        {
            "indexer": f"0x{n // 100:040x}",
            "chain": f"chain-name-{n % 100:03d}",
            "outcome": "void",
            "reason": "general_fault",
            "detail": detail,
            "deployment": f"Qm{n:044d}",
            "age_seconds": None,
        }
        for n in range(count)
    ]
    push = {"minute": minute.isoformat().replace("+00:00", "Z"), "outcomes": outcomes}
    return json.dumps(push).encode()


def test_a_full_minute_of_outcomes_with_long_details_is_accepted():
    body = _fault_minute(50_000, "indexer_error: " + "x" * 285)

    response = _post("/freshness", body, AUTH)

    assert response.status_code == 200, response.text
    assert response.json()["pairs"] == 50_000


def _pod_memory_limit() -> int:
    """The service pod's memory limit in bytes, as k8s/deployment.yaml sets it."""
    manifest = (REPO / "k8s" / "deployment.yaml").read_text()
    [(amount, unit)] = re.findall(r"limits:\n(?:\s+\S.*\n)*?\s+memory: (\d+)(Mi|Gi)\n", manifest)
    return int(amount) * {"Mi": 1024**2, "Gi": 1024**3}[unit]


def test_parsing_the_largest_freshness_body_takes_at_most_half_the_pods_memory():
    # FastAPI parses a whole body before the cap on outcomes applies, so only the byte limit
    # bounds what a push takes. Empty objects take the most for their size: each 3 bytes become a
    # dict of 64 bytes and its place in a list. The other half of the pod holds the service.
    count = 1024 * 1024 // 3
    body = b'{"minute": "2026-01-01T00:00:00Z", "outcomes": [' + b"{}," * count + b"{}]}"
    tracemalloc.start()
    try:
        response = _post("/freshness", body, AUTH)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert response.status_code == 422, response.text
    at_the_limit = peak * MAX_FRESHNESS_BODY_BYTES // len(body)
    assert at_the_limit <= _pod_memory_limit() // 2, f"{at_the_limit:,} bytes at the limit"


def test_a_real_score_request_is_answered():
    iisa_http_endpoints._state.load_scores_from_records(
        [_score_record(n, _chains(50)) for n in range(3)]
    )

    response = _post("/get-score", json.dumps({"indexer_id": f"0x{1:040x}"}), AUTH)

    assert response.status_code == 200, response.text
    assert response.json()["found"] is True
