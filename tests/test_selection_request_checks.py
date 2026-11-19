"""How strictly POST /select-indexers checks the request it is sent.

Dipper keeps its own copy of the request's fields in step with this service by hand, so a field
it renames must be refused rather than dropped without a word: a dropped blocklist or price
ceiling would select blocked or over-priced indexers. This is also the 1 endpoint that takes no
token, so nothing in a request may be unboundedly large.
"""

import json
import tracemalloc
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import (
    MAX_SELECTION_BODY_BYTES,
    SelectionRequest,
    _BodyLimit,
    app,
)

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


# --- Address case ---------------------------------------------------------------------------


def _capitals(indexer: str) -> str:
    """The address with its hex in capitals, as a checksummed address partly is."""
    return "0x" + indexer[2:].upper()


def _picked(response) -> list[str]:
    assert response.status_code == 200, response.text
    return [indexer["id"] for indexer in response.json()["indexers"]]


@pytest.mark.parametrize(
    "fields",
    [
        {"blocklist": [_capitals(INDEXERS[0])]},
        {"declined_indexers": {"QmDeployment": [_capitals(INDEXERS[0])]}},
        {"pending_agreements": {"QmOther": [_capitals(INDEXERS[0])]}},
    ],
    ids=["blocklist", "declined", "pending"],
)
def test_an_indexer_left_out_in_capitals_is_still_left_out(fields):
    assert INDEXERS[0] not in _picked(_post(_request(num_candidates=3, **fields)))


def test_a_member_named_in_capitals_is_kept_once_in_lowercase():
    picked = _picked(_post(_request(num_candidates=3, existing_indexers=[_capitals(INDEXERS[0])])))

    assert sorted(picked) == sorted(INDEXERS)


def test_expected_dips_fees_named_in_capitals_reach_their_indexer():
    request = SelectionRequest(**_request(optimistic_dips_fees={_capitals(INDEXERS[0]): 50.0}))

    assert request.optimistic_dips_fees == {INDEXERS[0]: 50.0}


def test_an_indexer_pushed_in_capitals_is_matched_by_a_lowercase_request():
    rows = [_capitals(INDEXERS[0]), *INDEXERS[1:]]
    iisa_http_endpoints._state.load_scores_from_records([_score_record(i) for i in rows])

    picked = _picked(_post(_request(num_candidates=3, blocklist=[INDEXERS[0]])))

    assert sorted(picked) == sorted(INDEXERS[1:])


def test_members_named_twice_in_different_case_are_kept_once():
    members = [_capitals(INDEXERS[0]), INDEXERS[0], INDEXERS[1]]

    picked = _picked(_post(_request(num_candidates=3, existing_indexers=members)))

    assert sorted(picked) == sorted(INDEXERS)


def test_expected_dips_fees_named_twice_in_different_case_are_added_together():
    fees = {_capitals(INDEXERS[0]): 10.0, INDEXERS[0]: 20.0}

    request = SelectionRequest(**_request(optimistic_dips_fees=fees))

    assert request.optimistic_dips_fees == {INDEXERS[0]: 30.0}


def test_deployment_ids_keep_their_case():
    """A deployment id is an IPFS hash, in which case matters."""
    request = SelectionRequest(
        **_request(
            deployment_id="QmDeployment",
            pending_agreements={"QmOther": [INDEXERS[0]]},
            declined_indexers={"QmDeployment": [INDEXERS[1]]},
        )
    )

    assert request.deployment_id == "QmDeployment"
    assert list(request.pending_agreements or {}) == ["QmOther"]
    assert list(request.declined_indexers or {}) == ["QmDeployment"]


# --- Sizes ----------------------------------------------------------------------------------


def test_100_candidates_is_accepted():
    assert _post(_request(num_candidates=100)).status_code == 200


def test_more_than_100_candidates_is_refused():
    response = _post(_request(num_candidates=101))

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "num_candidates"]


@pytest.mark.parametrize("count", [0, -1])
def test_no_candidates_still_selects_nobody(count):
    response = _post(_request(num_candidates=count))

    assert response.status_code == 200
    assert response.json()["indexers"] == []


def _members(count):
    return [f"0x{i:040x}" for i in range(count)]


def test_100_members_is_accepted():
    assert _post(_request(existing_indexers=_members(100))).status_code == 200


def test_more_than_100_members_is_refused():
    response = _post(_request(existing_indexers=_members(101)))

    assert response.status_code == 422
    assert [e["loc"] for e in response.json()["detail"]] == [["body", "existing_indexers"]]


@pytest.mark.parametrize(
    ("fields", "location"),
    [
        pytest.param({"deployment_id": "Q" * 129}, ["deployment_id"], id="long-deployment-id"),
        pytest.param({"chain_id": "c" * 65}, ["chain_id"], id="long-chain-id"),
        pytest.param({"blocklist": ["0x"] * 10_001}, ["blocklist"], id="long-blocklist"),
        pytest.param(
            {"existing_indexers": ["0x" + "1" * 127]}, ["existing_indexers", 0], id="long-address"
        ),
        pytest.param(
            {"declined_indexers": {f"Qm{i}": [] for i in range(100_001)}},
            ["declined_indexers"],
            id="too-many-deployments",
        ),
        pytest.param(
            {"pending_agreements": {"QmOther": ["0x"] * 10_001}},
            ["pending_agreements", "QmOther"],
            id="too-many-indexers-on-a-deployment",
        ),
        pytest.param(
            {"optimistic_dips_fees": {f"0x{i}": 1.0 for i in range(10_001)}},
            ["optimistic_dips_fees"],
            id="too-many-fees",
        ),
    ],
)
def test_an_oversized_field_is_refused(fields, location):
    response = _post(_request(**fields))

    assert response.status_code == 422, response.status_code
    assert [e["loc"] for e in response.json()["detail"]] == [["body", *location]]


# --- The size of a refusal ------------------------------------------------------------------
#
# A body well under the size limit could still hold thousands of bad values, and each used to
# be its own error, echoed back in full. Building and sending them all took far more memory and
# time than the body itself, on the same thread that answers every other request.

HOSTILE_BODIES = [
    pytest.param(
        {"pending_agreements": {f"Qm{i}": list(range(1_000)) for i in range(20)}},
        id="many-bad-entries",
    ),
    pytest.param({f"unknown{i}": 0 for i in range(20_000)}, id="many-unknown-fields"),
    pytest.param({"deployment_id": "Q" * 1_000_000}, id="huge-string"),
    pytest.param({"pending_agreements": {"Q" * 1_000_000: []}}, id="huge-key"),
]


@pytest.mark.parametrize("fields", HOSTILE_BODIES)
def test_a_hostile_body_gets_a_short_refusal(fields):
    response = _post(_request(**fields))

    assert response.status_code == 422
    assert len(response.content) < 20_000, f"{len(response.content):,} bytes"
    detail = response.json()["detail"]
    assert 1 <= len(detail) <= 20, f"{len(detail):,} errors"
    assert detail[0]["loc"][0] == "body"


# --- The size of the whole body -------------------------------------------------------------
#
# The checks above run only once the whole body has been read and parsed, which for a huge body
# could use up the service's memory first. So the body itself has a limit.

TOO_LARGE = b'{"padding": "' + b"x" * (4 * 1024 * 1024) + b'"}'


def test_a_body_declared_over_the_limit_is_refused():
    response = TestClient(app, raise_server_exceptions=False).post(
        "/select-indexers", content=TOO_LARGE, headers={"Content-Type": "application/json"}
    )

    assert response.status_code == 413
    assert "4,194,304" in response.json()["detail"]


def test_a_body_with_no_declared_length_is_refused_once_over_the_limit():
    def in_pieces():
        for start in range(0, len(TOO_LARGE), 64 * 1024):
            yield TOO_LARGE[start : start + 64 * 1024]

    response = TestClient(app, raise_server_exceptions=False).post(
        "/select-indexers", content=in_pieces(), headers={"Content-Type": "application/json"}
    )

    assert response.status_code == 413


# The test client hands the app a whole body as 1 piece. A real server hands over a large body
# in many, so these drive the limit directly with a stand-in for the server and the route.

PIECE = 64 * 1024


async def _through_the_limit(pieces, declared_length=None, path="/select-indexers"):
    """Send the pieces through the limit to a stand-in route.

    Returns the answer's status, the body the route read (None if it never ran), and how many
    times the limit or the route asked the server for more of the body.
    """
    messages = [
        {"type": "http.request", "body": piece, "more_body": i < len(pieces) - 1}
        for i, piece in enumerate(pieces)
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
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break
        read_by_route.append(body)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    headers = [(b"content-type", b"application/json")]
    if declared_length is not None:
        headers.append((b"content-length", str(declared_length).encode()))
    scope = {"type": "http", "method": "POST", "path": path, "headers": headers}
    await _BodyLimit(route)(scope, receive, send)
    return sent[0]["status"], (read_by_route[0] if read_by_route else None), asked


def _pieces(total):
    body = bytes(i % 251 for i in range(total))
    return [body[start : start + PIECE] for start in range(0, total, PIECE)]


@pytest.mark.parametrize("declare", [False, True], ids=["no-declared-length", "declared-length"])
async def test_a_body_in_many_pieces_under_the_limit_reaches_the_route_whole(declare):
    pieces = _pieces(MAX_SELECTION_BODY_BYTES - 1_000)
    body = b"".join(pieces)

    status, read, asked = await _through_the_limit(pieces, len(body) if declare else None)

    assert status == 200
    assert read == body
    # Each piece asked for once; the route then reads the whole body in 1 go.
    assert asked == len(pieces)


async def test_a_body_in_many_pieces_over_the_limit_is_refused_at_the_first_piece_past_it():
    pieces = _pieces(MAX_SELECTION_BODY_BYTES + 3 * PIECE)

    status, read, asked = await _through_the_limit(pieces)

    assert status == 413
    assert read is None
    assert asked == MAX_SELECTION_BODY_BYTES // PIECE + 1


async def test_a_body_declared_over_the_limit_is_refused_without_reading_any_of_it():
    pieces = _pieces(MAX_SELECTION_BODY_BYTES + 3 * PIECE)

    status, read, asked = await _through_the_limit(pieces, sum(map(len, pieces)))

    assert status == 413
    assert read is None
    assert asked == 0


async def test_the_pieces_are_let_go_once_joined():
    # Each piece is made only when asked for, so the limit holds the only copy. While the route
    # runs, only the joined body should still be held, not the pieces it was made from too.
    total = MAX_SELECTION_BODY_BYTES - 1_000
    count = -(-total // PIECE)
    sent = 0
    held_while_routing = []

    async def receive():
        nonlocal sent
        size = min(PIECE, total - sent)
        sent += size
        return {"type": "http.request", "body": bytes(size), "more_body": sent < total}

    async def route(scope, receive, send):
        await receive()
        held_while_routing.append(tracemalloc.get_traced_memory()[0] - before)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def send(message):
        pass

    scope = {"type": "http", "method": "POST", "path": "/select-indexers", "headers": []}
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    try:
        await _BodyLimit(route)(scope, receive, send)
    finally:
        tracemalloc.stop()

    assert count > 1
    [held] = held_while_routing
    assert held < 1.5 * total, f"{held:,} bytes held for a {total:,} byte body"


async def test_other_routes_are_not_held_to_the_selection_limit():
    pieces = _pieces(MAX_SELECTION_BODY_BYTES + 3 * PIECE)

    status, read, _ = await _through_the_limit(pieces, sum(map(len, pieces)), path="/scores")

    assert status == 200
    assert read == b"".join(pieces)


def test_the_scores_push_is_not_held_to_the_selection_body_limit():
    # The score job's daily push is legitimately large and carries the push token.
    records = [{"indexer": f"0x{i:040x}", "padding": "x" * 1_000} for i in range(5_000)]
    body = json.dumps(records).encode()
    assert len(body) > 4 * 1024 * 1024

    response = TestClient(app, raise_server_exceptions=False).post(
        "/scores", content=body, headers={"Content-Type": "application/json"}
    )

    # Read and checked in full: refused for lacking score columns, not for its size.
    assert response.status_code == 422, response.status_code
