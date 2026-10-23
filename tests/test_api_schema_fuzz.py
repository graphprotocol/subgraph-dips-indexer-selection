"""Fuzz every HTTP endpoint from the app's own OpenAPI schema.

Schemathesis reads the schema FastAPI publishes, generates requests that fit it and requests
that break it, and sends them to the app in process. A test fails on any 5xx response and on
any answer the schema does not list. Each request meets freshly seeded scores and sync status,
and carries the push token, or in one test any bearer token, so the endpoints do real work.
Since each request starts afresh, a last test pushes unusual scores and then reads them back.
"""

import copy
import json
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
import schemathesis
from fastapi.testclient import TestClient
from hypothesis import settings
from pydantic import ValidationError
from schemathesis.checks import CheckFunction
from schemathesis.specs.openapi.checks import (
    allow_header_conformance,
    negative_data_rejection,
    positive_data_acceptance,
)

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import IISAState, SelectionRequest, Settings

PUSH_TOKEN = "schema-fuzz-push-token"
AUTH_HEADERS = {"Authorization": f"Bearer {PUSH_TOKEN}"}
CHAIN = "arbitrum-one"
OTHER_CHAIN = "mainnet"
DEPLOYMENT = "QmSchemaFuzzDeployment"


def _score_record(indexer: str, **overrides: Any) -> dict[str, Any]:
    """One indexer's scores in the shape the daily score job pushes to POST /scores."""
    record: dict[str, Any] = {
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
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "450", "{OTHER_CHAIN}": "900"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": f'["{CHAIN}", "{OTHER_CHAIN}"]',
        "graph_node_version": "0.36.0",
        "graph_node_commit": "abc1234",
        "computed_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "query_count": 12_000,
    }
    record.update(overrides)
    return record


# 5 indexers that cover each way selection treats one: cheap and fast, expensive, priced
# for the other chain only, without DIPs info, and with no per-entity price or location.
SCORE_RECORDS = [
    _score_record("0x" + "a1" * 20),
    _score_record(
        "0x" + "b2" * 20,
        org="Other Hosting",
        lat_normalized_score=0.4,
        stake_to_fees=800.0,
        dips_min_grt_per_30_days=f'{{"{CHAIN}": "4000"}}',
        dips_min_grt_per_billion_entities_per_30_days="900",
    ),
    _score_record(
        "0x" + "c3" * 20,
        dips_min_grt_per_30_days=f'{{"{OTHER_CHAIN}": "300"}}',
        dips_supported_networks=f'["{OTHER_CHAIN}"]',
    ),
    _score_record(
        "0x" + "d4" * 20,
        dips_info_available=False,
        dips_min_grt_per_30_days="{}",
        dips_min_grt_per_billion_entities_per_30_days=None,
        dips_supported_networks="[]",
    ),
    _score_record(
        "0x" + "e5" * 20,
        org=None,
        dst_lat=None,
        dst_lon=None,
        dips_min_grt_per_billion_entities_per_30_days=None,
    ),
]


def _sync_status_payload() -> dict[str, Any]:
    """2 indexers synced on the test deployment, in the shape POST /sync-status takes."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    return {
        record["indexer"]: {"deployments": [DEPLOYMENT], "fetched_at": fetched_at}
        for record in SCORE_RECORDS[:2]
    }


# The state _seed_state last made, and the scores and sync status it held then.
_seeded: dict[str, Any] = {}


def _seed_state() -> None:
    """Replace the app's state with a fresh one holding the seeded scores and sync status."""
    state = IISAState()
    state.initialize(Settings())
    state.load_scores_from_records(copy.deepcopy(SCORE_RECORDS))
    state.load_sync_status_from_dict(_sync_status_payload())
    iisa_http_endpoints._state = state
    _seeded.update(state=state, history=state.history, sync_status=state.sync_status)


def _start_from_seeded_state() -> None:
    """Seed the app's state unless it still holds the seeded scores and sync status, so a
    request that pushes new ones cannot change what the next sees and each failure replays on
    its own. A push swaps in new objects, so comparing them by identity spots one.
    """
    state = iisa_http_endpoints._state
    if (
        state is not _seeded.get("state")
        or state.history is not _seeded["history"]
        or state.sync_status is not _seeded["sync_status"]
    ):
        _seed_state()


@pytest.fixture(scope="module")
def app_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """The push token and cache paths the app reads, set for the whole module, with the cache
    files in a temporary folder. Puts the app's state back afterwards.
    """
    cache_dir = tmp_path_factory.mktemp("iisa-cache")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("IISA_PUSH_TOKEN", PUSH_TOKEN)
        mp.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
        mp.setenv("IISA_SYNC_STATUS_FILE_PATH", str(cache_dir / "sync_status.json"))
        mp.setattr(score_loader, "SCORES_FILE_PATH", str(cache_dir / "indexer_scores.json"))
        iisa_http_endpoints.get_settings.cache_clear()
        try:
            yield
        finally:
            iisa_http_endpoints._state = IISAState()
            iisa_http_endpoints.get_settings.cache_clear()


@pytest.fixture(scope="module")
def api_schema(app_settings: None) -> schemathesis.BaseSchema:
    """The app's OpenAPI schema, loaded in process. Loading it starts the app's startup code
    once for the whole process, so it runs with the settings above.
    """
    return schemathesis.openapi.from_asgi("/openapi.json", iisa_http_endpoints.app)


@pytest.fixture(scope="module")
def latin1_api_schema(api_schema: schemathesis.BaseSchema) -> schemathesis.BaseSchema:
    """The same schema, drawing text from all of Latin-1, as a header can carry, not just ASCII."""
    config = schemathesis.Config.from_dict({"generation": {"codec": "latin-1"}})
    return schemathesis.openapi.from_asgi("/openapi.json", iisa_http_endpoints.app, config=config)


schema = schemathesis.pytest.from_fixture("api_schema")
latin1_schema = schemathesis.pytest.from_fixture("latin1_api_schema")

# Ids the seeded data knows, swapped in for generated ones so a lookup finds an indexer,
# and selection finds priced candidates and synced indexers.
SEEDED_BODY_IDS = {
    "chain_id": CHAIN,
    "deployment_id": DEPLOYMENT,
    "indexer_id": SCORE_RECORDS[0]["indexer"],
}


def _use_seeded_data(case: schemathesis.Case) -> None:
    """Swap generated text ids for ones the seeded data matches, and give each pushed score
    record the columns a push needs, so requests get past lookups, filters and checks.
    """
    if isinstance(case.body, dict):
        seeded = {
            key: SEEDED_BODY_IDS[key]
            for key, value in case.body.items()
            if key in SEEDED_BODY_IDS and isinstance(value, str)
        }
        case.body = {**case.body, **seeded}
    if case.path == "/scores" and isinstance(case.body, list):
        case.body = [
            {**SCORE_RECORDS[0], **record} if isinstance(record, dict) else record
            for record in case.body
        ]
    if case.query and isinstance(case.query.get("chain"), str):
        case.query = {**case.query, "chain": CHAIN}


def _accepted_by_lenient_number_parsing(case: schemathesis.Case, status_code: int) -> bool:
    """True when a selection request that breaks the schema was accepted only because its
    model, like every pydantic model by default, reads true, false or numeric text as a number.
    """
    if case.path != "/select-indexers" or status_code != 200 or not isinstance(case.body, dict):
        return False
    try:
        SelectionRequest.model_validate_json(json.dumps(case.body), strict=True)
    except ValidationError:
        return True
    return False


def _rejected_by_push_checks(case: schemathesis.Case, response: schemathesis.Response) -> bool:
    """True when a scores push got the 422 that names the score columns it lacks, or the
    records whose indexer is not text.
    """
    if (case.method, case.path, response.status_code) != ("POST", "/scores", 422):
        return False
    return all(
        (error["type"], error["loc"]) == ("missing", ["body"])
        or (error["type"], error["loc"][::2]) == ("string_type", ["body", "indexer"])
        for error in response.json()["detail"]
    )


def _known_gaps(case: schemathesis.Case, response: schemathesis.Response) -> list[CheckFunction]:
    """Checks to skip for the cases that need a decision rather than a fix."""
    excluded: list[CheckFunction] = []
    # Starlette answers OPTIONS /scores with `Allow: POST` alone, since GET and POST are two
    # routes on one path and it stops at the first. Only a custom 405 handler would fix that.
    if case.path == "/scores":
        excluded.append(allow_header_conformance)
    # The push schema lists no required score columns and no type for the indexer, so a
    # schema-valid record can lack one or hold a number there and get a 422. Listing them
    # means settling which value types a push may carry.
    if _rejected_by_push_checks(case, response):
        excluded.append(positive_data_acceptance)
    # Accepting `"num_candidates": true` or `"max_grt_per_30_days": "4500"` follows pydantic's
    # lax mode. Making the request model strict would reject callers that send numbers as text.
    if _accepted_by_lenient_number_parsing(case, response.status_code):
        excluded.append(negative_data_rejection)
    return excluded


def _call_authorised_and_validate(case: schemathesis.Case) -> None:
    """Send the case with the push token, then run every check but the known gaps."""
    response = case.call(headers=AUTH_HEADERS)
    case.validate_response(
        response, excluded_checks=_known_gaps(case, response), headers=AUTH_HEADERS
    )


@schema.parametrize()
def test_every_endpoint_matches_its_schema(case: schemathesis.Case) -> None:
    """Authorised requests, valid and invalid, get no 5xx and only answers the schema lists.
    Each is sent as generated, then with ids and score columns the seeded data matches, so
    requests also do real work. Sending both from one generated case saves generating twice.
    """
    _start_from_seeded_state()
    _call_authorised_and_validate(case)
    _start_from_seeded_state()
    _use_seeded_data(case)
    _call_authorised_and_validate(case)


# /health and /select-indexers take no token. Every other route checks it the same way, so
# a fifth of the profile's examples is enough to cover each one.
@latin1_schema.exclude(path=["/health", "/select-indexers"]).parametrize()
@settings(max_examples=max(10, settings().max_examples // 5))
def test_any_bearer_token_gets_an_answer_the_schema_lists(case: schemathesis.Case) -> None:
    """The generated Authorization text, sent as a bearer token, so most fail the token check."""
    _start_from_seeded_state()
    header = (case.headers or {}).get("authorization")
    if isinstance(header, str):
        case.headers = {**case.headers, "authorization": f"Bearer {header}"}
    response = case.call()
    case.validate_response(response, excluded_checks=_known_gaps(case, response))


# Values the score job never sends. Each case puts one in a column of the first record, or
# takes the column out of every record, and pushes the result.
UNUSUAL_VALUES = [None, 5, "x", {"a": 1}]
ABSENT = "absent"
# The reads that use pushed scores, sent once a push is accepted.
SCORE_READS: list[tuple[str, str, Optional[dict[str, Any]]]] = [
    (
        "POST",
        "/select-indexers",
        {
            "deployment_id": DEPLOYMENT,
            "num_candidates": 3,
            "chain_id": CHAIN,
            "existing_indexers": [SCORE_RECORDS[0]["indexer"]],
            "max_grt_per_30_days": 5_000.0,
            "max_grt_per_billion_entities_per_30_days": 1_000.0,
            "optimistic_dips_fees": {SCORE_RECORDS[1]["indexer"]: 50.0},
        },
    ),
    ("POST", "/get-score", {"indexer_id": SCORE_RECORDS[0]["indexer"]}),
    ("GET", "/scores", None),
    ("GET", "/scores/weighted", None),
    ("GET", f"/dips-indexers?chain={CHAIN}", None),
]
# With DIPs fees in the request, selection recomputes stake to fees from stake and query fees,
# and text, a list or an object in either answers 500. How to score an unreadable stake needs
# a decision (see test_selection_survives_unreadable_stake_or_query_fees), so these are xfail.
UNREADABLE_FEES_OR_STAKE = pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="unreadable stake or query fees need a decision"
)


def _unusual_push_cases() -> Iterator[Any]:
    for column in SCORE_RECORDS[0]:
        yield pytest.param(column, ABSENT, id=f"{column}-absent")
        for value in UNUSUAL_VALUES:
            known_gap = column in ("total_query_fees", "last_known_slashable_stake") and (
                isinstance(value, (str, dict))
            )
            marks = [UNREADABLE_FEES_OR_STAKE] if known_gap else []
            yield pytest.param(column, value, id=f"{column}-{value!r}", marks=marks)


@pytest.fixture(scope="module")
def client(app_settings: None) -> Iterator[TestClient]:
    """A client kept open for the module, which runs the app's startup code once and saves
    starting a thread for every request.
    """
    with TestClient(iisa_http_endpoints.app, raise_server_exceptions=False) as client:
        yield client


@pytest.mark.parametrize(("column", "value"), _unusual_push_cases())
def test_every_accepted_push_can_be_served(client: TestClient, column: str, value: Any) -> None:
    """A push holding a value the score job never sends is refused with a 422 or accepted,
    and once accepted, every read of the scores answers without a 5xx.
    """
    _seed_state()
    records = copy.deepcopy(SCORE_RECORDS)
    for record in records if value == ABSENT else []:
        del record[column]
    if value != ABSENT:
        records[0][column] = value

    push = client.post("/scores", json=records, headers=AUTH_HEADERS)

    assert push.status_code in (200, 422), push.text
    for method, path, body in SCORE_READS if push.status_code == 200 else []:
        response = client.request(method, path, json=body, headers=AUTH_HEADERS)
        assert response.status_code < 500, f"{method} {path}: {response.text}"
