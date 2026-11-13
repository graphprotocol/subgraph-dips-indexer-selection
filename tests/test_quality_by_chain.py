"""Tests for selection reading each indexer's uptime and success rate on the request's chain.

The daily job pushes, beside each indexer's overall uptime_score and success_rate, the values on
each chain it served as {chain: share} maps. Selection uses the request chain's value when there
is one and the overall value otherwise, including for scores from a job that sends no maps.
"""

import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import IISAState, SelectionRequest, Settings, _select_with_processor
from iisa.indexer_selection import IndexerSelector

CHAIN = "arbitrum-one"
OTHER_CHAIN = "mainnet"
DEPLOYMENT = "QmFakeQualityByChainDeployment"
FIRST, SECOND = ("0x" + pair * 20 for pair in ("a1", "b2"))


def _record(
    indexer: str,
    uptime: float,
    success: float,
    uptime_by_chain: Optional[Any] = None,
    success_by_chain: Optional[Any] = None,
) -> dict[str, Any]:
    """Scores for 1 indexer priced on both chains, with per-chain maps only when given."""
    record = {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_coefficient_upper_bound": 0.01,
        "lat_normalized_score": 0.9,
        "uptime_score": uptime,
        "success_rate": success,
        "stake_to_fees": 3_000.0,
        "total_query_fees": 2_000.0,
        "last_known_slashable_stake": 3_000_000.0,
        "org": f"Org {indexer[2:4]}",
        "dst_lat": 10.0,
        "dst_lon": 10.0,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "100", "{OTHER_CHAIN}": "100"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "100",
        "dips_supported_networks": f'["{CHAIN}", "{OTHER_CHAIN}"]',
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    if uptime_by_chain is not None:
        record["uptime_by_chain"] = uptime_by_chain
    if success_by_chain is not None:
        record["success_rate_by_chain"] = success_by_chain
    return record


@pytest.fixture
def service(tmp_path, monkeypatch) -> Iterator[IISAState]:
    for name in ("IISA_PUSH_TOKEN", "IISA_REQUIRE_PUSH_TOKEN", "IISA_FRESHNESS_EXEMPT_CHAINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(tmp_path / "freshness.json"))
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "scores.json"))
    iisa_http_endpoints.get_settings.cache_clear()
    state = IISAState()
    state.initialize(Settings())
    iisa_http_endpoints._state = state
    yield state
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _shares(chain: str) -> dict[str, tuple[float, float]]:
    """Each indexer's (uptime, success rate) shares as a selection on ``chain`` scores them."""
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    request = SelectionRequest(deployment_id=DEPLOYMENT, chain_id=chain, num_candidates=1)
    with patch.object(iisa_http_endpoints, "IndexerSelector", side_effect=build):
        _select_with_processor(request)
    data = built[0].data.set_index("indexer")
    return {
        indexer: (round(row["% up_x"] / 100, 9), round(row["average_status"], 9))
        for indexer, row in data.iterrows()
    }


def test_the_requests_chain_is_scored_on_its_own_values(service):
    service.load_scores_from_records(
        [
            _record(FIRST, 0.9, 0.8, {CHAIN: 1.0, OTHER_CHAIN: 0.5}, {CHAIN: 0.95}),
            _record(SECOND, 0.7, 0.6, {OTHER_CHAIN: 0.4}, {OTHER_CHAIN: 0.3}),
        ]
    )

    assert _shares(CHAIN) == {FIRST: (1.0, 0.95), SECOND: (0.7, 0.6)}
    assert _shares(OTHER_CHAIN) == {FIRST: (0.5, 0.8), SECOND: (0.4, 0.3)}


def test_scores_from_a_job_without_maps_are_scored_on_the_overall_values(service):
    service.load_scores_from_records([_record(FIRST, 0.9, 0.8), _record(SECOND, 0.7, 0.6)])

    assert _shares(CHAIN) == {FIRST: (0.9, 0.8), SECOND: (0.7, 0.6)}


def test_a_mix_of_rows_with_and_without_maps_is_scored(service):
    service.load_scores_from_records(
        [_record(FIRST, 0.9, 0.8, {CHAIN: 1.0}, {CHAIN: 1.0}), _record(SECOND, 0.7, 0.6)]
    )

    assert _shares(CHAIN) == {FIRST: (1.0, 1.0), SECOND: (0.7, 0.6)}


@pytest.mark.parametrize("value", [1.5, -0.1, "high", True, None, float("nan")])
def test_an_unusable_value_falls_back_to_the_overall_one_with_a_warning(service, caplog, value):
    service.load_scores_from_records(
        [_record(FIRST, 0.9, 0.8, {CHAIN: value}, {CHAIN: 1.0}), _record(SECOND, 0.7, 0.6)]
    )

    with caplog.at_level(logging.WARNING, logger="iisa-service"):
        shares = _shares(CHAIN)

    assert shares[FIRST] == (0.9, 1.0)
    warnings = [r.getMessage() for r in caplog.records if "uptime_by_chain" in r.getMessage()]
    assert warnings and f"chain {CHAIN}" in warnings[0]


@pytest.mark.parametrize("by_chain", ["not a map", 7, ["arbitrum-one"]])
def test_a_map_that_isnt_one_falls_back_to_the_overall_value_with_a_warning(
    service, caplog, by_chain
):
    service.load_scores_from_records(
        [_record(FIRST, 0.9, 0.8, by_chain, by_chain), _record(SECOND, 0.7, 0.6)]
    )

    with caplog.at_level(logging.WARNING, logger="iisa-service"):
        assert _shares(CHAIN)[FIRST] == (0.9, 0.8)

    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("success_rate_by_chain" in message for message in warned)


def test_a_chain_no_indexer_has_values_on_is_noted_at_debug_only(service, caplog):
    """As when the job's chain names don't match the ones selections ask about."""
    service.load_scores_from_records(
        [
            _record(FIRST, 0.9, 0.8, {OTHER_CHAIN: 1.0}, {OTHER_CHAIN: 1.0}),
            _record(SECOND, 0.7, 0.6),
        ]
    )

    with caplog.at_level(logging.DEBUG, logger="iisa-service"):
        assert _shares(CHAIN)[FIRST] == (0.9, 0.8)

    notes = [r for r in caplog.records if f"value on chain {CHAIN}" in r.getMessage()]
    assert [r.levelno for r in notes] == [logging.DEBUG] * 2


def test_post_scores_takes_the_maps_as_they_are_and_serves_them_back(service):
    client = TestClient(iisa_http_endpoints.app)
    records = [
        _record(FIRST, 0.9, 0.8, {CHAIN: 1.0, OTHER_CHAIN: 0.5}, {CHAIN: 0.95}),
        _record(SECOND, 0.7, 0.6),
    ]

    pushed = client.post("/scores", json=records)
    served = {row["indexer"]: row for row in client.get("/scores").json()["scores"]}

    assert pushed.status_code == 200, pushed.text
    assert served[FIRST]["uptime_by_chain"] == {CHAIN: 1.0, OTHER_CHAIN: 0.5}
    assert served[FIRST]["success_rate_by_chain"] == {CHAIN: 0.95}
    assert served[SECOND].get("uptime_by_chain") is None
    assert _shares(CHAIN)[FIRST] == (1.0, 0.95)


def test_each_chains_reported_score_uses_its_values_and_the_network_score_the_overall(service):
    service.load_scores_from_records(
        [_record(FIRST, 0.9, 0.8, {CHAIN: 1.0}, {CHAIN: 1.0}), _record(SECOND, 0.7, 0.6)]
    )

    body = TestClient(iisa_http_endpoints.app).post("/get-score", json={"indexer_id": FIRST}).json()

    assert body["components"]["uptime"] == pytest.approx(0.9**6)
    assert body["chains"][CHAIN]["components"]["uptime"] == pytest.approx(1.0)
    assert body["chains"][OTHER_CHAIN]["components"]["uptime"] == pytest.approx(0.9**6)
