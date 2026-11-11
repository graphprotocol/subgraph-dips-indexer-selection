"""What a sync claim is worth in selection, and so why the fetcher checks it.

Selection prefers an indexer listed as synced on the deployment: the first gets in at any score.
The sync-status fetcher now pushes a claim only where the indexer has an active allocation on
that deployment. That check lives in the fetcher, which the cronjob tests cover; tests here may
not load the fetcher, so these run the real selection on the 2 snapshots it can push.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader
from iisa.iisa_http_endpoints import app

CHAIN = "arbitrum-one"
DEPLOYMENT = "QmDeployment"
STRONG = ["0x" + digit * 40 for digit in "1234"]
WEAK = "0x" + "9" * 40


def _score_record(indexer: str, quality: float) -> dict:
    """1 indexer's scores in the shape the daily score job pushes, better as quality rises."""
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_std_error": 0.002,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": quality,
        "uptime_score": quality,
        "observed_duration_seconds": 2_419_200.0,
        "uptime_duration_seconds": 2_395_000.0 * quality,
        "success_rate": quality,
        "stake_to_fees": 1_500.0,
        "total_query_fees": 2_000.0,
        "last_known_slashable_stake": 3_000_000.0,
        "norm_uptime_score": quality,
        "norm_success_rate": quality,
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
def fresh_state(monkeypatch, tmp_path):
    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    iisa_http_endpoints.get_settings.cache_clear()
    yield
    iisa_http_endpoints._state = iisa_http_endpoints.IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _select_1(synced_on_deployment: list[str]) -> str:
    """Load 4 strong indexers and 1 weak one, the given sync snapshot, and select 1."""
    state = iisa_http_endpoints.IISAState()
    state.initialize(iisa_http_endpoints.get_settings())
    state.load_scores_from_records(
        [_score_record(indexer, 0.95) for indexer in STRONG] + [_score_record(WEAK, 0.2)]
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    state.load_sync_status_from_dict(
        {
            indexer: {"deployments": [DEPLOYMENT], "fetched_at": fetched_at}
            for indexer in synced_on_deployment
        }
    )
    iisa_http_endpoints._state = state

    response = TestClient(app).post(
        "/select-indexers",
        json={"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1},
    )
    assert response.status_code == 200, response.text
    [selected] = response.json()["indexers"]
    return selected["id"]


def test_a_weak_indexer_listed_as_synced_is_picked_over_strong_ones():
    # The snapshot the fetcher pushed before it checked allocations: the claim alone counted.
    assert _select_1(synced_on_deployment=[WEAK]) == WEAK


def test_the_same_weak_indexer_without_a_backed_claim_is_not_picked():
    # The snapshot the fetcher pushes now that the weak indexer has no allocation there.
    assert _select_1(synced_on_deployment=[]) in STRONG
