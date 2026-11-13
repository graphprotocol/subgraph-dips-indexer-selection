"""Sync status stops counting once it is 6 hours old, whether or not a newer push arrives.

The service judged an entry's age only when a snapshot was built, on a push or at startup, so
when pushes stopped it went on preferring the indexers in its last snapshot for as long as the
pod ran. These tests move the service's clock forward instead of waiting.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints, score_loader, sync_status_loader
from iisa.iisa_http_endpoints import app
from iisa.sync_status_loader import SyncStatusData

DEPLOYMENT = "QmDeployment"


def _snapshot(fetched_at: datetime) -> dict:
    return {"0xAAA": {"deployments": [DEPLOYMENT, "QmOther"], "fetched_at": fetched_at.isoformat()}}


def _clock_moved_to(monkeypatch, moment: datetime) -> None:
    # raising=False so that, before the clock existed, this ran and failed on what it checks.
    monkeypatch.setattr(sync_status_loader, "_utc_now", lambda: moment, raising=False)


def test_a_fresh_snapshot_counts_until_it_is_6_hours_old(monkeypatch):
    fetched_at = datetime.now(timezone.utc)
    data = SyncStatusData(_snapshot(fetched_at))

    _clock_moved_to(monkeypatch, fetched_at + timedelta(hours=5, minutes=59))
    assert data.synced_indexers_for(DEPLOYMENT) == {"0xaaa"}

    _clock_moved_to(monkeypatch, fetched_at + timedelta(hours=6, minutes=1))
    assert data.synced_indexers_for(DEPLOYMENT) == set()


def test_the_counts_leave_out_entries_too_old_to_count(monkeypatch):
    fetched_at = datetime.now(timezone.utc)
    data = SyncStatusData(_snapshot(fetched_at))
    assert (data.total_indexers, data.total_deployments) == (1, 2)

    _clock_moved_to(monkeypatch, fetched_at + timedelta(hours=7))

    assert (data.total_indexers, data.total_deployments) == (0, 0)


def test_each_entry_ages_on_its_own(monkeypatch):
    now = datetime.now(timezone.utc)
    raw = {
        "0xOLD": {
            "deployments": [DEPLOYMENT],
            "fetched_at": (now - timedelta(hours=5)).isoformat(),
        },
        "0xNEW": {"deployments": [DEPLOYMENT], "fetched_at": now.isoformat()},
    }
    data = SyncStatusData(raw)

    _clock_moved_to(monkeypatch, now + timedelta(hours=2))

    assert data.synced_indexers_for(DEPLOYMENT) == {"0xnew"}


# --- Through the service --------------------------------------------------------------------

CHAIN = "arbitrum-one"
STRONG = ["0x" + digit * 40 for digit in "1234"]
WEAK = "0x" + "9" * 40
# Well below the strong indexers, yet its total stays above 15% of theirs, the cut-off under
# which a group member is replaced in the same selection by a much better candidate.
WEAK_QUALITY = 0.8


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


@pytest.fixture
def weak_indexer_synced(monkeypatch, tmp_path):
    """4 strong indexers and 1 weak one, with the weak one pushed as synced just now."""
    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    iisa_http_endpoints.get_settings.cache_clear()
    state = iisa_http_endpoints.IISAState()
    state.initialize(iisa_http_endpoints.get_settings())
    state.load_scores_from_records(
        [_score_record(indexer, 0.95) for indexer in STRONG] + [_score_record(WEAK, WEAK_QUALITY)]
    )
    pushed_at = datetime.now(timezone.utc)
    state.load_sync_status_from_dict(
        {WEAK: {"deployments": [DEPLOYMENT], "fetched_at": pushed_at.isoformat()}}
    )
    iisa_http_endpoints._state = state
    yield pushed_at
    iisa_http_endpoints._state = iisa_http_endpoints.IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


def _select_1() -> str:
    response = TestClient(app).post(
        "/select-indexers",
        json={"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1},
    )
    assert response.status_code == 200, response.text
    [selected] = response.json()["indexers"]
    return selected["id"]


def test_a_fresh_claim_still_earns_the_preference(weak_indexer_synced):
    assert _select_1() == WEAK


def test_a_claim_6_hours_old_no_longer_earns_the_preference(weak_indexer_synced, monkeypatch):
    _clock_moved_to(monkeypatch, weak_indexer_synced + timedelta(hours=7))

    assert _select_1() in STRONG


def test_health_stops_reporting_sync_status_once_none_of_it_counts(
    weak_indexer_synced, monkeypatch
):
    client = TestClient(app)
    assert client.get("/health").json()["sync_status_loaded"] is True

    _clock_moved_to(monkeypatch, weak_indexer_synced + timedelta(hours=7))

    assert client.get("/health").json()["sync_status_loaded"] is False
