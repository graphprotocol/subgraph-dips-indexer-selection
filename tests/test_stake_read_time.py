"""The service keeps the time each indexer's stake was read, and never scores by it.

The score job records when it read stake from the network subgraph, and on a day it cannot
read stake it reuses an earlier reading from the scores the service holds, keeping that time.
The service must accept the field, keep it across a restart and send it back on GET /scores,
and score reused stake exactly as it scored the same values on the day they were read.
"""

import json
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from iisa import DataManager, score_loader
from iisa import iisa_http_endpoints as endpoints
from iisa.iisa_http_endpoints import IISAState, SelectionRequest, Settings, _select_with_processor
from iisa.indexer_selection import IndexerSelector

PUSH_TOKEN = "stake-read-time-push-token"
AUTH = {"Authorization": f"Bearer {PUSH_TOKEN}"}
CHAIN = "arbitrum-one"
A, B, C, D = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4"))
DAY_READ = "2026-11-09T11:30:00Z"
TWO_DAYS_LATER = "2026-11-11T11:30:00Z"


def _record(indexer: str, stake: float, fees: float, read_at: Optional[str], **extra: Any) -> dict:
    """One indexer as the score job pushes it, with its stake read at read_at."""
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "success_rate": 0.98,
        "stake_to_fees": stake / fees if fees else None,
        "total_query_fees": fees,
        "last_known_slashable_stake": stake,
        "stake_read_at": read_at,
        "org": f"org-{indexer[2:4]}",
        "dst_lat": 52.4,
        "dst_lon": 4.9,
        "computed_at": TWO_DAYS_LATER,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "450"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": f'["{CHAIN}"]',
        **extra,
    }


STAKE = {A: (1_000.0, 10.0), B: (2_000.0, 10.0), C: (3_000.0, 0.0), D: (4_000.0, 10.0)}


def _snapshot(read_at: Optional[str]) -> list[dict]:
    return [_record(indexer, *values, read_at) for indexer, values in STAKE.items()]


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch, tmp_path) -> SimpleNamespace:
    """A fresh service with its scores cache in tmp_path, and a way to restart it from there."""
    monkeypatch.setenv("IISA_PUSH_TOKEN", PUSH_TOKEN)
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    endpoints.get_settings.cache_clear()

    def start() -> None:
        state = IISAState()
        state.initialize(Settings())
        state.refresh_data()
        monkeypatch.setattr(endpoints, "_state", state)

    start()
    yield SimpleNamespace(
        client=TestClient(endpoints.app, raise_server_exceptions=False),
        restart=start,
        cache=tmp_path / "indexer_scores.json",
    )
    endpoints.get_settings.cache_clear()


def _served_read_times(client: TestClient) -> dict[str, Any]:
    response = client.get("/scores", headers=AUTH)
    assert response.status_code == 200
    return {row["indexer"]: row["stake_read_at"] for row in response.json()["scores"]}


class TestReadTimeIsKept:
    def test_a_push_with_read_times_is_accepted_and_served_back(self, service):
        records = _snapshot(DAY_READ)
        records[3]["stake_read_at"] = None

        response = service.client.post("/scores", json=records, headers=AUTH)

        assert response.status_code == 200
        assert _served_read_times(service.client) == {
            A: DAY_READ,
            B: DAY_READ,
            C: DAY_READ,
            D: None,
        }

    def test_the_read_times_survive_a_restart(self, service):
        service.client.post("/scores", json=_snapshot(DAY_READ), headers=AUTH)

        service.restart()

        cached = json.loads(service.cache.read_text())
        assert {row["stake_read_at"] for row in cached} == {DAY_READ}
        assert set(_served_read_times(service.client).values()) == {DAY_READ}


def _scored(records: list[dict], fees: Optional[dict[str, float]] = None) -> pd.DataFrame:
    """Load records through the score loader, run /select-indexers selection on them and
    return the selector's scored rows, indexed by indexer.
    """
    history = DataManager(MagicMock()).transform_scores_df(pd.DataFrame(records))
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    request = SelectionRequest(
        deployment_id="QmStakeReadTime", chain_id=CHAIN, num_candidates=2, optimistic_dips_fees=fees
    )
    with (
        patch.object(endpoints, "_state", SimpleNamespace(history=history, sync_status=None)),
        patch.object(endpoints, "IndexerSelector", side_effect=build),
    ):
        _select_with_processor(request)
    return built[0].data.set_index("indexer")


class TestReusedStakeScoresAsOnTheDayItWasRead:
    """A run 2 days later reuses the reading: only computed_at and the quality could differ,
    and here they do not, so every score must match the day the stake was read.
    """

    @pytest.mark.parametrize("fees", [None, {A: 90.0, C: 30.0}], ids=["no-dips-fees", "dips-fees"])
    def test_stake_to_fees_and_the_total_match(self, fees):
        on_the_day = [{**row, "computed_at": DAY_READ} for row in _snapshot(DAY_READ)]
        reused = _snapshot(DAY_READ)
        without_read_times = _snapshot(None)

        columns = ["norm_stake_to_fees", "weighted_score"]
        expected = _scored(on_the_day, fees).loc[[A, B, C, D], columns].to_numpy()
        assert _scored(reused, fees).loc[[A, B, C, D], columns].to_numpy().tolist() == (
            expected.tolist()
        )
        assert _scored(without_read_times, fees).loc[[A, B, C, D], columns].to_numpy().tolist() == (
            expected.tolist()
        )
