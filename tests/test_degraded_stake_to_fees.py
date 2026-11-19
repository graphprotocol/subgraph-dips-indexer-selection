"""On a degraded day no stake was read, so stake-to-fees must not tell indexers apart.

When the score job can measure nothing and has nothing recent to carry over, it pushes the
fallback scores: the same quality for every indexer, a neutral stake-to-fees score of 0.5, and
stake and fees missing, or 0 from older jobs. Selection adds dipper's expected DIPs fees to the
query fees before it works out stake-to-fees, and doing that with stake nobody read would put
every indexer that holds an agreement at the bottom on the heaviest part of value. So it must
keep the neutral score.
"""

from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints as endpoints
from iisa import indexer_selection, score_loader
from iisa.iisa_http_endpoints import IISAState, SelectionRequest, Settings, _select_with_processor
from iisa.indexer_selection import NO_STAKE_NOTE_MINUTES, IndexerSelector
from iisa.log_limit import LogLimit

PUSH_TOKEN = "degraded-stake-to-fees-push-token"
AUTH = {"Authorization": f"Bearer {PUSH_TOKEN}"}
CHAIN = "arbitrum-one"
A, B, C, D = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4"))
INDEXERS = [A, B, C, D]


@pytest.fixture(autouse=True)
def fresh_no_stake_notes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts at a fixed minute with its own limit on the note that the expected DIPs
    fees were left out, as the limit is shared by the whole process.
    """
    monkeypatch.setattr(indexer_selection, "_NO_STAKE_NOTES", LogLimit(NO_STAKE_NOTE_MINUTES))
    monkeypatch.setattr(indexer_selection, "_current_minute", lambda: 1_000)


def _fallback_record(indexer: str, unread: Optional[float]) -> dict[str, Any]:
    """1 indexer as the score job's fallback pushes it, which the job's own tests check for the
    stake columns, with unread for its stake and fees. Every indexer has the same price, so
    nothing should set 1 apart from another.
    """
    return {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.0,
        "lat_coefficient_std_error": 0.0,
        "lat_coefficient_upper_bound": 0.0,
        "lat_normalized_score": 0.5,
        "latency_ms": None,
        "uptime_score": 1.0,
        "observed_duration_seconds": None,
        "uptime_duration_seconds": None,
        "success_rate": 1.0,
        "uptime_by_chain": None,
        "success_rate_by_chain": None,
        "queries_by_chain": None,
        "stake_to_fees": None,
        "norm_stake_to_fees": 0.5,
        "org": None,
        "dst_lat": None,
        "dst_lon": None,
        "total_query_fees": unread,
        "last_known_slashable_stake": unread,
        "stake_read_at": None,
        "quality_measured_at": None,
        "computed_at": "2026-11-19T09:00:00Z",
        "query_count": 0,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": 100}}',
        "dips_min_grt_per_billion_entities_per_30_days": None,
        "dips_supported_networks": f'["{CHAIN}"]',
    }


@pytest.fixture(params=[None, 0.0], ids=["stake-missing", "stake-0-as-older-jobs-send"])
def service(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> SimpleNamespace:
    """A fresh service holding the fallback scores, pushed to it as the score job pushes them,
    and a way to restart it so it reads them back from its cache file.
    """
    monkeypatch.setenv("IISA_PUSH_TOKEN", PUSH_TOKEN)
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    endpoints.get_settings.cache_clear()

    def start() -> None:
        state = IISAState()
        state.initialize(Settings())
        state.refresh_data()
        monkeypatch.setattr(endpoints, "_state", state)

    start()
    client = TestClient(endpoints.app, raise_server_exceptions=False)
    records = [_fallback_record(indexer, request.param) for indexer in INDEXERS]
    pushed = client.post("/scores", json=records, headers=AUTH)
    assert pushed.status_code == 200, pushed.text
    yield SimpleNamespace(restart=start)
    endpoints.get_settings.cache_clear()


def _scored(fees: Optional[dict[str, float]]) -> pd.DataFrame:
    """Run /select-indexers selection on the scores the service holds and return the
    selector's scored rows, indexed by indexer.
    """
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    request = SelectionRequest(
        deployment_id="QmDegradedStakeToFees",
        chain_id=CHAIN,
        num_candidates=2,
        optimistic_dips_fees=fees,
    )
    with patch.object(endpoints, "IndexerSelector", side_effect=build):
        _select_with_processor(request)
    return built[0].data.set_index("indexer").loc[INDEXERS]


@pytest.mark.parametrize("restart", [False, True], ids=["as-pushed", "after-a-restart"])
@pytest.mark.parametrize(
    "fees", [None, {A: 90.0, B: 30.0}], ids=["no-dips-fees", "dips-fees-for-a-and-b"]
)
def test_every_indexer_keeps_the_neutral_stake_to_fees_score(service, restart, fees):
    if restart:
        service.restart()

    scored = _scored(fees)

    assert scored["norm_stake_to_fees"].tolist() == [0.5] * len(INDEXERS)
    assert scored["weighted_score"].nunique() == 1


def test_the_note_that_dips_fees_were_left_out_is_logged_at_most_once_every_10_minutes(
    service, monkeypatch, caplog
):
    with caplog.at_level("INFO", logger="iisa.indexer_selection"):
        for minute in (1_000, 1_000, 1_009, 1_010):
            monkeypatch.setattr(indexer_selection, "_current_minute", lambda m=minute: m)
            _scored({A: 90.0})

    notes = [r.getMessage() for r in caplog.records if "fees out" in r.getMessage()]
    assert len(notes) == 2
    assert "0 held back" in notes[0] and "2 held back" in notes[1]
