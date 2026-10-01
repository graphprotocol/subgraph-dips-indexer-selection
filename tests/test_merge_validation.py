"""Merges that look up per-indexer data must fail on a duplicate key instead of copying rows."""

import logging
import sys
from pathlib import Path

import pandas as pd
import pytest

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import processing  # noqa: E402
from processing import (  # noqa: E402
    merge_and_prepare_dataframes,
    merge_in_indexers_info,
    merge_in_query_geolocation_info,
)


def _per_indexer(column: str, indexers: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"indexer": indexers, column: [1.0] * len(indexers)})


def test_indexer_geoip_merge_rejects_duplicate_indexer_url():
    queries = pd.DataFrame({"indexer": ["0xa"], "url": ["https://a/"]})
    indexers = pd.DataFrame(
        {"indexer": ["0xa", "0xa"], "url": ["https://a/", "https://a/"], "dst_lat": [1.0, 2.0]}
    )

    with pytest.raises(pd.errors.MergeError):
        merge_in_indexers_info(queries, indexers)


def test_query_geolocation_keeps_first_row_for_a_repeated_airport_code(monkeypatch, caplog):
    airports = pd.DataFrame(
        {"IATA_code": ["LHR", "LHR"], "latitude": [51.5, 0.0], "longitude": [-0.4, 0.0]}
    )
    monkeypatch.setattr(processing, "load_iata_data", lambda: airports)
    queries = pd.DataFrame({"query_id": ["q1-LHR"]})

    with caplog.at_level(logging.WARNING, logger=processing.logger.name):
        result = merge_in_query_geolocation_info(queries)

    assert len(result) == 1
    assert result["src_lat"].iloc[0] == 51.5
    assert "IATA code(s) more than once" in caplog.text


def test_score_assembly_rejects_duplicate_indexer_in_a_lookup_table():
    indexers = ["0xa", "0xb"]
    uptime = _per_indexer("% up_x", indexers)
    rankings = _per_indexer("Latency Coefficient", indexers)
    success = _per_indexer("average_status", indexers)
    stake = _per_indexer("stake_to_fees", indexers)
    counts = _per_indexer("query_count", indexers)
    agg_with_duplicate = _per_indexer("dst_lat", ["0xa", "0xa", "0xb"])

    with pytest.raises(pd.errors.MergeError):
        merge_and_prepare_dataframes(
            uptime,
            rankings,
            agg_with_duplicate,
            success,
            stake,
            counts,
            drop_missing_latency=False,
        )


def test_dips_info_merge_rejects_duplicate_indexer(monkeypatch):
    dips_info = pd.DataFrame({"indexer": ["0xa", "0xa"], "dips_info_available": [True, False]})
    monkeypatch.setattr(processing, "fetch_dips_info", lambda _urls: dips_info)
    scores = _per_indexer("weighted_score", ["0xa"])

    with pytest.raises(pd.errors.MergeError):
        processing._attach_dips_info(scores, {"0xa": "https://a/"})
