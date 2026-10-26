"""Reading indexer URLs from the network subgraph.

Scoring without those URLs marks every indexer as failing its DIPs probe, so selection
would return no one and dipper would cancel every agreement. A failed read must therefore
stop the job, leaving the service on the last scores it was sent.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import main  # noqa: E402
import processing  # noqa: E402
from processing import discover_indexers_from_network_subgraph  # noqa: E402

SUBGRAPH_URL = "https://subgraph.example.com"


@pytest.fixture
def no_retry_wait():
    """Skip the retry backoff, which otherwise sleeps for about 30s."""
    with patch.object(processing._query_indexer_urls.retry, "sleep", lambda _: None):
        yield


def _page(indexers):
    response = MagicMock()
    response.json.return_value = {"data": {"indexers": indexers}}
    return response


@patch("processing.paginate_subgraph_query")
def test_returns_id_to_url_mapping(mock_paginate):
    mock_paginate.return_value = [
        {"id": "0xaaa", "url": "https://indexer-a.example.com"},
        {"id": "0xbbb", "url": "https://indexer-b.example.com"},
    ]

    result = discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert result == {
        "0xaaa": "https://indexer-a.example.com",
        "0xbbb": "https://indexer-b.example.com",
    }
    mock_paginate.assert_called_once()


@patch("processing.paginate_subgraph_query")
def test_skips_indexers_without_a_url(mock_paginate):
    mock_paginate.return_value = [
        {"id": "0xaaa", "url": "https://indexer-a.example.com"},
        {"id": "0xbbb", "url": ""},
        {"id": "0xccc"},
    ]

    result = discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert result == {"0xaaa": "https://indexer-a.example.com"}


def test_unset_url_returns_empty_without_querying():
    with patch("processing.paginate_subgraph_query") as mock_paginate:
        assert discover_indexers_from_network_subgraph("") == {}
    mock_paginate.assert_not_called()


def test_retries_a_failed_request(no_retry_wait):
    page = _page([{"id": "0xaaa", "url": "https://indexer-a.example.com"}])

    with patch(
        "subgraph.requests.post", side_effect=[requests.ConnectionError("reset"), page]
    ) as post:
        result = discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 2
    assert result == {"0xaaa": "https://indexer-a.example.com"}


def test_raises_when_every_attempt_fails(no_retry_wait):
    with patch("subgraph.requests.post", side_effect=requests.Timeout("timed out")) as post:
        with pytest.raises(requests.Timeout):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 5


def test_raises_on_graphql_errors(no_retry_wait):
    response = MagicMock()
    response.json.return_value = {"errors": [{"message": "indexer unavailable"}]}

    with patch("subgraph.requests.post", return_value=response):
        with pytest.raises(RuntimeError, match="GraphQL errors"):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)


def test_raises_when_no_indexer_has_a_url(no_retry_wait):
    page = _page([{"id": "0xaaa", "url": ""}])

    with patch("subgraph.requests.post", return_value=page) as post:
        with pytest.raises(RuntimeError, match="no indexers with a URL"):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 5


def test_degraded_scoring_raises_instead_of_scoring_without_urls(no_retry_wait):
    with patch("subgraph.requests.post", side_effect=requests.ConnectionError("refused")):
        with pytest.raises(requests.ConnectionError):
            processing.compute_degraded_scores(SUBGRAPH_URL)


def test_full_pipeline_raises_instead_of_scoring_without_urls(monkeypatch, no_retry_wait):
    indexer_ids = ["0x" + c * 40 for c in "abc"]
    queries = pd.DataFrame(
        [
            {
                "query_id": f"q-{indexer[2:8]}-{i:03d}-LAX",
                "deployment_hash": "QmTestDeployment",
                "fee": 1.0,
                "timestamp": pd.Timestamp("2026-05-01") + pd.Timedelta(minutes=i),
                "blocks_behind": 0,
                "response_time_ms": 100.0,
                "indexer": indexer,
                "status": 200,
                "day_partition": "2026-05-01",
                "subgraph_network": "arbitrum",
                "url": f"http://{indexer[2:8]}-svc:7601",
            }
            for indexer in indexer_ids
            for i in range(20)
        ]
    )
    provider = MagicMock()
    provider.fetch_initial_query_results.return_value = pd.DataFrame(
        {"deployment_hash": ["QmTestDeployment"] * 3, "indexer": indexer_ids, "num_rows": [20] * 3}
    )
    provider.fetch_combined_query_results.return_value = queries
    provider.fetch_stake_to_fees.return_value = pd.DataFrame(
        {
            "indexer": indexer_ids,
            "stake_to_fees": [1.0, 2.0, 3.0],
            "total_query_fees": [10.0, 20.0, 30.0],
            "last_known_slashable_stake": [100.0, 200.0, 300.0],
        }
    )
    provider.graph_network_url = SUBGRAPH_URL
    geoip = pd.DataFrame(
        {
            "indexer": indexer_ids,
            "url": [f"http://{i[2:8]}-svc:7601" for i in indexer_ids],
            "ip_addr": ["172.18.0.5", "172.18.0.6", "172.18.0.7"],
            "org": [None] * 3,
            "dst_country": [None] * 3,
            "dst_lat": [float("nan")] * 3,
            "dst_lon": [float("nan")] * 3,
            "indexer_network": ["arbitrum"] * 3,
        }
    )
    monkeypatch.setattr(processing, "resolve_indexer_geoip", lambda _queries: geoip)

    with patch("subgraph.requests.post", side_effect=requests.ConnectionError("refused")):
        with pytest.raises(requests.ConnectionError):
            processing.compute_all_scores(
                provider=provider,
                start_date=pd.Timestamp("2026-05-01").date(),
                num_days=28,
                target_rows=20_000_000,
                geoip_available=True,
            )


def test_job_pushes_nothing_while_the_subgraph_stays_down(monkeypatch, no_retry_wait):
    """The service keeps its last scores rather than ones where every indexer fails DIPs.

    The degraded fallback reads the subgraph again; if it has recovered by then, the job
    pushes degraded scores, which carry real URLs and DIPs results.
    """
    mock_provider = MagicMock()

    def full_pipeline(**_kwargs):
        processing.discover_indexers_from_network_subgraph(SUBGRAPH_URL)
        return pd.DataFrame({"indexer": ["0xaaa"]})

    monkeypatch.setattr(main, "RedpandaProvider", lambda: mock_provider)
    monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
    monkeypatch.setattr(main, "compute_all_scores", full_pipeline)
    monkeypatch.setattr(main, "GRAPH_NETWORK_SUBGRAPH_URL", SUBGRAPH_URL)

    with patch("subgraph.requests.post", side_effect=requests.ConnectionError("refused")):
        result = main.run_scoring()

    assert result is False
    mock_provider.write_scores.assert_not_called()
