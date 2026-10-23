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


def test_job_pushes_nothing_when_indexer_urls_cannot_be_read(monkeypatch, no_retry_wait):
    """The service keeps its last scores rather than ones where every indexer fails DIPs."""
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
