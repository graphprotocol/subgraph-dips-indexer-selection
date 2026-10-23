"""Tests for subgraph pagination helper and indexer discovery."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from processing import discover_indexers_from_network_subgraph  # noqa: E402
from subgraph import paginate_subgraph_query  # noqa: E402

# ---------------------------------------------------------------------------
# paginate_subgraph_query
# ---------------------------------------------------------------------------

DUMMY_URL = "https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-arbitrum"
DUMMY_QUERY = """
query($first: Int!, $lastId: String!) {
  indexers(first: $first, where: { id_gt: $lastId }, orderBy: id) {
    id
    url
  }
}
"""


def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@patch("subgraph.requests.post")
def test_pagination_two_pages(mock_post):
    """Two full pages followed by an empty page -- all results collected."""
    page1 = [{"id": f"0x{i:04x}"} for i in range(1000)]
    page2 = [{"id": f"0x{i:04x}"} for i in range(1000, 1500)]

    mock_post.side_effect = [
        _mock_response({"data": {"indexers": page1}}),
        _mock_response({"data": {"indexers": page2}}),
    ]

    result = paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY, entity="indexers")

    assert len(result) == 1500
    assert result[:1000] == page1
    assert result[1000:] == page2

    # First call uses empty lastId, second uses the last id from page1
    first_call_vars = mock_post.call_args_list[0].kwargs.get(
        "json", mock_post.call_args_list[0][1].get("json", {})
    )["variables"]
    assert first_call_vars["lastId"] == ""
    assert first_call_vars["first"] == 1000

    second_call_vars = mock_post.call_args_list[1].kwargs.get(
        "json", mock_post.call_args_list[1][1].get("json", {})
    )["variables"]
    assert second_call_vars["lastId"] == page1[-1]["id"]


@patch("subgraph.requests.post")
def test_pagination_empty_response(mock_post):
    """Subgraph returns no entities on the first page."""
    mock_post.return_value = _mock_response({"data": {"indexers": []}})

    result = paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY, entity="indexers")

    assert result == []
    assert mock_post.call_count == 1


@patch("subgraph.requests.post")
def test_pagination_graphql_errors(mock_post):
    """GraphQL errors in the response raise RuntimeError."""
    mock_post.return_value = _mock_response(
        {
            "errors": [{"message": "something went wrong"}],
        }
    )

    with pytest.raises(RuntimeError, match="GraphQL errors"):
        paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY)


@patch("subgraph.requests.post")
def test_pagination_custom_entity_and_page_size(mock_post):
    """Custom entity name and page_size are respected."""
    page = [{"id": "a"}, {"id": "b"}]
    mock_post.return_value = _mock_response({"data": {"allocations": page}})

    result = paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY, entity="allocations", page_size=50)

    assert result == page
    call_vars = mock_post.call_args.kwargs.get("json", mock_post.call_args[1].get("json", {}))[
        "variables"
    ]
    assert call_vars["first"] == 50


@patch("subgraph.requests.post")
def test_pagination_http_error_propagates(mock_post):
    """HTTP errors from requests propagate to the caller."""
    import requests

    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
    mock_post.return_value = resp

    with pytest.raises(requests.exceptions.HTTPError):
        paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY)


# ---------------------------------------------------------------------------
# discover_indexers_from_network_subgraph
# ---------------------------------------------------------------------------


@patch("processing.paginate_subgraph_query")
def test_discover_returns_id_to_url_mapping(mock_paginate):
    """Successful discovery maps indexer id to url."""
    mock_paginate.return_value = [
        {"id": "0xaaa", "url": "https://indexer-a.example.com"},
        {"id": "0xbbb", "url": "https://indexer-b.example.com"},
    ]

    result = discover_indexers_from_network_subgraph("https://subgraph.example.com")

    assert result == {
        "0xaaa": "https://indexer-a.example.com",
        "0xbbb": "https://indexer-b.example.com",
    }
    mock_paginate.assert_called_once()


@patch("processing.paginate_subgraph_query")
def test_discover_skips_empty_urls(mock_paginate):
    """Indexers with empty URLs are excluded from the result."""
    mock_paginate.return_value = [
        {"id": "0xaaa", "url": "https://indexer-a.example.com"},
        {"id": "0xbbb", "url": ""},
        {"id": "0xccc"},
    ]

    result = discover_indexers_from_network_subgraph("https://subgraph.example.com")

    assert result == {"0xaaa": "https://indexer-a.example.com"}


@patch("processing.paginate_subgraph_query")
def test_discover_catches_errors_returns_empty(mock_paginate):
    """Any exception from the helper is caught, returning an empty dict."""
    mock_paginate.side_effect = RuntimeError("GraphQL errors: ...")

    result = discover_indexers_from_network_subgraph("https://subgraph.example.com")

    assert result == {}


def test_discover_empty_url_returns_empty():
    """Empty URL short-circuits without calling the helper."""
    result = discover_indexers_from_network_subgraph("")
    assert result == {}
