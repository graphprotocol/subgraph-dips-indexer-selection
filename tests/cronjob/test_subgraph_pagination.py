"""Tests for the subgraph pagination helper."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from subgraph import SubgraphResponseError, paginate_subgraph_query  # noqa: E402

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
    """A full page then a short one: all results collected, the short page ends paging."""
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
    first_call_vars = mock_post.call_args_list[0].kwargs["json"]["variables"]
    assert first_call_vars["lastId"] == ""
    assert first_call_vars["first"] == 1000

    second_call_vars = mock_post.call_args_list[1].kwargs["json"]["variables"]
    assert second_call_vars["lastId"] == page1[-1]["id"]


@patch("subgraph.requests.post")
def test_pagination_full_page_then_empty_page(mock_post):
    """Exactly 1,000 entities: a full page, then an empty one asked for after the last id."""
    page1 = [{"id": f"0x{i:04x}"} for i in range(1000)]
    mock_post.side_effect = [
        _mock_response({"data": {"indexers": page1}}),
        _mock_response({"data": {"indexers": []}}),
    ]

    result = paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY, entity="indexers")

    assert result == page1
    assert mock_post.call_count == 2
    second_call_vars = mock_post.call_args_list[1].kwargs["json"]["variables"]
    assert second_call_vars["lastId"] == page1[-1]["id"]


@patch("subgraph.requests.post")
def test_pagination_empty_response(mock_post):
    """Subgraph returns no entities on the first page."""
    mock_post.return_value = _mock_response({"data": {"indexers": []}})

    result = paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY, entity="indexers")

    assert result == []
    assert mock_post.call_count == 1


@pytest.mark.parametrize(
    "message, retryable",
    [("bad indexers: {0x1234: Timeout}", True), ("auth error: API key not found", False)],
    ids=["temporary", "permanent"],
)
@patch("subgraph.requests.post")
def test_pagination_graphql_errors(mock_post, message, retryable):
    """GraphQL errors raise SubgraphResponseError, flagged by whether a retry can clear them."""
    mock_post.return_value = _mock_response({"errors": [{"message": message}]})

    with pytest.raises(SubgraphResponseError, match="GraphQL errors") as excinfo:
        paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY)

    assert excinfo.value.retryable is retryable


@patch("subgraph.requests.post")
def test_pagination_reply_without_data(mock_post):
    """A reply with no data raises rather than reading as an empty page."""
    mock_post.return_value = _mock_response({"data": None})

    with pytest.raises(SubgraphResponseError, match="no data") as excinfo:
        paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY)

    assert excinfo.value.retryable is True


@patch("subgraph.requests.post")
def test_pagination_custom_entity_and_page_size(mock_post):
    """Custom entity name and page_size are respected."""
    page = [{"id": "a"}, {"id": "b"}]
    mock_post.return_value = _mock_response({"data": {"allocations": page}})

    result = paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY, entity="allocations", page_size=50)

    assert result == page
    assert mock_post.call_args.kwargs["json"]["variables"]["first"] == 50


@patch("subgraph.requests.post")
def test_pagination_http_error_propagates(mock_post):
    """HTTP errors from requests propagate to the caller."""
    import requests

    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
    mock_post.return_value = resp

    with pytest.raises(requests.exceptions.HTTPError):
        paginate_subgraph_query(DUMMY_URL, DUMMY_QUERY)
