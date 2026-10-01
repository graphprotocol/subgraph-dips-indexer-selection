"""Gateway API keys in the network subgraph URL must never reach the job's logs.

The key sits in the URL path, so these tests plant a fake key in the URL and check every
path that could print it: the progress log lines, the error raised on a failed request,
and the tracebacks the callers log when they catch that error.
"""

import logging
import os
import sys
import traceback
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import processing  # noqa: E402
import subgraph  # noqa: E402
from redpanda import RedpandaProvider  # noqa: E402

FAKE_KEY = "0123456789abcdef0123456789abcdef"
KEYED_URL = f"https://gateway.thegraph.com/api/{FAKE_KEY}/subgraphs/id/QmNetworkSubgraph"
QUERY = "query($first: Int!, $lastId: String!) { indexers(first: $first) { id } }"


def _http_error_response():
    """A response whose raise_for_status fails the way requests does, URL included."""
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError(
        f"404 Client Error: Not Found for url: {KEYED_URL}"
    )
    return response


def _connection_error():
    return requests.ConnectionError(
        "HTTPSConnectionPool(host='gateway.thegraph.com', port=443): Max retries exceeded "
        f"with url: /api/{FAKE_KEY}/subgraphs/id/QmNetworkSubgraph"
    )


class TestRedactUrl:
    @pytest.mark.parametrize(
        "url, expected",
        [
            (
                KEYED_URL,
                "https://gateway.thegraph.com/api/<redacted>/subgraphs/id/QmNetworkSubgraph",
            ),
            (
                f"https://gateway.testnet.thegraph.com/api/{FAKE_KEY}/deployments/id/QmDeployment",
                "https://gateway.testnet.thegraph.com/api/<redacted>/deployments/id/QmDeployment",
            ),
            # Path-only form, as requests writes it into connection errors.
            (
                f"with url: /api/{FAKE_KEY}/subgraphs/id/X",
                "with url: /api/<redacted>/subgraphs/id/X",
            ),
        ],
    )
    def test_masks_key(self, url, expected):
        assert subgraph.redact_url(url) == expected

    @pytest.mark.parametrize(
        "url",
        [
            # Key sent in a header instead: nothing after /api/ is secret.
            "https://gateway.thegraph.com/api/subgraphs/id/QmNetworkSubgraph",
            "http://graph-node:8000/subgraphs/name/graph-network",
            "",
        ],
    )
    def test_leaves_keyless_urls_alone(self, url):
        assert subgraph.redact_url(url) == url


class TestPaginateSubgraphQuery:
    def test_progress_log_hides_key(self, caplog):
        caplog.set_level(logging.INFO, logger="subgraph")
        empty_page = MagicMock()
        empty_page.json.return_value = {"data": {"indexers": []}}

        with patch("subgraph.requests.post", return_value=empty_page):
            subgraph.paginate_subgraph_query(KEYED_URL, QUERY)

        assert "<redacted>" in caplog.text
        assert FAKE_KEY not in caplog.text

    @pytest.mark.parametrize(
        "post_kwargs, expected_type",
        [
            ({"return_value": _http_error_response()}, requests.HTTPError),
            ({"side_effect": _connection_error()}, requests.ConnectionError),
        ],
        ids=["http_error", "connection_error"],
    )
    def test_request_failure_hides_key_and_keeps_error_type(self, post_kwargs, expected_type):
        """Callers pick retries by error type, so masking the key must not change it."""
        with patch("subgraph.requests.post", **post_kwargs):
            with pytest.raises(requests.RequestException) as excinfo:
                subgraph.paginate_subgraph_query(KEYED_URL, QUERY)

        formatted = "".join(traceback.format_exception(excinfo.value))
        assert type(excinfo.value) is expected_type
        assert "<redacted>" in str(excinfo.value)
        assert FAKE_KEY not in formatted


@pytest.fixture
def no_retry_wait():
    """Skip the stake fetch's retry backoff, which otherwise sleeps for about 30s."""
    with patch.object(RedpandaProvider._paginate_graphql_indexers.retry, "sleep", lambda _: None):
        yield


def _provider_with_keyed_url() -> RedpandaProvider:
    with patch.dict(
        os.environ,
        {"REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092", "GRAPH_NETWORK_SUBGRAPH_URL": KEYED_URL},
    ):
        return RedpandaProvider()


class TestCallersHideKey:
    """The 2 production callers catch the error and log it; neither log may carry the key."""

    def test_stake_fetch_still_retries_a_failed_request(self, no_retry_wait):
        page = MagicMock()
        page.json.return_value = {
            "data": {"indexers": [{"id": "0xa", "stakedTokens": "1", "lockedTokens": "0"}]}
        }

        with patch("subgraph.requests.post", side_effect=[_connection_error(), page]) as post:
            indexers = _provider_with_keyed_url()._paginate_graphql_indexers()

        assert post.call_count == 2
        assert [i["id"] for i in indexers] == ["0xa"]

    def test_stake_fetch_logs_hide_key(self, caplog, no_retry_wait):
        caplog.set_level(logging.INFO)
        provider = _provider_with_keyed_url()

        with patch("subgraph.requests.post", side_effect=_connection_error()):
            result = provider.fetch_stake_to_fees()

        assert result.empty
        assert "Failed to fetch stake data" in caplog.text
        assert FAKE_KEY not in caplog.text

    def test_indexer_discovery_logs_hide_key(self, caplog):
        caplog.set_level(logging.INFO)

        with patch("subgraph.requests.post", return_value=_http_error_response()):
            result = processing.discover_indexers_from_network_subgraph(KEYED_URL)

        assert result == {}
        assert "Failed to query network subgraph" in caplog.text
        assert FAKE_KEY not in caplog.text
