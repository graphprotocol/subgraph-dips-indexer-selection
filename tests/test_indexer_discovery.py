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


@pytest.mark.parametrize(
    "url, expected",
    [("", "is required"), ("http://graph-node:8000/x\n", "contains whitespace")],
    ids=["unset", "trailing_newline"],
)
def test_unusable_url_raises_without_querying(url, expected):
    """An empty answer would score every indexer as failing its DIPs probe."""
    with patch("processing.paginate_subgraph_query") as mock_paginate:
        with pytest.raises(ValueError, match=f"GRAPH_NETWORK_SUBGRAPH_URL {expected}"):
            discover_indexers_from_network_subgraph(url)
    mock_paginate.assert_not_called()


def test_job_with_no_subgraph_url_pushes_nothing(monkeypatch):
    provider = MagicMock()
    monkeypatch.setattr(main, "RedpandaProvider", lambda: provider)
    monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
    monkeypatch.setattr(main, "GRAPH_NETWORK_SUBGRAPH_URL", "")

    assert main.run_scoring() is False
    provider.fetch_initial_query_results.assert_not_called()
    provider.write_scores.assert_not_called()


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


def _graphql_error(message):
    response = MagicMock()
    response.json.return_value = {"errors": [{"message": message}]}
    return response


@pytest.mark.parametrize(
    "message",
    [
        "bad indexers: {0x1234: Unavailable(no status: indexer not available)}",
        "no indexers found",
        "internal error: database timeout",
    ],
)
def test_retries_gateway_errors_that_can_clear(no_retry_wait, message):
    """The gateway reports an indexer that failed to serve the query as a GraphQL error."""
    with patch("subgraph.requests.post", return_value=_graphql_error(message)) as post:
        with pytest.raises(RuntimeError, match="GraphQL errors"):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 5


@pytest.mark.parametrize(
    "message",
    [
        "auth error: API key not found",
        "auth error: missing authorization header",
        "subgraph not found: no allocations",
        "bad query: failed to parse",
    ],
)
def test_does_not_retry_gateway_errors_that_cannot_clear(no_retry_wait, message):
    """The gateway sends these with HTTP 200, so they arrive as GraphQL errors, not a 401."""
    with patch("subgraph.requests.post", return_value=_graphql_error(message)) as post:
        with pytest.raises(RuntimeError, match="GraphQL errors"):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 1


def _http_error(status):
    response = MagicMock()
    response.status_code = status
    response.raise_for_status.side_effect = requests.HTTPError(f"{status} Error", response=response)
    return response


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_retries_rate_limits_and_server_errors(no_retry_wait, status):
    page = _page([{"id": "0xaaa", "url": "https://indexer-a.example.com"}])

    with patch("subgraph.requests.post", side_effect=[_http_error(status), page]) as post:
        result = discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 2
    assert result == {"0xaaa": "https://indexer-a.example.com"}


def _unreadable_reply():
    response = MagicMock()
    response.json.side_effect = requests.exceptions.JSONDecodeError(
        "Expecting value", "<html>maintenance</html>", 0
    )
    return response


@pytest.mark.parametrize(
    "first_reply",
    [
        requests.exceptions.ChunkedEncodingError("Connection broken: IncompleteRead"),
        requests.exceptions.ContentDecodingError("Received response with content-encoding: gzip"),
        ConnectionResetError("Connection reset by peer"),
        _unreadable_reply(),
    ],
    ids=["dropped_mid_reply", "corrupt_encoding", "socket_reset", "not_json"],
)
def test_retries_replies_cut_off_or_unreadable(no_retry_wait, first_reply):
    page = _page([{"id": "0xaaa", "url": "https://indexer-a.example.com"}])

    with patch("subgraph.requests.post", side_effect=[first_reply, page]) as post:
        result = discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 2
    assert result == {"0xaaa": "https://indexer-a.example.com"}


@pytest.mark.parametrize(
    "post_kwargs",
    [
        {"return_value": _http_error(401)},
        {"return_value": _http_error(403)},
        {"return_value": _http_error(404)},
        {"side_effect": requests.exceptions.MissingSchema("No scheme supplied")},
        {"side_effect": requests.exceptions.InvalidURL("Invalid URL")},
    ],
    ids=["401_rejected_key", "403_forbidden", "404_wrong_url", "missing_scheme", "invalid_url"],
)
def test_does_not_retry_errors_that_cannot_clear(no_retry_wait, post_kwargs):
    with patch("subgraph.requests.post", **post_kwargs) as post:
        with pytest.raises(requests.RequestException):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 1


def test_retries_a_reply_without_data(no_retry_wait):
    empty_reply = MagicMock()
    empty_reply.json.return_value = {"data": None}
    page = _page([{"id": "0xaaa", "url": "https://indexer-a.example.com"}])

    with patch("subgraph.requests.post", side_effect=[empty_reply, page]) as post:
        result = discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 2
    assert result == {"0xaaa": "https://indexer-a.example.com"}


def test_raises_when_no_indexer_has_a_url(no_retry_wait):
    page = _page([{"id": "0xaaa", "url": ""}])

    with patch("subgraph.requests.post", return_value=page) as post:
        with pytest.raises(RuntimeError, match="no indexers with a URL"):
            discover_indexers_from_network_subgraph(SUBGRAPH_URL)

    assert post.call_count == 5


def _run_job(monkeypatch, provider, post_side_effect):
    monkeypatch.setattr(main, "RedpandaProvider", lambda: provider)
    monkeypatch.setattr(main, "validate_geoip_databases", lambda: True)
    monkeypatch.setattr(main, "GRAPH_NETWORK_SUBGRAPH_URL", SUBGRAPH_URL)
    with patch("subgraph.requests.post", side_effect=post_side_effect) as post:
        return main.run_scoring(), post


def test_failed_read_stops_the_job_before_the_replay(monkeypatch, no_retry_wait):
    """The service keeps its last scores, and the costly replay never starts.

    The run reads the subgraph once, so recovering after the 5th attempt changes nothing.
    """
    provider = MagicMock()
    recovered = _page([{"id": "0xaaa", "url": "https://indexer-a.example.com"}])

    result, post = _run_job(
        monkeypatch, provider, [requests.ConnectionError("refused")] * 5 + [recovered]
    )

    assert result is False
    assert post.call_count == 5
    provider.fetch_initial_query_results.assert_not_called()
    provider.write_scores.assert_not_called()


def test_both_pipelines_use_the_urls_read_up_front(monkeypatch):
    urls = {"0xaaa": "https://indexer-a.example.com"}
    reads, full_kwargs, degraded_urls = [], {}, []
    degraded_df = pd.DataFrame({"indexer": ["0xaaa"], "scoring_mode": ["degraded"]})

    def full_pipeline(**kwargs):
        full_kwargs.update(kwargs)
        raise RuntimeError("no rows remain after iterative filtering")

    def degraded(indexer_urls):
        degraded_urls.append(indexer_urls)
        return degraded_df

    monkeypatch.setattr(
        main, "discover_indexers_from_network_subgraph", lambda u: reads.append(u) or urls
    )
    monkeypatch.setattr(main, "compute_all_scores", full_pipeline)
    monkeypatch.setattr(main, "compute_degraded_scores", degraded)
    provider = MagicMock()

    result, _post = _run_job(monkeypatch, provider, AssertionError("no other subgraph read"))

    assert result is True
    assert reads == [SUBGRAPH_URL]
    assert full_kwargs["indexer_urls"] == urls
    assert degraded_urls == [urls]
    provider.write_scores.assert_called_once_with(degraded_df)


def test_full_pipeline_probes_the_urls_it_is_given(monkeypatch):
    indexer_ids = ["0x" + c * 40 for c in "abc"]
    urls = {indexer: f"http://{indexer[2:8]}-svc:7601" for indexer in indexer_ids}
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
                "url": urls[indexer],
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
    geoip = pd.DataFrame(
        {
            "indexer": indexer_ids,
            "url": list(urls.values()),
            "ip_addr": ["172.18.0.5", "172.18.0.6", "172.18.0.7"],
            "org": [None] * 3,
            "dst_country": [None] * 3,
            "dst_lat": [float("nan")] * 3,
            "dst_lon": [float("nan")] * 3,
            "indexer_network": ["arbitrum"] * 3,
        }
    )
    probed = []

    def fetch_dips_info(indexer_urls):
        probed.append(indexer_urls)
        return pd.DataFrame(
            {
                "indexer": indexer_ids,
                "dips_info_available": [True] * 3,
                "dips_min_grt_per_30_days": ['{"arbitrum-one": 100}'] * 3,
                "dips_min_grt_per_billion_entities_per_30_days": [None] * 3,
                "dips_supported_networks": ['["arbitrum-one"]'] * 3,
            }
        )

    monkeypatch.setattr(processing, "resolve_indexer_geoip", lambda _queries: geoip)
    monkeypatch.setattr(processing, "fetch_dips_info", fetch_dips_info)
    monkeypatch.setattr(processing, "fetch_and_filter_graph_node_versions", lambda df, _u: df)

    with patch("subgraph.requests.post", side_effect=AssertionError("must not read the subgraph")):
        scores = processing.compute_all_scores(
            provider=provider,
            indexer_urls=urls,
            start_date=pd.Timestamp("2026-05-01").date(),
            num_days=28,
            target_rows=20_000_000,
            geoip_available=True,
        )

    assert probed == [urls]
    assert scores["dips_info_available"].all()
