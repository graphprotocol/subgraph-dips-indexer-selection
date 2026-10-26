"""Shared GraphQL pagination helper for querying Graph Network subgraphs.

Both the indexer discovery below and the stake data path (redpanda.py) need
cursor-based pagination over subgraph entities. This module provides a single
implementation so the pagination logic, error handling, and page-size defaults
live in one place. Indexer discovery lives here rather than in processing.py so
the sync-status fetcher can use it without loading the scoring libraries.
"""

import logging
import re
import time
from typing import Dict, List

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Gateway URLs carry the API key in the path (https://gateway.thegraph.com/api/<key>/subgraphs/...),
# so logging the raw URL hands the key to anyone who can read the logs. Match on the route after
# the key rather than the key's format, so this keeps working if the key format changes.
_GATEWAY_API_KEY = re.compile(r"(/api/)[^/?#\s]+(?=/(?:subgraphs|deployments)/)")


def redact_url(text: str) -> str:
    """Return ``text`` with any gateway API key in a URL path replaced by ``<redacted>``."""
    return _GATEWAY_API_KEY.sub(r"\1<redacted>", text)


# The gateway answers a rejected key, an unknown subgraph and an invalid query with HTTP 200 and a
# GraphQL error starting with these (its error type in edgeandnode/gateway); none clears on retry.
_PERMANENT_GATEWAY_ERRORS = ("auth error", "subgraph not found", "bad query")


class SubgraphResponseError(RuntimeError):
    """The subgraph answered, but with GraphQL errors or without the data asked for."""

    def __init__(self, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


def _graphql_errors_are_permanent(errors: object) -> bool:
    """True when any error in a GraphQL ``errors`` list is one of _PERMANENT_GATEWAY_ERRORS."""
    if not isinstance(errors, list):
        return False
    messages = [str(e.get("message", "")) for e in errors if isinstance(e, dict)]
    return any(message.startswith(_PERMANENT_GATEWAY_ERRORS) for message in messages)


# Failures of the connection or the reply rather than of the request: no connection, a timeout,
# a reply cut off or corrupted in transit, or a body that isn't JSON (such as a proxy's error page).
_TRANSIENT_ERRORS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
    requests.exceptions.JSONDecodeError,
    ConnectionError,
    SubgraphResponseError,
)


def is_retryable_subgraph_error(error: BaseException) -> bool:
    """True for failures a retry can clear: _TRANSIENT_ERRORS, a 408, 429 or 5xx reply, or a
    SubgraphResponseError (the gateway reports a failing indexer as a GraphQL error).
    A rejected key, a wrong URL or a malformed one fails the same way every time.
    """
    if isinstance(error, requests.HTTPError):
        status = error.response.status_code if error.response is not None else None
        return status is None or status in (408, 429) or status >= 500
    if isinstance(error, SubgraphResponseError):
        return error.retryable
    return isinstance(error, _TRANSIENT_ERRORS)


# Every network subgraph read retries the same way: up to 5 attempts, 2, 4, 8 then 16 seconds
# apart, and only for the failures is_retryable_subgraph_error says can clear.
retry_subgraph_read = retry(
    retry=retry_if_exception(is_retryable_subgraph_error),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


def paginate_subgraph_query(
    url: str,
    query: str,
    entity: str = "indexers",
    page_size: int = 1000,
) -> List[dict]:
    """Return every ``entity`` across pages, paging on an ``id_gt`` cursor until a short page.

    ``query`` must take ``$first`` (Int!) and ``$lastId`` (String!) and order by ``id``.
    Raises SubgraphResponseError on GraphQL errors; ``requests`` errors keep their type,
    with any API key masked, so callers can choose what to retry.
    """
    last_id = ""
    all_entities: List[dict] = []
    page_num = 0

    logger.info(
        "Paginating subgraph query (entity=%s, page_size=%d) at %s",
        entity,
        page_size,
        redact_url(url),
    )

    while True:
        page_num += 1
        t0 = time.monotonic()
        try:
            response = requests.post(
                url,
                json={"query": query, "variables": {"first": page_size, "lastId": last_id}},
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            # requests writes the full URL into its error messages. Re-raise the same type so
            # callers' retry rules still match, with the key masked; "from None" keeps the
            # original error, and its unmasked message, out of any traceback a caller logs.
            raise type(e)(redact_url(str(e)), response=e.response) from None
        data = response.json()

        if "errors" in data:
            raise SubgraphResponseError(
                f"GraphQL errors: {data['errors']}",
                retryable=not _graphql_errors_are_permanent(data["errors"]),
            )

        payload = data.get("data")
        if not isinstance(payload, dict):
            raise SubgraphResponseError(f"subgraph reply has no data: {data}")
        page = payload.get(entity) or []
        elapsed = time.monotonic() - t0
        logger.info(
            "Fetched page %d: %d entities in %.2fs (cumulative=%d)",
            page_num,
            len(page),
            elapsed,
            len(all_entities) + len(page),
        )

        if not page:
            break

        all_entities.extend(page)
        if len(page) < page_size:
            break
        last_id = page[-1]["id"]

    logger.info("Pagination complete: %d entities across %d page(s)", len(all_entities), page_num)
    return all_entities


INDEXER_URLS_QUERY = """
query($first: Int!, $lastId: String!) {
  indexers(first: $first, where: { id_gt: $lastId, url_not: "" }, orderBy: id) {
    id
    url
  }
}
"""


@retry_subgraph_read
def _query_indexer_urls(network_subgraph_url: str) -> Dict[str, str]:
    """Map each indexer with a URL to that URL; raise when the subgraph lists none."""
    raw_indexers = paginate_subgraph_query(
        network_subgraph_url, INDEXER_URLS_QUERY, entity="indexers"
    )
    indexer_urls = {
        indexer["id"]: indexer["url"] for indexer in raw_indexers if indexer.get("url", "")
    }
    if not indexer_urls:
        # Retried: the gateway may have served the query from an indexer that is behind.
        raise SubgraphResponseError("network subgraph returned no indexers with a URL")
    return indexer_urls


def discover_indexers_from_network_subgraph(network_subgraph_url: str) -> Dict[str, str]:
    """Return {address: url} for every indexer the network subgraph lists with a URL.

    Redpanda only sees indexers the gateway has routed to, so this lookup finds new ones.
    Raises when no URL is set, the subgraph can't be read or it lists no one: without URLs
    every indexer fails its DIPs probe, selection picks no one and dipper cancels everything.
    """
    if not network_subgraph_url:
        raise ValueError("GRAPH_NETWORK_SUBGRAPH_URL is not set")

    all_indexers = _query_indexer_urls(network_subgraph_url)
    logger.info(f"Discovered {len(all_indexers)} indexers from network subgraph")
    return all_indexers
