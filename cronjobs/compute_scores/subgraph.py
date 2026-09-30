"""Shared GraphQL pagination helper for querying Graph Network subgraphs.

Both the indexer discovery path (processing.py) and the stake data path
(redpanda.py) need cursor-based pagination over subgraph entities. This
module provides a single implementation so the pagination logic, error
handling, and page-size defaults live in one place.
"""

import logging
import re
import time
from typing import List

import requests

logger = logging.getLogger(__name__)

# Gateway URLs carry the API key in the path (https://gateway.thegraph.com/api/<key>/subgraphs/...),
# so logging the raw URL hands the key to anyone who can read the logs. Match on the route after
# the key rather than the key's format, so this keeps working if the key format changes.
_GATEWAY_API_KEY = re.compile(r"(/api/)[^/?#\s]+(?=/(?:subgraphs|deployments)/)")


def redact_url(text: str) -> str:
    """Return ``text`` with any gateway API key in a URL path replaced by ``<redacted>``."""
    return _GATEWAY_API_KEY.sub(r"\1<redacted>", text)


def paginate_subgraph_query(
    url: str,
    query: str,
    entity: str = "indexers",
    page_size: int = 1000,
) -> List[dict]:
    """Paginate a Graph Network subgraph query using id_gt cursor advancement.

    The query string must use ``$first`` (Int!) and ``$lastId`` (String!)
    variables, and order results by ``id``.  The function fetches pages until
    either an empty page is returned or a page smaller than ``page_size``
    indicates there are no more results.

    Raises ``RuntimeError`` on GraphQL-level errors (``"errors"`` key in
    response).  HTTP and connection errors from ``requests`` propagate with
    their original type, and any API key masked in the message, so the caller
    can decide on retry/fallback policy.

    Args:
        url: Subgraph endpoint URL.
        query: GraphQL query string with $first and $lastId variables.
        entity: Top-level field name inside ``data`` to extract results from.
        page_size: Number of entities per page (default 1000, the subgraph
            maximum).

    Returns:
        Flat list of entity dicts across all pages.
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
            raise RuntimeError(f"GraphQL errors: {data['errors']}")

        page = data.get("data", {}).get(entity, [])
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
