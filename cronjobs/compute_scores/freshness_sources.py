"""Where the freshness prober learns what to check: GET /scores and the network subgraph.

Every FRESHNESS_REFRESH_INTERVAL the prober reads which chains each indexer offers DIPs on, then
each of those indexers' open allocations, and works out which deployments it may be asked about
(freshness_rules.eligible_deployments). The network subgraph URL carries an API key, so errors
raised here have it masked, as subgraph.py does.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Collection, Optional, Sequence

import requests
from escaping import for_log
from freshness_rules import (
    MAX_PAIRS,
    Allocation,
    LeftOut,
    Targets,
    dips_pairs,
    eligible_deployments,
    first_allocation_lookups,
    is_chain_name,
    is_deployment_entity_id,
    is_deployment_hash,
)
from subgraph import (
    SubgraphResponseError,
    _graphql_errors_are_permanent,
    paginate_subgraph_query,
    redact_url,
    retry_subgraph_read,
)

logger = logging.getLogger(__name__)

# Indexers per page of the allocations query. Each brings up to NESTED_ALLOCATION_LIMIT
# allocations, so pages stay small.
INDEXERS_PER_PAGE = 20
NESTED_ALLOCATION_LIMIT = 1000
FIRST_ALLOCATIONS_PER_QUERY = 50

_ALLOCATION_FIELDS = "id createdAt subgraphDeployment { id ipfsHash manifest { network } }"

_INDEXER_ALLOCATIONS_QUERY = """
query($first: Int!, $lastId: String!) {
  indexers(first: $first, where: { id_gt: $lastId, id_in: [%(ids)s] }, orderBy: id) {
    id
    allocations(first: %(limit)d, orderBy: id, where: { status: Active }) { %(fields)s }
  }
}
"""

# For an indexer with more open allocations than 1 nested page holds.
_ALLOCATIONS_OF_ONE_INDEXER_QUERY = """
query($first: Int!, $lastId: String!) {
  allocations(
    first: $first, orderBy: id, where: { id_gt: $lastId, indexer: "%(indexer)s", status: Active }
  ) { %(fields)s }
}
"""

_FIRST_ALLOCATION_FIELD = (
    "%(alias)s: allocations(first: 1, orderBy: createdAt, orderDirection: asc, where: "
    '{ indexer: "%(indexer)s", subgraphDeployment: "%(deployment)s" }) { createdAt }'
)


def post_graphql(url: str, query: str) -> dict:
    """Send 1 GraphQL query and return its ``data``, raising as paginate_subgraph_query does."""
    try:
        response = requests.post(url, json={"query": query}, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        # requests puts the full URL in its messages; "from None" keeps the unmasked original
        # out of any traceback a caller logs.
        raise type(e)(redact_url(str(e)), response=e.response) from None
    body = response.json()
    if isinstance(body, dict) and "errors" in body:
        raise SubgraphResponseError(
            f"GraphQL errors: {body['errors']}",
            retryable=not _graphql_errors_are_permanent(body["errors"]),
        )
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        raise SubgraphResponseError(f"subgraph reply has no data: {body!r:.200}")
    return data


def _created_at(raw: dict) -> Optional[int]:
    try:
        return int(raw.get("createdAt"))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _allocation(indexer: str, raw: Any) -> Optional[Allocation]:
    """An Allocation from 1 raw record, or None when it lacks a usable chain or deployment."""
    deployment = raw.get("subgraphDeployment") if isinstance(raw, dict) else None
    if not isinstance(deployment, dict):
        return None
    manifest = deployment.get("manifest")
    chain = manifest.get("network") if isinstance(manifest, dict) else None
    created_at = _created_at(raw)
    if (
        created_at is None
        or not is_chain_name(chain)
        or not is_deployment_hash(deployment.get("ipfsHash"))
        or not is_deployment_entity_id(deployment.get("id"))
    ):
        return None
    return Allocation(
        indexer=indexer,
        chain=chain,
        deployment_id=deployment["id"].lower(),
        ipfs_hash=deployment["ipfsHash"],
        created_at=created_at,
    )


def _all_allocations_of(url: str, indexer: str) -> list:
    query = _ALLOCATIONS_OF_ONE_INDEXER_QUERY % {"indexer": indexer, "fields": _ALLOCATION_FIELDS}
    return paginate_subgraph_query(url, query, entity="allocations")


@retry_subgraph_read
def read_open_allocations(url: str, indexers: Sequence[str]) -> list[Allocation]:
    """Every open allocation of ``indexers`` with a known chain, from the network subgraph.

    Raises when none of them has any, which more likely means the gateway served the read from
    an indexer that is behind than that every DIPs indexer closed everything at once.
    """
    if not indexers:
        return []
    query = _INDEXER_ALLOCATIONS_QUERY % {
        "ids": ", ".join(f'"{indexer}"' for indexer in indexers),
        "limit": NESTED_ALLOCATION_LIMIT,
        "fields": _ALLOCATION_FIELDS,
    }
    raw_indexers = paginate_subgraph_query(
        url, query, entity="indexers", page_size=INDEXERS_PER_PAGE
    )

    allocations: list[Allocation] = []
    skipped = 0
    for raw_indexer in raw_indexers:
        indexer = str(raw_indexer.get("id", "")).lower()
        raw_allocations = raw_indexer.get("allocations") or []
        if len(raw_allocations) >= NESTED_ALLOCATION_LIMIT:
            raw_allocations = _all_allocations_of(url, indexer)
        for raw in raw_allocations:
            allocation = _allocation(indexer, raw)
            skipped += allocation is None
            if allocation is not None:
                allocations.append(allocation)

    if skipped:
        logger.info("Left out %d open allocations without a known chain or deployment", skipped)
    if not allocations:
        raise SubgraphResponseError(
            f"network subgraph returned no open allocations for any of {len(indexers)} indexers"
        )
    return allocations


@retry_subgraph_read
def _read_first_allocation_chunk(
    url: str, keys: Sequence[tuple[str, str]]
) -> dict[tuple[str, str], int]:
    fields = "\n".join(
        _FIRST_ALLOCATION_FIELD % {"alias": f"a{i}", "indexer": indexer, "deployment": deployment}
        for i, (indexer, deployment) in enumerate(keys)
    )
    data = post_graphql(url, "{\n" + fields + "\n}")
    found = {}
    for i, key in enumerate(keys):
        rows = data.get(f"a{i}")
        created_at = _created_at(rows[0]) if isinstance(rows, list) and rows else None
        if created_at is not None:
            found[key] = created_at
    return found


def read_first_allocations(url: str, keys: Sequence[tuple[str, str]]) -> dict[tuple[str, str], int]:
    """When each (indexer, deployment entity id) was first allocated to, open or closed."""
    found: dict[tuple[str, str], int] = {}
    for start in range(0, len(keys), FIRST_ALLOCATIONS_PER_QUERY):
        found.update(
            _read_first_allocation_chunk(url, keys[start : start + FIRST_ALLOCATIONS_PER_QUERY])
        )
    return found


def score_records(scores_response: Any) -> list:
    """The records in a GET /scores response; raise ValueError when it holds none."""
    records = scores_response.get("scores") if isinstance(scores_response, dict) else None
    if not isinstance(records, list) or not records:
        raise ValueError("GET /scores returned no score records")
    return records


def _counted(count: int, noun: str) -> str:
    return f"{count:,} {noun}" if count == 1 else f"{count:,} {noun}s"


def _log_left_out(left_out: LeftOut) -> None:
    """1 line for each kind of thing a refresh left out of GET /scores, however many there were."""
    if left_out.no_price_field:
        logger.warning(
            "GET /scores records have no dips_min_grt_per_30_days field, so every chain an "
            "indexer lists is checked, as selection applies no price filter without it"
        )
    if left_out.refused_names:
        indexer, name = left_out.refused_names[0]
        logger.warning(
            "Left out chain names that aren't in The Graph's networks registry or "
            "FRESHNESS_ADDITIONAL_NETWORKS: %s, listed by %s, such as %s from indexer %s",
            _counted(len(left_out.refused_names), "chain name"),
            _counted(len({indexer for indexer, _ in left_out.refused_names}), "indexer"),
            for_log(name),
            indexer,
        )
    if left_out.unreadable_prices:
        logger.warning(
            "Couldn't read the DIPs prices of %s, so none of their chains is checked, "
            "such as indexer %s",
            _counted(len(left_out.unreadable_prices), "indexer"),
            left_out.unreadable_prices[0],
        )
    if left_out.unpriced:
        logger.debug(
            "Left out %s without a price, which selection can't pick an indexer on",
            _counted(left_out.unpriced, "listed chain"),
        )


class TargetLoader:
    """Builds the Targets each minute checks, keeping first-allocation times between refreshes,
    since an indexer's first allocation on a deployment never changes."""

    def __init__(
        self,
        *,
        networks: Callable[[], Collection[str]],
        fetch_scores: Callable[[], Any],
        read_allocations: Callable[[Sequence[str]], list[Allocation]],
        read_first_allocations: Callable[[Sequence[tuple[str, str]]], dict[tuple[str, str], int]],
        min_allocation_age_seconds: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._networks = networks
        self._fetch_scores = fetch_scores
        self._read_allocations = read_allocations
        self._read_first_allocations = read_first_allocations
        self._min_age = min_allocation_age_seconds
        self._clock = clock
        self._first_allocated: dict[tuple[str, str], int] = {}

    def __call__(self) -> Targets:
        networks = self._networks()
        left_out = LeftOut()
        pairs = dips_pairs(score_records(self._fetch_scores()), networks, left_out)
        _log_left_out(left_out)
        if len(pairs) > MAX_PAIRS:
            logger.error(
                "%d (indexer, chain) pairs offer DIPs, more than 1 push holds; checking %d",
                len(pairs),
                MAX_PAIRS,
            )
            pairs = pairs[:MAX_PAIRS]

        indexers = sorted({indexer for indexer, _ in pairs})
        allocations = self._read_allocations(indexers) if indexers else []
        now = self._clock()
        open_keys = {(a.indexer, a.deployment_id) for a in allocations}
        # Only open allocations need a first allocation, so the cache holds no more than those.
        known = {key: t for key, t in self._first_allocated.items() if key in open_keys}
        lookups = first_allocation_lookups(allocations, now, self._min_age, known)
        if lookups:
            known.update(self._read_first_allocations(lookups))
        self._first_allocated = known

        eligible = eligible_deployments(allocations, known, now, self._min_age)
        wanted = set(pairs)
        return Targets(
            pairs=tuple(pairs),
            eligible={pair: hashes for pair, hashes in eligible.items() if pair in wanted},
        )
