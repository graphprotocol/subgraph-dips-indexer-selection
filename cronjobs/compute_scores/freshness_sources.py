"""Where the freshness prober learns what to check: GET /scores and the network subgraph.

Every FRESHNESS_REFRESH_INTERVAL the prober reads which chains each indexer offers DIPs on, then
each of those indexers' open allocations, and works out which deployments it may be asked about
(freshness_rules.eligible_deployments). The network subgraph URL carries an API key, so errors
raised here have it masked, as subgraph.py does.
"""

from __future__ import annotations

import json
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
    escrow_is_funded,
    first_allocation_lookups,
    is_chain_name,
    is_deployment_entity_id,
    is_deployment_hash,
    is_indexer_address,
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
ESCROWS_PER_QUERY = 50
# While escrow reads fail, the last answers that were read are used for at most this long. Past it
# an empty escrow recorded long ago would keep excusing refused payments, made-up ones included.
ESCROW_REUSE_LIMIT_SECONDS = 3600

# Reads for second opinions get 1 attempt and this many seconds: a verdict too late for its
# minute does no good, and a slow read mustn't hold up the next one.
SECOND_OPINION_READ_TIMEOUT = 5

# What a failed read raises when the network or the subgraph is at fault rather than this code.
EXPECTED_READ_ERRORS = (requests.RequestException, SubgraphResponseError, ValueError)

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

# For a second opinion: 1 indexer's open deployments on 1 chain, each with its oldest open
# allocation there. A chain filter on the indexer's allocations would nest 2 child filters, which
# the network subgraph refuses.
_DEPLOYMENTS_ON_CHAIN_FIELD = (
    "%(alias)s: subgraphDeployments(first: 1000, where: { manifest_: { network: %(chain)s }, "
    'indexerAllocations_: { indexer: "%(indexer)s", status: Active } }) { ipfsHash '
    'indexerAllocations(first: 1, orderBy: createdAt, where: { indexer: "%(indexer)s", '
    "status: Active }) { createdAt } }"
)

_ESCROW_FIELD = (
    '%(alias)s: paymentsEscrowAccounts(first: 1000, where: { receiver: "%(indexer)s", '
    "payer_in: [%(payers)s] }) { payer { id } collector balance totalAmountThawing }"
)

# The gateway only routes to the 100 largest open allocations on a subgraph.
_ALLOCATED_INDEXERS_QUERY = """
{
  subgraphDeployments(where: { ipfsHash: "%(deployment)s" }) {
    indexerAllocations(
      first: 100, orderBy: allocatedTokens, orderDirection: desc, where: { status: Active }
    ) { indexer { id } }
  }
}
"""


def post_graphql(url: str, query: str, timeout: float = 30) -> dict:
    """Send 1 GraphQL query and return its ``data``, raising as paginate_subgraph_query does."""
    try:
        response = requests.post(url, json={"query": query}, timeout=timeout)
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
    query = _INDEXER_ALLOCATIONS_QUERY % _allocations_query_values(indexers)
    raw_indexers = paginate_subgraph_query(
        url, query, entity="indexers", page_size=INDEXERS_PER_PAGE
    )

    def every_allocation(indexer: str, raw_allocations: list) -> list:
        if len(raw_allocations) >= NESTED_ALLOCATION_LIMIT:
            return _all_allocations_of(url, indexer)
        return raw_allocations

    allocations = _parse_allocations(raw_indexers, every_allocation)
    if not allocations:
        raise SubgraphResponseError(
            f"network subgraph returned no open allocations for any of {len(indexers)} indexers"
        )
    return allocations


def _allocations_query_values(indexers: Sequence[str]) -> dict[str, Any]:
    return {
        "ids": ", ".join(f'"{indexer}"' for indexer in indexers),
        "limit": NESTED_ALLOCATION_LIMIT,
        "fields": _ALLOCATION_FIELDS,
    }


def _parse_allocations(
    raw_indexers: list, complete: Callable[[str, list], list]
) -> list[Allocation]:
    """The usable allocations in raw indexer records; ``complete`` can fetch the rest of an
    indexer's allocations when the nested list was cut short."""
    allocations: list[Allocation] = []
    skipped = 0
    for raw_indexer in raw_indexers:
        indexer = str(raw_indexer.get("id", "")).lower()
        for raw in complete(indexer, raw_indexer.get("allocations") or []):
            allocation = _allocation(indexer, raw)
            skipped += allocation is None
            if allocation is not None:
                allocations.append(allocation)
    if skipped:
        logger.info("Left out %d open allocations without a known chain or deployment", skipped)
    return allocations


def read_deployments_on_chain(
    url: str, indexers: Sequence[str], chain: str
) -> dict[str, dict[str, int]]:
    """For a second opinion, each indexer's open deployments on ``chain``, each with when its
    oldest open allocation there was created, in 1 quick attempt.

    Raises ValueError before sending anything when an address or the chain isn't one that may
    go into a query, and SubgraphResponseError when the reply can't be read.
    """
    if not is_chain_name(chain) or not all(is_indexer_address(i) for i in indexers):
        raise ValueError(f"not an indexer address or chain name: {indexers!r:.200}, {chain!r:.80}")
    if not indexers:
        return {}
    fields = "\n".join(
        _DEPLOYMENTS_ON_CHAIN_FIELD
        % {"alias": f"a{i}", "indexer": indexer.lower(), "chain": json.dumps(chain)}
        for i, indexer in enumerate(indexers)
    )
    data = post_graphql(url, "{\n" + fields + "\n}", timeout=SECOND_OPINION_READ_TIMEOUT)
    return {indexer: _deployment_ages(data.get(f"a{i}")) for i, indexer in enumerate(indexers)}


def _deployment_ages(rows: object) -> dict[str, int]:
    if not isinstance(rows, list):
        raise SubgraphResponseError(f"unreadable deployments in a subgraph reply: {rows!r:.200}")
    ages: dict[str, int] = {}
    skipped = 0
    for row in rows:
        oldest = row.get("indexerAllocations") if isinstance(row, dict) else None
        first = oldest[0] if isinstance(oldest, list) and oldest else None
        created_at = _created_at(first) if isinstance(first, dict) else None
        if created_at is None or not is_deployment_hash(row.get("ipfsHash")):
            skipped += 1
            continue
        ages[row["ipfsHash"]] = created_at
    if skipped:
        logger.info("Left out %d deployments without a usable hash or allocation time", skipped)
    return ages


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


@retry_subgraph_read
def _read_escrow_chunk(
    url: str, indexers: Sequence[str], payers: Collection[str], collector: Optional[str]
) -> dict[str, bool]:
    payer_list = ", ".join(f'"{payer}"' for payer in payers)
    fields = "\n".join(
        _ESCROW_FIELD % {"alias": f"e{i}", "indexer": indexer, "payers": payer_list}
        for i, indexer in enumerate(indexers)
    )
    data = post_graphql(url, "{\n" + fields + "\n}")
    return {
        indexer: escrow_is_funded(data.get(f"e{i}"), payers, collector)
        for i, indexer in enumerate(indexers)
    }


def read_escrow_funded(
    url: str, indexers: Sequence[str], payers: Collection[str], collector: Optional[str] = None
) -> dict[str, bool]:
    """Whether the gateway's escrow holds funds for each indexer, from ``payers``' accounts,
    and only those under ``collector`` when it is given.

    An indexer with no account at all is unfunded. Raises ValueError, before anything is sent,
    when ``payers`` is empty or holds anything but addresses, and raises when any reply can't be
    read.
    """
    if not payers or not all(is_indexer_address(payer) for payer in payers):
        raise ValueError(f"escrow payers must be 0x addresses: {sorted(map(repr, payers))!s:.200}")
    payers = tuple(sorted({payer.lower() for payer in payers}))
    funded: dict[str, bool] = {}
    for start in range(0, len(indexers), ESCROWS_PER_QUERY):
        chunk = indexers[start : start + ESCROWS_PER_QUERY]
        funded.update(_read_escrow_chunk(url, chunk, payers, collector))
    return funded


def read_allocated_indexers(url: str, deployment: str) -> list[str]:
    """The indexers with an open allocation on ``deployment`` that the gateway routes to, in 1
    quick attempt, for a second opinion."""
    if not is_deployment_hash(deployment):
        raise ValueError(f"not a deployment hash: {deployment!r:.80}")
    query = _ALLOCATED_INDEXERS_QUERY % {"deployment": deployment}
    data = post_graphql(url, query, timeout=SECOND_OPINION_READ_TIMEOUT)
    indexers = []
    for raw_deployment in data.get("subgraphDeployments") or []:
        for allocation in raw_deployment.get("indexerAllocations") or []:
            indexer = (allocation.get("indexer") or {}).get("id")
            if is_indexer_address(indexer):
                indexers.append(indexer.lower())
    return list(dict.fromkeys(indexers))


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
        read_escrow: Callable[[Sequence[str]], dict[str, bool]],
        min_allocation_age_seconds: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._networks = networks
        self._fetch_scores = fetch_scores
        self._read_allocations = read_allocations
        self._read_first_allocations = read_first_allocations
        self._read_escrow = read_escrow
        self._min_age = min_allocation_age_seconds
        self._clock = clock
        self._first_allocated: dict[tuple[str, str], int] = {}
        self._last_escrow: dict[str, bool] = {}
        self._last_escrow_at: Optional[float] = None

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
            escrow_funded=self._escrow(indexers),
        )

    def _escrow(self, indexers: Sequence[str]) -> dict[str, bool]:
        """The escrow of each indexer. When it can't be read, the answers of the last read that
        worked are used again rather than keeping the whole refresh back; an indexer they don't
        cover has its refused payments counted against it."""
        if not indexers:
            return {}
        try:
            funded = self._read_escrow(indexers)
        except Exception as e:
            return self._escrow_after_failed_read(e)
        self._last_escrow, self._last_escrow_at = funded, self._clock()
        return funded

    def _escrow_after_failed_read(self, error: Exception) -> dict[str, bool]:
        read_at = self._last_escrow_at
        age = None if read_at is None else self._clock() - read_at
        cause = (type(error).__name__, error)
        exc_info = not isinstance(error, EXPECTED_READ_ERRORS)
        if age is not None and age > ESCROW_REUSE_LIMIT_SECONDS:
            logger.warning(
                "The gateway's escrow has been unreadable for over an hour (last read %.0fs ago), "
                "so its old answers are dropped and refused payments count against indexers "
                "until it can be read again: %s: %s",
                age,
                *cause,
                exc_info=exc_info,
            )
            return {}
        logger.warning(
            "Couldn't read the gateway's escrow, so the last answers (%s) are used until the "
            "next refresh: %s: %s",
            "none read yet" if age is None else f"read {age:.0f}s ago",
            *cause,
            exc_info=exc_info,
        )
        return dict(self._last_escrow)
