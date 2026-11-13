"""Uptime and success rate on each chain an indexer serves, blended with its other chains.

Each replayed query carries the chain of the subgraph it asked about, so an indexer's uptime and
success rate can be worked out on each chain. A chain with few queries says little on its own,
so it leans on the indexer's overall value, the query-weighted average over all its chains, this
one included: its own value counts in full from FULL_WEIGHT_QUERIES queries, and below that with
weight n / (n + PRIOR_QUERIES), the overall value making up the rest. Leaning on the other chains
alone would let 2 small chains swap values. The overall value is also what a chain the indexer
has no data on and the network-wide scores use.
"""

from __future__ import annotations

from typing import Mapping, Optional

# A chain with at least this many queries is scored on its own value alone.
FULL_WEIGHT_QUERIES = 10_000
# Below FULL_WEIGHT_QUERIES, a chain with this many queries weighs its own value and its prior
# equally.
PRIOR_QUERIES = 250


def own_weight(queries: float) -> float:
    """How much a chain's own value counts against its prior, from its query count."""
    if queries >= FULL_WEIGHT_QUERIES:
        return 1.0
    return queries / (queries + PRIOR_QUERIES)


def weighted_average(values: Mapping[str, float], queries: Mapping[str, float]) -> Optional[float]:
    """The average of ``values`` weighted by each chain's queries, or None with no queries."""
    total = sum(queries.get(chain, 0) for chain in values)
    if total <= 0:
        return None
    return sum(queries.get(chain, 0) * value for chain, value in values.items()) / total


def blend(
    values: Mapping[str, float], queries: Mapping[str, float]
) -> tuple[Optional[float], dict[str, float]]:
    """(overall, by chain) for 1 indexer from its value and query count on each chain.

    Each chain's value is blended with the overall value, the query-weighted average over every
    chain, so an indexer on 1 chain keeps its value there. With no queries on any chain there is
    no overall value, and each chain's own value stands.
    """
    overall = weighted_average(values, queries)
    by_chain = {}
    for chain, own in values.items():
        weight = own_weight(queries.get(chain, 0))
        by_chain[chain] = own if overall is None else weight * own + (1 - weight) * overall
    return overall, by_chain
