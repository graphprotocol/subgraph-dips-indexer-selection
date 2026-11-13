"""Rules for judging whether an indexer serves fresh data on a chain.

The freshness prober asks each indexer 1 real query per chain per minute through the gateway
and pushes the outcomes to IISA (see freshness_prober.py). Everything here is pure: no network,
no clock and no randomness, so each rule can be tested on its own. The outcome and reason codes
are the ones the service's POST /freshness accepts.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Collection, Iterable, Mapping, Optional, Sequence, TypeGuard, Union

PASS = "pass"
FAIL = "fail"
NO_EVIDENCE = "no_evidence"
VOID = "void"

STALE = "stale"
NO_TIMESTAMP = "no_timestamp"
INDEXER_ERROR = "indexer_error"
INDEXER_TIMEOUT = "indexer_timeout"
INDEXER_UNAVAILABLE = "indexer_unavailable"
PAYMENT_REFUSED = "payment_refused"
NO_ELIGIBLE_SUBGRAPH = "no_eligible_subgraph"
GATEWAY_ERROR = "gateway_error"
KEY_OUT_OF_FUNDS = "key_out_of_funds"
PAIRING_UNKNOWN = "pairing_unknown"
GENERAL_FAULT = "general_fault"
NOT_SENT = "not_sent"
SUBGRAPH_BROKEN = "subgraph_broken"

# The gateway returns an indexer's answer with the indexer's signature in this header and its own
# messages without it. Only the header decides which is which: an indexer could sign a body that
# reads like a gateway error to void its own check.
ATTESTATION_HEADER = "graph-attestation"

DETAIL_LIMIT = 300
# A block dated up to this long after its answer arrived counts as 0 seconds old, allowing for
# clocks that disagree a little; any later and the timestamp can't be trusted.
FUTURE_TOLERANCE_SECONDS = 60
# The last second a timestamp can name and still be a date (9999-12-31T23:59:59Z).
_MAX_BLOCK_TIMESTAMP = 253_402_300_799
# POST /freshness accepts at most this many outcomes in 1 minute.
MAX_PAIRS = 50_000

# A minute points to a fault between the gateway and indexers in general when at least this
# many indexers were asked and none answered, or at least the second number were asked and more
# than half didn't answer.
GENERAL_FAULT_ALL_SILENT_MIN = 3
GENERAL_FAULT_MAJORITY_SILENT_MIN = 10

# A subgraph is broken for everyone when at least this many indexers are behind on it while
# fresh on another subgraph of the same chain, and none answered fresh on it.
BROKEN_SUBGRAPH_MIN_SUPPORTERS = 2

_INDEXER_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_DEPLOYMENT_HASH = re.compile(r"^Qm[1-9A-HJ-NP-Za-km-z]{44}$")
_DEPLOYMENT_ENTITY_ID = re.compile(r"^0x[0-9a-fA-F]{64}$")
# Printable ASCII without spaces: every real network name fits, and a name an indexer made up
# can't hold text the service refuses or a line break that would start a fake log line.
_CHAIN_NAME = re.compile(r"[\x21-\x7e]{1,64}")

# The error the gateway names for the indexer, after "bad indexers: {0x...: ". The address is
# optional so a change in how the gateway lists indexers still reaches the error text.
_BAD_INDEXER_ERROR = re.compile(
    r"^bad indexers:\s*\{?\s*(?:0x[0-9a-fA-F]{40}\s*:\s*)?(?P<error>.*)", re.DOTALL
)
# indexer-service answers 400 or 402 when it rejects the gateway's payment receipt.
_PAYMENT_REFUSED_ERROR = re.compile(r'^BadResponse\("?40[02]\b')


@dataclass(frozen=True)
class GatewayReply:
    """What came back over HTTP from 1 query on the gateway's single-indexer route."""

    status: int
    attested: bool
    body: bytes
    arrived_at: float


@dataclass(frozen=True)
class GatewayUnreachable:
    """No HTTP answer: a timeout or a connection error."""

    detail: str


GatewayResult = Union[GatewayReply, GatewayUnreachable]


@dataclass(frozen=True)
class Judgement:
    """How 1 query came out, before it is tied to an indexer and chain."""

    outcome: str
    reason: Optional[str] = None
    detail: Optional[str] = None
    age_seconds: Optional[float] = None
    attested: bool = False


@dataclass(frozen=True)
class CheckResult:
    """1 minute's outcome for 1 (indexer, chain) pair.

    ``attested`` says the answer carried the indexer's signature (a stale answer is still an
    answer) and ``asked`` that a query was sent; the general-fault rule reads both. Only the
    other fields are pushed.
    """

    indexer: str
    chain: str
    outcome: str
    reason: Optional[str] = None
    detail: Optional[str] = None
    deployment: Optional[str] = None
    age_seconds: Optional[float] = None
    attested: bool = False
    asked: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "indexer": self.indexer,
            "chain": self.chain,
            "outcome": self.outcome,
            "reason": self.reason,
            "detail": self.detail,
            "deployment": self.deployment,
            "age_seconds": self.age_seconds,
        }


@dataclass(frozen=True)
class Allocation:
    """An open allocation, with the chain from its deployment's manifest."""

    indexer: str
    chain: str
    deployment_id: str
    ipfs_hash: str
    created_at: int


@dataclass(frozen=True)
class Targets:
    """What each minute checks: the (indexer, chain) pairs and each pair's eligible deployments."""

    pairs: tuple[tuple[str, str], ...]
    eligible: Mapping[tuple[str, str], tuple[str, ...]]


def truncate_detail(text: Optional[str]) -> Optional[str]:
    """Text the service accepts as ``detail``: anything UTF-8 can't hold, such as the half of a
    surrogate pair a JSON escape like \\ud800 decodes to, is written out as its escape, then the
    whole is cut to DETAIL_LIMIT characters."""
    if text is None:
        return None
    return text.encode("utf-8", "backslashreplace").decode("utf-8")[:DETAIL_LIMIT]


# ---------------------------------------------------------------------------
# Classifying a response
# ---------------------------------------------------------------------------


def judge(result: GatewayResult, bar_seconds: float) -> Judgement:
    """Judge 1 query: an indexer's answer on its block's age, a gateway message on its wording."""
    if isinstance(result, GatewayUnreachable):
        return Judgement(VOID, GATEWAY_ERROR, truncate_detail(result.detail))
    if result.status != 200:
        return Judgement(VOID, GATEWAY_ERROR, f"gateway answered HTTP {result.status}")
    if result.attested:
        return judge_indexer_answer(result.body, result.arrived_at, bar_seconds)
    return judge_gateway_message(result.body)


_UNPARSEABLE = object()


def _parse_json(body: bytes) -> object:
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        return _UNPARSEABLE


def _get_dict(value: object, key: str) -> Optional[dict]:
    child = value.get(key) if isinstance(value, dict) else None
    return child if isinstance(child, dict) else None


def _first_error_message(parsed: object) -> Optional[str]:
    errors = parsed.get("errors") if isinstance(parsed, dict) else None
    if not isinstance(errors, list) or not errors or not isinstance(errors[0], dict):
        return None
    message = errors[0].get("message")
    return message if isinstance(message, str) else None


def _is_unix_seconds(value: object) -> TypeGuard[int]:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= _MAX_BLOCK_TIMESTAMP
    )


def judge_indexer_answer(body: bytes, arrived_at: float, bar_seconds: float) -> Judgement:
    """Judge an answer the indexer signed: fresh when its block is at most the bar old."""
    parsed = _parse_json(body)
    block = _get_dict(_get_dict(_get_dict(parsed, "data"), "_meta"), "block")
    if block is None:
        detail = _first_error_message(parsed) or "answer has no _meta block"
        return Judgement(FAIL, INDEXER_ERROR, truncate_detail(detail), attested=True)

    timestamp = block.get("timestamp")
    if timestamp is None:
        # graph-node leaves the timestamp out once the block has left its block store, which
        # only happens when the subgraph is far behind.
        return Judgement(FAIL, NO_TIMESTAMP, attested=True)
    if not _is_unix_seconds(timestamp):
        detail = "block timestamp is not a unix time in seconds"
        return Judgement(FAIL, INDEXER_ERROR, detail, attested=True)
    ahead = timestamp - arrived_at
    if ahead > FUTURE_TOLERANCE_SECONDS:
        detail = f"block is dated {ahead:.0f}s in the future"
        return Judgement(FAIL, INDEXER_ERROR, detail, attested=True)

    age = round(max(0.0, arrived_at - timestamp), 3)
    if age <= bar_seconds:
        return Judgement(PASS, age_seconds=age, attested=True)
    return Judgement(FAIL, STALE, age_seconds=age, attested=True)


def judge_gateway_message(body: bytes) -> Judgement:
    """Judge a reply the indexer didn't sign, by how the gateway's message starts."""
    parsed = _parse_json(body)
    if parsed is _UNPARSEABLE:
        return Judgement(VOID, GATEWAY_ERROR, "gateway reply is not JSON")
    message = _first_error_message(parsed)
    if message is None:
        return Judgement(VOID, GATEWAY_ERROR, "gateway reply has no attestation and no error")
    outcome, reason = _gateway_message_outcome(message)
    return Judgement(outcome, reason, truncate_detail(message))


def _gateway_message_outcome(message: str) -> tuple[str, str]:
    # Matched on how the whole message starts: the text inside "bad indexers: {...}" comes from
    # the indexer and can itself contain words like "internal error".
    if message.startswith("bad indexers:"):
        return _bad_indexer_outcome(message)
    if message.startswith(("no indexers found", "subgraph not found")):
        return VOID, PAIRING_UNKNOWN
    if message.startswith("auth error"):
        return VOID, KEY_OUT_OF_FUNDS
    return VOID, GATEWAY_ERROR


def _bad_indexer_outcome(message: str) -> tuple[str, str]:
    match = _BAD_INDEXER_ERROR.match(message)
    error = match.group("error") if match else ""
    if _PAYMENT_REFUSED_ERROR.match(error):
        # The indexer may be owed more than the gateway's escrow holds, which IISA can't see, so
        # a refusal can't be shown to be the indexer's fault.
        return VOID, PAYMENT_REFUSED
    if error.startswith("Timeout"):
        return FAIL, INDEXER_TIMEOUT
    if error.startswith("Unavailable(internal error"):
        # The gateway's own inconsistency, which its code logs as something that shouldn't happen.
        return VOID, GATEWAY_ERROR
    if error.startswith("Unavailable("):
        return FAIL, INDEXER_UNAVAILABLE
    return FAIL, INDEXER_ERROR


# ---------------------------------------------------------------------------
# The general-fault rule
# ---------------------------------------------------------------------------


def is_general_fault(results: Iterable[CheckResult]) -> bool:
    """True when too few of the indexers asked this minute answered at all, across all chains."""
    results = list(results)
    asked = {r.indexer for r in results if r.asked}
    answered = {r.indexer for r in results if r.asked and r.attested}
    silent = len(asked) - len(answered)
    if len(asked) >= GENERAL_FAULT_ALL_SILENT_MIN and not answered:
        return True
    return len(asked) >= GENERAL_FAULT_MAJORITY_SILENT_MIN and silent * 2 > len(asked)


def apply_general_fault(results: Sequence[CheckResult]) -> list[CheckResult]:
    """Void every fail not backed by a signed answer when the minute points to a general fault."""
    if not is_general_fault(results):
        return list(results)
    return [
        _void_for_general_fault(r) if r.outcome == FAIL and not r.attested else r for r in results
    ]


def _void_for_general_fault(result: CheckResult) -> CheckResult:
    was = f"{result.reason}: {result.detail}" if result.detail else result.reason
    return replace(result, outcome=VOID, reason=GENERAL_FAULT, detail=truncate_detail(was))


# ---------------------------------------------------------------------------
# Which deployments an indexer is asked about
# ---------------------------------------------------------------------------


def _oldest_open(allocations: Iterable[Allocation]) -> dict[tuple[str, str], Allocation]:
    """The oldest open allocation for each (indexer, deployment entity id)."""
    oldest: dict[tuple[str, str], Allocation] = {}
    for allocation in allocations:
        key = (allocation.indexer, allocation.deployment_id)
        if key not in oldest or allocation.created_at < oldest[key].created_at:
            oldest[key] = allocation
    return oldest


def first_allocation_lookups(
    allocations: Iterable[Allocation],
    now: float,
    min_age_seconds: float,
    known: Mapping[tuple[str, str], int],
) -> list[tuple[str, str]]:
    """(indexer, deployment entity id) pairs whose open allocation is too young to decide alone
    and whose first allocation ever isn't known yet."""
    return sorted(
        key
        for key, allocation in _oldest_open(allocations).items()
        if now - allocation.created_at < min_age_seconds and key not in known
    )


def eligible_deployments(
    allocations: Iterable[Allocation],
    first_allocated: Mapping[tuple[str, str], int],
    now: float,
    min_age_seconds: float,
) -> dict[tuple[str, str], tuple[str, ...]]:
    """Map each (indexer, chain) to the deployments it may be asked about.

    A deployment is eligible when the indexer has an open allocation on it and first allocated
    to it at least ``min_age_seconds`` ago. A young open allocation counts from the first
    allocation ever, so closing and reopening doesn't restart the wait.
    """
    eligible: dict[tuple[str, str], set[str]] = {}
    for key, allocation in _oldest_open(allocations).items():
        first = min(allocation.created_at, first_allocated.get(key, allocation.created_at))
        if now - first >= min_age_seconds:
            eligible.setdefault((allocation.indexer, allocation.chain), set()).add(
                allocation.ipfs_hash
            )
    return {pair: tuple(sorted(hashes)) for pair, hashes in eligible.items()}


# ---------------------------------------------------------------------------
# Which pairs are checked, and what is pushed
# ---------------------------------------------------------------------------


def is_indexer_address(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(_INDEXER_ADDRESS.fullmatch(value))


def is_deployment_hash(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(_DEPLOYMENT_HASH.fullmatch(value))


def is_deployment_entity_id(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(_DEPLOYMENT_ENTITY_ID.fullmatch(value))


def is_chain_name(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(_CHAIN_NAME.fullmatch(value))


@dataclass
class LeftOut:
    """What dips_pairs left out of GET /scores records, for the caller to log once per refresh
    rather than once per item."""

    # (indexer, the entry it listed) for each listed chain that isn't a known network
    refused_names: list[tuple[str, object]] = field(default_factory=list)
    # Indexers whose price field is there but can't be read
    unreadable_prices: list[str] = field(default_factory=list)
    # Listed chains without a price, which selection can't pick an indexer on anyway
    unpriced: int = 0
    # No record had a price field, so every listed chain was paired, as selection then does
    no_price_field: bool = False


def _listed(value: object) -> list[object]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, RecursionError):
            return []
    return value if isinstance(value, list) else []


def supported_chains(value: object) -> list[str]:
    """The chain names in a ``dips_supported_networks`` value, a list or a JSON string of one.

    Anything unreadable gives no chains, as the service's own parser does.
    """
    return list(dict.fromkeys(chain for chain in _listed(value) if is_chain_name(chain)))


def _is_price(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return False
    try:
        price = float(value)
    except (ValueError, OverflowError):
        return False
    return math.isfinite(price) and price >= 0


def _priced_chains(value: object) -> Optional[set[str]]:
    """The chains with a usable price in a ``dips_min_grt_per_30_days`` value, an object or a
    JSON string of one, or None when the value is there but can't be read. Selection leaves an
    indexer out on any other chain, as the service's _extract_chain_price reads a price: a finite
    number of at least 0, not true or false.
    """
    if value is None:
        return set()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, RecursionError):
            return None
    if not isinstance(value, dict):
        return None
    return {chain for chain, price in value.items() if _is_price(price)}


def _is_known(entry: object, networks: Collection[str]) -> bool:
    return is_chain_name(entry) and entry in networks


def _chains_of(
    indexer: str, record: Mapping[str, Any], networks: Collection[str], left_out: LeftOut
) -> list[str]:
    entries = _listed(record.get("dips_supported_networks"))
    left_out.refused_names.extend(
        (indexer, entry) for entry in entries if not _is_known(entry, networks)
    )
    listed = [chain for chain in supported_chains(entries) if chain in networks]
    if left_out.no_price_field:
        return listed
    priced = _priced_chains(record.get("dips_min_grt_per_30_days"))
    if priced is None:
        left_out.unreadable_prices.append(indexer)
        priced = set()
    kept = [chain for chain in listed if chain in priced]
    left_out.unpriced += len(listed) - len(kept)
    return kept


def _offered_chains(
    records: Iterable[object], networks: Collection[str], left_out: LeftOut
) -> dict[str, list[str]]:
    """Each indexer's chains from GET /scores records: those in ``networks`` that it offers DIPs
    on and has priced, or all such chains when no record has a price field, in the order it
    lists them, without repeats. Indexers are in lower case."""
    usable = [r for r in records if isinstance(r, dict) and is_indexer_address(r.get("indexer"))]
    # The service applies no price filter when its scores have no price column at all.
    left_out.no_price_field = bool(usable) and not any(
        "dips_min_grt_per_30_days" in record for record in usable
    )
    chains: dict[str, dict[str, None]] = {}
    for record in usable:
        indexer = record["indexer"].lower()
        offered = chains.setdefault(indexer, {})
        offered.update(dict.fromkeys(_chains_of(indexer, record, networks, left_out)))
    return {indexer: list(offered) for indexer, offered in chains.items() if offered}


def dips_pairs(
    records: Iterable[object], networks: Collection[str], left_out: Optional[LeftOut] = None
) -> list[tuple[str, str]]:
    """Every (indexer, chain) to check from GET /scores records, sorted: each chain in
    ``networks`` that an indexer offers DIPs on and has priced. What is left out is added to
    ``left_out`` when it is given."""
    left_out = LeftOut() if left_out is None else left_out
    offered = _offered_chains(records, networks, left_out)
    return sorted((indexer, chain) for indexer, chains in offered.items() for chain in chains)


def format_minute(minute_start: float) -> str:
    return datetime.fromtimestamp(minute_start, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def minute_payload(minute_start: float, results: Iterable[CheckResult]) -> dict[str, Any]:
    """The POST /freshness body for 1 minute."""
    return {
        "minute": format_minute(minute_start),
        "outcomes": [result.to_payload() for result in results],
    }


# ---------------------------------------------------------------------------
# A subgraph broken for every indexer on it
# ---------------------------------------------------------------------------


def is_behind(judgement: Optional[Judgement]) -> bool:
    """An answer whose block was too old, or so old that it had no timestamp."""
    return (
        judgement is not None
        and judgement.outcome == FAIL
        and judgement.reason in (STALE, NO_TIMESTAMP)
    )


def supports_broken_verdict(
    on_subgraph: Optional[Judgement], elsewhere: Optional[Judgement]
) -> bool:
    """An indexer behind on the subgraph but fresh on another of the same chain: its own chain
    data is fine, so the subgraph is what is stuck."""
    return is_behind(on_subgraph) and elsewhere is not None and elsewhere.outcome == PASS


def is_broken_for_everyone(supporters: int, anyone_fresh_on_it: bool) -> bool:
    return supporters >= BROKEN_SUBGRAPH_MIN_SUPPORTERS and not anyone_fresh_on_it
