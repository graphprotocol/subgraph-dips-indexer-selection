"""Rules for judging whether an indexer serves fresh data on a chain.

The freshness prober asks each indexer 1 real query per chain per minute through the gateway
and pushes the outcomes to IISA (see freshness_prober.py). Everything here is pure: no network,
no clock and no randomness, so each rule can be tested on its own. The outcome and reason codes
are the ones the service's POST /freshness accepts.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence, TypeGuard, Union

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
CHAIN_NAME_LIMIT = 64
# POST /freshness accepts at most this many outcomes in 1 minute.
MAX_PAIRS = 50_000

# A minute points to a fault between the gateway and indexers in general when at least this
# many indexers were asked and none answered, or at least the second number were asked and more
# than half didn't answer.
GENERAL_FAULT_ALL_SILENT_MIN = 3
GENERAL_FAULT_MAJORITY_SILENT_MIN = 10

_INDEXER_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_DEPLOYMENT_HASH = re.compile(r"^Qm[1-9A-HJ-NP-Za-km-z]{44}$")
_DEPLOYMENT_ENTITY_ID = re.compile(r"^0x[0-9a-fA-F]{64}$")

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
    return None if text is None else text[:DETAIL_LIMIT]


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
        # Whose fault a refused payment is depends on the gateway's escrow, which is not read
        # here, so it is not counted for now.
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
    return isinstance(value, str) and bool(_INDEXER_ADDRESS.match(value))


def is_deployment_hash(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(_DEPLOYMENT_HASH.match(value))


def is_deployment_entity_id(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(_DEPLOYMENT_ENTITY_ID.match(value))


def is_chain_name(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and 0 < len(value) <= CHAIN_NAME_LIMIT


def supported_chains(value: object) -> list[str]:
    """The chain names in a ``dips_supported_networks`` value, a list or a JSON string of one.

    Anything unreadable gives no chains, as the service's own parser does.
    """
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, RecursionError):
            return []
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(chain for chain in value if is_chain_name(chain)))


def dips_pairs(records: Iterable[object]) -> list[tuple[str, str]]:
    """Every (indexer, chain) an indexer offers DIPs on, from GET /scores records, sorted."""
    pairs: set[tuple[str, str]] = set()
    for record in records:
        if not isinstance(record, dict) or not is_indexer_address(record.get("indexer")):
            continue
        indexer = record["indexer"].lower()
        pairs.update(
            (indexer, chain) for chain in supported_chains(record.get("dips_supported_networks"))
        )
    return sorted(pairs)


def format_minute(minute_start: float) -> str:
    return datetime.fromtimestamp(minute_start, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def minute_payload(minute_start: float, results: Iterable[CheckResult]) -> dict[str, Any]:
    """The POST /freshness body for 1 minute."""
    return {
        "minute": format_minute(minute_start),
        "outcomes": [result.to_payload() for result in results],
    }
