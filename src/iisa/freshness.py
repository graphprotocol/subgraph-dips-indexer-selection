"""Freshness: whether each indexer served up-to-date data on each chain, minute by minute.

A prober checks every indexer on every chain it offers DIPs on once a minute and pushes that
minute's outcomes to the service. From them this module keeps, for each indexer and chain, a
stale mark worked out from the last 10 minutes, and daily counts of passed and counted checks
for the last 28 days. Minutes and days are counted from the Unix epoch in UTC, so a day here
is a UTC day.
"""

import logging
import threading
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ValidationError

__all__ = [
    "Check",
    "FreshnessState",
    "FreshnessStore",
    "load_state",
    "minute_index",
    "minute_text",
]

logger = logging.getLogger(__name__)

OutcomeKind = Literal["pass", "fail", "no_evidence", "void"]

WINDOW_MINUTES = 10
MIN_COUNTED_MINUTES = 5
HISTORY_DAYS = 28
MINUTES_PER_DAY = 1_440

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def minute_index(moment: datetime) -> int:
    """Whole minutes from the Unix epoch to the time-zone-aware ``moment``, rounded down."""
    return (moment - _EPOCH) // timedelta(minutes=1)


def minute_text(index: int) -> str:
    """The minute ``index`` as ISO 8601 UTC text, such as ``2026-11-10T22:31:00Z``."""
    return (_EPOCH + timedelta(minutes=index)).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Check:
    """One indexer's outcome on one chain in a pushed minute."""

    indexer: str
    chain: str
    outcome: OutcomeKind
    reason: Optional[str] = None
    detail: Optional[str] = None
    deployment: Optional[str] = None
    age_seconds: Optional[float] = None


class LastCheck(BaseModel):
    """The latest outcome recorded for an indexer on a chain, with what the prober said."""

    minute: int
    outcome: OutcomeKind
    reason: Optional[str] = None
    detail: Optional[str] = None
    deployment: Optional[str] = None
    age_seconds: Optional[float] = None


class DayCount(BaseModel):
    """Checks passed and counted on 1 UTC day."""

    passed: int = Field(default=0, ge=0)
    counted: int = Field(default=0, ge=0)


class PairState(BaseModel):
    """What is kept for 1 indexer on 1 chain."""

    # A pair starts marked, so an indexer has to pass checks before it can win agreements.
    marked: bool = True
    # Set by the pair's first pass or fail. Until then its no_evidence minutes aren't counted.
    counting: bool = False
    # Outcome by minute, for the minutes in the current window only.
    minutes: dict[int, OutcomeKind] = Field(default_factory=dict)
    # Counts by UTC day, for the last HISTORY_DAYS days only.
    days: dict[int, DayCount] = Field(default_factory=dict)
    last: LastCheck


class FreshnessState(BaseModel):
    """Everything the service keeps about freshness, in the shape of the file it is saved to."""

    version: Literal[1] = 1
    last_minute: Optional[int] = None
    # The minutes in the current window that the service received a push for.
    received: set[int] = Field(default_factory=set)
    # Chain, then indexer.
    chains: dict[str, dict[str, PairState]] = Field(default_factory=dict)


def _window(pair: PairState, received: set[int], last_minute: int) -> tuple[int, int]:
    """(passed, counted) over the WINDOW_MINUTES clock minutes ending at ``last_minute``.

    A minute the service never received is void, and so is a void outcome. A received minute
    with no outcome for the pair counts as not passed, so silence never clears a mark.
    """
    passed = counted = 0
    for minute in range(last_minute - WINDOW_MINUTES + 1, last_minute + 1):
        outcome = pair.minutes.get(minute)
        if minute not in received or outcome == "void":
            continue
        counted += 1
        if outcome == "pass":
            passed += 1
    return passed, counted


def _count(pair: PairState, day: int, outcome: OutcomeKind) -> None:
    """Add 1 outcome to the pair's daily counts, once counting has started for it."""
    if outcome in ("pass", "fail"):
        pair.counting = True
    if outcome == "void" or not pair.counting:
        return
    counts = pair.days.setdefault(day, DayCount())
    counts.counted += 1
    if outcome == "pass":
        counts.passed += 1


def _record(state: FreshnessState, minute: int, check: Check) -> None:
    """Store 1 check's outcome for its pair, creating the pair the first time it is seen."""
    last = LastCheck(
        minute=minute,
        outcome=check.outcome,
        reason=check.reason,
        detail=check.detail,
        deployment=check.deployment,
        age_seconds=check.age_seconds,
    )
    pairs = state.chains.setdefault(check.chain, {})
    pair = pairs.get(check.indexer)
    if pair is None:
        pair = pairs[check.indexer] = PairState(last=last)
    else:
        pair.last = last
    pair.minutes[minute] = check.outcome
    _count(pair, minute // MINUTES_PER_DAY, check.outcome)


def _prune(state: FreshnessState, last_minute: int) -> None:
    """Forget minutes outside the window, days older than HISTORY_DAYS, and pairs with no
    outcome in HISTORY_DAYS.
    """
    window_start = last_minute - WINDOW_MINUTES + 1
    forget_before = last_minute - HISTORY_DAYS * MINUTES_PER_DAY
    state.received = {minute for minute in state.received if minute >= window_start}
    for chain, pairs in list(state.chains.items()):
        kept = {ix: pair for ix, pair in pairs.items() if pair.last.minute > forget_before}
        for pair in kept.values():
            _trim(pair, last_minute)
        if kept:
            state.chains[chain] = kept
        else:
            del state.chains[chain]


def _trim(pair: PairState, last_minute: int) -> None:
    """Forget a pair's minutes outside the window and its days older than HISTORY_DAYS."""
    window_start = last_minute - WINDOW_MINUTES + 1
    oldest_day = last_minute // MINUTES_PER_DAY - HISTORY_DAYS + 1
    pair.minutes = {m: o for m, o in pair.minutes.items() if m >= window_start}
    pair.days = {day: counts for day, counts in pair.days.items() if day >= oldest_day}


def _update_marks(state: FreshnessState, last_minute: int) -> None:
    """Judge every pair on record over the window. With too few counted minutes a pair keeps
    the mark it had.
    """
    for pairs in state.chains.values():
        for pair in pairs.values():
            passed, counted = _window(pair, state.received, last_minute)
            if counted >= MIN_COUNTED_MINUTES:
                pair.marked = passed * 2 < counted


def _report(pair: PairState, received: set[int], last_minute: int) -> dict[str, Any]:
    """1 pair as GET /freshness reports it."""
    passed, counted = _window(pair, received, last_minute)
    passed_28d = sum(day.passed for day in pair.days.values())
    counted_28d = sum(day.counted for day in pair.days.values())
    return {
        "marked": pair.marked,
        "judged": counted >= MIN_COUNTED_MINUTES,
        "window_passed": passed,
        "window_counted": counted,
        "last": {**pair.last.model_dump(), "minute": minute_text(pair.last.minute)},
        "passed_28d": passed_28d,
        "counted_28d": counted_28d,
        "share_28d": passed_28d / counted_28d if counted_28d else None,
    }


class FreshnessStore:
    """The freshness state behind a lock, since pushes and reads run on different threads."""

    def __init__(self, state: Optional[FreshnessState] = None) -> None:
        self._state = state if state is not None else FreshnessState()
        self._lock = threading.Lock()

    @property
    def last_minute(self) -> Optional[int]:
        """The last minute applied, or None before the first push."""
        with self._lock:
            return self._state.last_minute

    def apply(
        self, minute: int, checks: Sequence[Check], save: Callable[[dict[str, Any]], None]
    ) -> bool:
        """Record 1 pushed minute, judge every pair again, then hand the state to ``save``.

        ``checks`` holds at most 1 check per indexer and chain. A minute at or before the last
        applied one changes nothing and gives False, so a retried push can't count twice. If
        ``save`` raises, the minute stays applied in memory and the next save writes it out.
        """
        with self._lock:
            state = self._state
            if state.last_minute is not None and minute <= state.last_minute:
                return False
            for check in checks:
                _record(state, minute, check)
            state.last_minute = minute
            state.received.add(minute)
            _prune(state, minute)
            _update_marks(state, minute)
            save(state.model_dump(mode="json"))
            return True

    def stale_indexers(self, chain: str, indexers: Iterable[str]) -> set[str]:
        """The ``indexers`` that are marked on ``chain`` or have no record there, since an
        indexer starts marked until it passes checks.
        """
        with self._lock:
            pairs = self._state.chains.get(chain, {})
            return {ix for ix in indexers if (pair := pairs.get(ix.lower())) is None or pair.marked}

    def snapshot(self, enforced: frozenset[str] = frozenset()) -> dict[str, Any]:
        """Every pair's mark, window, latest outcome and 28-day counts, by chain, with whether
        each chain is in ``enforced``. An enforced chain is listed even with no indexers.
        """
        with self._lock:
            state = self._state
            last_minute = state.last_minute
            reports: dict[str, dict[str, Any]] = {chain: {} for chain in sorted(enforced)}
            if last_minute is not None:
                for chain, pairs in state.chains.items():
                    reports[chain] = {
                        indexer: _report(pair, state.received, last_minute)
                        for indexer, pair in pairs.items()
                    }
            return {
                "last_minute": minute_text(last_minute) if last_minute is not None else None,
                "chains": {
                    chain: {"enforced": chain in enforced, "indexers": indexers}
                    for chain, indexers in reports.items()
                },
            }


def load_state(path: str) -> FreshnessState:
    """Read the saved state from ``path``. A missing file gives empty state, and so does an
    unreadable or malformed one, after a warning saying why.
    """
    try:
        raw = Path(path).read_bytes()
    except FileNotFoundError:
        logger.debug("No freshness file at %s; starting with no freshness history", path)
        return FreshnessState()
    except OSError as e:
        logger.warning("Cannot read freshness file %s (%s); starting with no history", path, e)
        return FreshnessState()
    try:
        return FreshnessState.model_validate_json(raw)
    except ValidationError as e:
        first = e.errors()[0]
        logger.warning(
            "Freshness file %s is malformed (%d errors, first at %s: %s); starting with no history",
            path,
            e.error_count(),
            ".".join(str(part) for part in first["loc"]) or "top level",
            first["msg"],
        )
        return FreshnessState()
