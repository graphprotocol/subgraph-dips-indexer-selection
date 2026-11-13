"""Freshness: whether each indexer served up-to-date data on each chain, minute by minute.

A prober checks every indexer on every chain it offers DIPs on once a minute and pushes that
minute's outcomes to the service. From them this module keeps, for each indexer and chain, a
stale mark worked out from the last 10 minutes and from its latest counted checks, and daily
counts of passed and counted checks
for the last 28 days, from which it scores the indexer's freshness on the chain. Minutes and
days are counted from the Unix epoch in UTC, so a day here is a UTC day.
"""

import logging
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional, Self

from pydantic import BaseModel, Field, ValidationError, model_validator

from .share_score import shaped_share

__all__ = [
    "Check",
    "Enforcement",
    "FreshnessState",
    "FreshnessStore",
    "freshness_score",
    "load_state",
    "minute_index",
    "minute_text",
]

logger = logging.getLogger(__name__)

OutcomeKind = Literal["pass", "fail", "no_evidence", "void"]

WINDOW_MINUTES = 10
MIN_COUNTED_MINUTES = 5
# A pair whose window holds too few counted minutes is judged on its last MIN_COUNTED_MINUTES
# counted outcomes instead, while the oldest is no more than this many of its chain's answered
# minutes old, and is marked once it has too few that recent: with no evidence it is fresh, it
# isn't picked. An answered minute on a chain is 1 in which some check there passed or failed,
# so went through the gateway; an outage that voids every check, or leaves only the no_evidence
# ones the prober works out alone, ages nobody.
RECENT_ANSWERED_MINUTES = 60
HISTORY_DAYS = 28
MINUTES_PER_DAY = 1_440
# Freshness is weighed on a chain only once it has this many minutes of checks, 7 days, and an
# indexer's share of checks passed is taken over at least this many, so a newcomer can't score
# 1 on a good hour.
MIN_SCORED_MINUTES = 7 * MINUTES_PER_DAY

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


class RecentOutcome(BaseModel):
    """1 counted outcome of a pair, with how many answered minutes its chain had by then."""

    passed: bool
    answered: int = Field(ge=0)


class PairState(BaseModel):
    """What is kept for 1 indexer on 1 chain."""

    # A pair starts marked, so an indexer has to pass checks before it can win agreements.
    marked: bool = True
    # The minute of the pair's first recorded outcome.
    first_minute: int
    # Set by the pair's first pass or fail. Until then its no_evidence minutes aren't counted.
    counting: bool = False
    # Outcome by minute, for the minutes in the current window only.
    minutes: dict[int, OutcomeKind] = Field(default_factory=dict)
    # Counts by UTC day, for the last HISTORY_DAYS days only.
    days: dict[int, DayCount] = Field(default_factory=dict)
    last: LastCheck
    # The minute of the pair's last outcome that counted, meaning any but void, or None before
    # it has one.
    last_counted: Optional[int] = None
    # The pair's last MIN_COUNTED_MINUTES counted outcomes, oldest first.
    recent: list[RecentOutcome] = Field(default_factory=list)

    @model_validator(mode="after")
    def _fill_in_for_an_older_file(self) -> Self:
        """A pair saved before last_counted and recent were kept gets them from the counted
        minutes in its window, each taken as answered at 0, the count every chain starts from
        when loaded from such a file. A pair that was judged fine then stays fine.
        """
        counted = sorted((m, outcome) for m, outcome in self.minutes.items() if outcome != "void")
        if "recent" not in self.model_fields_set:
            self.recent = [
                RecentOutcome(passed=outcome == "pass", answered=0)
                for _, outcome in counted[-MIN_COUNTED_MINUTES:]
            ]
        if "last_counted" not in self.model_fields_set and counted:
            self.last_counted = counted[-1][0]
        return self


class FreshnessState(BaseModel):
    """Everything the service keeps about freshness, in the shape of the file it is saved to."""

    version: Literal[1] = 1
    last_minute: Optional[int] = None
    # The minutes in the current window that the service received a push for.
    received: set[int] = Field(default_factory=set)
    # Chain, then indexer.
    chains: dict[str, dict[str, PairState]] = Field(default_factory=dict)
    # By chain, the last minute holding an outcome there that counted, meaning any but void.
    last_counted: dict[str, int] = Field(default_factory=dict)
    # By chain, how many answered minutes it has had: minutes in which some check there passed or
    # failed. A file saved before this was kept starts every chain from 0.
    answered: dict[str, int] = Field(default_factory=dict)


def _window(pair: PairState, received: set[int], last_minute: int) -> tuple[int, int]:
    """(passed, counted) over the WINDOW_MINUTES clock minutes ending at ``last_minute``.

    A minute the service never received is void, and so is a void outcome. A received minute
    with no outcome for the pair counts as not passed before the pair's first outcome, so a
    newcomer earns its place, and is void after it, so a short push marks nobody it leaves out.
    """
    passed = counted = 0
    for minute in range(last_minute - WINDOW_MINUTES + 1, last_minute + 1):
        outcome = pair.minutes.get(minute)
        if minute not in received or outcome == "void":
            continue
        if outcome is None and minute >= pair.first_minute:
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
        pair = pairs[check.indexer] = PairState(first_minute=minute, last=last)
    else:
        pair.last = last
    pair.minutes[minute] = check.outcome
    _count(pair, minute // MINUTES_PER_DAY, check.outcome)
    if check.outcome != "void":
        pair.last_counted = minute
        outcome = RecentOutcome(
            passed=check.outcome == "pass", answered=state.answered.get(check.chain, 0)
        )
        pair.recent = [*pair.recent, outcome][-MIN_COUNTED_MINUTES:]
        state.last_counted[check.chain] = minute


def _prune(state: FreshnessState, last_minute: int) -> None:
    """Forget minutes outside the window, days older than HISTORY_DAYS, and pairs with no
    outcome in HISTORY_DAYS, along with any chain left with no pairs.
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
            state.last_counted.pop(chain, None)
            state.answered.pop(chain, None)


def _trim(pair: PairState, last_minute: int) -> None:
    """Forget a pair's minutes outside the window and its days older than HISTORY_DAYS."""
    window_start = last_minute - WINDOW_MINUTES + 1
    oldest_day = last_minute // MINUTES_PER_DAY - HISTORY_DAYS + 1
    pair.minutes = {m: o for m, o in pair.minutes.items() if m >= window_start}
    pair.days = {day: counts for day, counts in pair.days.items() if day >= oldest_day}


def _answered_chains(checks: Iterable[Check]) -> set[str]:
    """The chains on which some check in a minute passed or failed, so went through the gateway."""
    return {check.chain for check in checks if check.outcome in ("pass", "fail")}


def _has_counted(pair: PairState) -> bool:
    """Whether any of the pair's checks has ever counted, as its daily counts show for a pair
    loaded from a file saved before recent outcomes were kept."""
    return bool(pair.recent) or any(day.counted for day in pair.days.values())


def _recent(pair: PairState, answered: int) -> list[RecentOutcome]:
    """The pair's recent outcomes no more than RECENT_ANSWERED_MINUTES answered minutes old,
    ``answered`` being its chain's count now."""
    return [o for o in pair.recent if answered - o.answered <= RECENT_ANSWERED_MINUTES]


def _recent_mark(pair: PairState, answered: int) -> bool:
    """The mark from the pair's last MIN_COUNTED_MINUTES counted outcomes: unmarked when all of
    them are recent and at least half passed, and marked otherwise."""
    recent = _recent(pair, answered)
    if len(recent) < MIN_COUNTED_MINUTES:
        return True
    return sum(outcome.passed for outcome in recent) * 2 < len(recent)


def _update_marks(state: FreshnessState, last_minute: int, answered: Iterable[str]) -> None:
    """Judge every pair on each chain answered in the minute just applied. On any other chain
    every mark stays as it was, as nothing there went through the gateway. A pair with too few
    counted minutes in its window is judged on its recent outcomes instead (_recent_mark); a
    pair that never counted keeps the mark it started with.
    """
    for chain in answered:
        for pair in state.chains.get(chain, {}).values():
            passed, counted = _window(pair, state.received, last_minute)
            if counted >= MIN_COUNTED_MINUTES:
                pair.marked = passed * 2 < counted
            elif _has_counted(pair):
                pair.marked = _recent_mark(pair, state.answered[chain])


def _report(pair: PairState, received: set[int], last_minute: int, answered: int) -> dict[str, Any]:
    """1 pair as GET /freshness reports it, ``answered`` being its chain's answered count."""
    passed, counted = _window(pair, received, last_minute)
    recent = _recent(pair, answered)
    passed_28d = sum(day.passed for day in pair.days.values())
    counted_28d = sum(day.counted for day in pair.days.values())
    return {
        "marked": pair.marked,
        "judged": counted >= MIN_COUNTED_MINUTES,
        "window_passed": passed,
        "window_counted": counted,
        "last": {**pair.last.model_dump(), "minute": minute_text(pair.last.minute)},
        "last_counted_minute": (
            minute_text(pair.last_counted) if pair.last_counted is not None else None
        ),
        "recent_passed": sum(outcome.passed for outcome in recent),
        "recent_counted": len(recent),
        "passed_28d": passed_28d,
        "counted_28d": counted_28d,
        "share_28d": passed_28d / counted_28d if counted_28d else None,
    }


@dataclass(frozen=True)
class Enforcement:
    """The chains exempt from freshness, whose marks are only recorded and reported, how many
    minutes a chain's last counted check may be behind ``now`` for its marks to still decide
    selection, and ``now``, the service's current minute.
    """

    exempt: frozenset[str]
    max_age_minutes: int
    now: int

    def enforced(self, chain: str) -> bool:
        """Whether the marks on ``chain`` are meant to decide selection: on every chain that
        isn't exempt.
        """
        return chain not in self.exempt

    def current(self, last_counted: Optional[int]) -> bool:
        """Whether a chain whose last counted check was in the minute ``last_counted`` has
        checks recent enough for its marks to be trusted.
        """
        return last_counted is not None and self.now - last_counted <= self.max_age_minutes


def _chain_report(
    chain: str,
    indexers: dict[str, Any],
    last_counted: Optional[int],
    enforcement: Optional[Enforcement],
) -> dict[str, Any]:
    """1 chain as GET /freshness reports it. Without ``enforcement`` nothing is enforced."""
    enforced = enforcing = False
    if enforcement is not None:
        enforced = enforcement.enforced(chain)
        enforcing = enforced and enforcement.current(last_counted)
    return {
        "enforced": enforced,
        "enforcing": enforcing,
        "last_counted_minute": minute_text(last_counted) if last_counted is not None else None,
        "indexers": indexers,
    }


def freshness_score(passed: int, counted: int) -> float:
    """An indexer's freshness score on a chain from its checks passed and counted over the
    last HISTORY_DAYS: its share passed, over at least MIN_SCORED_MINUTES checks, shaped.
    """
    return shaped_share(passed / max(counted, MIN_SCORED_MINUTES))


def _pair_score(pair: PairState) -> float:
    days = pair.days.values()
    return freshness_score(sum(day.passed for day in days), sum(day.counted for day in days))


def _scores_of(state: FreshnessState) -> dict[str, dict[str, float]]:
    """By chain, each indexer's freshness score there, for the chains whose earliest pair was
    first seen at least MIN_SCORED_MINUTES before the last minute applied.
    """
    last_minute = state.last_minute
    if last_minute is None:
        return {}
    return {
        chain: {indexer: _pair_score(pair) for indexer, pair in pairs.items()}
        for chain, pairs in state.chains.items()
        if pairs
        and last_minute - min(pair.first_minute for pair in pairs.values()) >= MIN_SCORED_MINUTES
    }


@dataclass(frozen=True)
class _Marks:
    """The last minute applied, by chain the indexers not marked there, by chain the last
    minute a check there counted, and by chain each indexer's freshness score on the chains
    scored. Rebuilt after each push and swapped in with 1 assignment, so selection reads it
    without the lock.
    """

    last_minute: Optional[int]
    unmarked: Mapping[str, frozenset[str]]
    last_counted: Mapping[str, int]
    scores: Mapping[str, Mapping[str, float]]


def _marks_of(state: FreshnessState) -> _Marks:
    unmarked = {
        chain: frozenset(indexer for indexer, pair in pairs.items() if not pair.marked)
        for chain, pairs in state.chains.items()
    }
    return _Marks(state.last_minute, unmarked, dict(state.last_counted), _scores_of(state))


class FreshnessStore:
    """The freshness state behind a lock, since pushes and reads run on different threads.
    Selection reads only the marks swapped in after each push, so it never waits for a push
    to be recorded and saved.
    """

    def __init__(self, state: Optional[FreshnessState] = None) -> None:
        self._state = state if state is not None else FreshnessState()
        self._lock = threading.Lock()
        self._marks = _marks_of(self._state)

    @property
    def last_minute(self) -> Optional[int]:
        """The last minute applied, or None before the first push."""
        return self._marks.last_minute

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
            answered = _answered_chains(checks)
            for chain in answered:
                state.answered[chain] = state.answered.get(chain, 0) + 1
            for check in checks:
                _record(state, minute, check)
            state.last_minute = minute
            state.received.add(minute)
            _prune(state, minute)
            _update_marks(state, minute, answered)
            self._marks = _marks_of(state)
            save(state.model_dump(mode="json"))
            return True

    def stale_indexers(self, chain: str, indexers: Iterable[str]) -> set[str]:
        """The ``indexers`` that are marked on ``chain`` or have no record there, since an
        indexer starts marked until it passes checks.
        """
        unmarked = self._marks.unmarked.get(chain, frozenset())
        return {indexer for indexer in indexers if indexer.lower() not in unmarked}

    def scores_on(self, chain: str) -> Optional[Mapping[str, float]]:
        """Each indexer's freshness score on ``chain``, by address in lower case, or None when
        the chain has fewer than MIN_SCORED_MINUTES of checks or none, so freshness isn't
        weighed there. An indexer missing from the answer has no record on the chain.
        """
        return self._marks.scores.get(chain)

    def last_counted_minute(self, chain: str) -> Optional[int]:
        """The last minute in which a check on ``chain`` counted, or None when none has in
        HISTORY_DAYS.
        """
        return self._marks.last_counted.get(chain)

    def snapshot(self, enforcement: Optional[Enforcement] = None) -> dict[str, Any]:
        """Every pair's mark, window, latest outcome and 28-day counts, by chain, with whether
        each chain is enforced and enforcing under ``enforcement`` and the last minute a check
        on it counted. An exempt chain is listed even with no indexers.
        """
        exempt = enforcement.exempt if enforcement is not None else frozenset()
        with self._lock:
            state = self._state
            last_minute = state.last_minute
            reports: dict[str, dict[str, Any]] = {chain: {} for chain in sorted(exempt)}
            if last_minute is not None:
                for chain, pairs in state.chains.items():
                    reports[chain] = {
                        indexer: _report(
                            pair, state.received, last_minute, state.answered.get(chain, 0)
                        )
                        for indexer, pair in pairs.items()
                    }
            return {
                "last_minute": minute_text(last_minute) if last_minute is not None else None,
                "chains": {
                    chain: _chain_report(
                        chain, indexers, state.last_counted.get(chain), enforcement
                    )
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
