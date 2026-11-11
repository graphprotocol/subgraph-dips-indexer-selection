"""The rules POST /freshness checks each outcome against, applied just before every push.

The service refuses the whole push when a single outcome breaks one of them (FreshnessOutcome in
the service), so the prober leaves such an outcome out instead and the rest of the minute still
counts. The prober builds outcomes that pass already; this is the last line of defence.
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any, Callable, Mapping, Optional, Sequence

from escaping import for_log
from freshness_rules import DETAIL_LIMIT, FAIL, MAX_PAIRS, NO_EVIDENCE, PASS, VOID, CheckResult

logger = logging.getLogger(__name__)

FIELDS = frozenset({"indexer", "chain", "outcome", "reason", "detail", "deployment", "age_seconds"})
_OUTCOMES = (PASS, FAIL, NO_EVIDENCE, VOID)
_INDEXER = re.compile(r"0x[0-9a-fA-F]{40}")
_REASON = re.compile(r"[a-z][a-z0-9_]{0,63}")


def _is_text(value: object, shortest: int, longest: int) -> bool:
    if not isinstance(value, str) or not shortest <= len(value) <= longest:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _optional(check: Callable[[object], bool]) -> Callable[[object], bool]:
    return lambda value: value is None or check(value)


def _is_age(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        # An int too big for a float, which the service can't hold as a float either.
        return False


_RULES: tuple[tuple[str, Callable[[object], bool]], ...] = (
    ("indexer", lambda v: isinstance(v, str) and bool(_INDEXER.fullmatch(v))),
    ("chain", lambda v: _is_text(v, 1, 64)),
    ("outcome", lambda v: v in _OUTCOMES),
    ("reason", _optional(lambda v: isinstance(v, str) and bool(_REASON.fullmatch(v)))),
    ("detail", _optional(lambda v: _is_text(v, 0, DETAIL_LIMIT))),
    ("deployment", _optional(lambda v: _is_text(v, 0, 64))),
    ("age_seconds", _optional(_is_age)),
)


def refusal(outcome: Mapping[str, Any]) -> Optional[str]:
    """Why the service would refuse this 1 outcome, or None when it would accept it."""
    if set(outcome) != FIELDS:
        return f"fields are {sorted(outcome)}, not {sorted(FIELDS)}"
    for field, accepts in _RULES:
        if not accepts(outcome[field]):
            return f"{field} breaks the service's rules"
    return None


def refusals(outcomes: Sequence[Mapping[str, Any]]) -> list[Optional[str]]:
    """For each of a minute's outcomes, in order, why the service would refuse it, or None when
    it would accept it. Past the first outcome for an (indexer, chain) and past MAX_PAIRS
    accepted outcomes, the rest are refused too."""
    problems: list[Optional[str]] = []
    accepted: set[tuple[str, str]] = set()
    for outcome in outcomes:
        problem = refusal(outcome)
        pair = (str(outcome.get("indexer")).lower(), str(outcome.get("chain")))
        if problem is None and pair in accepted:
            problem = "a second outcome for the same indexer and chain"
        if problem is None and len(accepted) >= MAX_PAIRS:
            problem = f"more than {MAX_PAIRS:,} outcomes in 1 minute"
        if problem is None:
            accepted.add(pair)
        problems.append(problem)
    return problems


def without_refused(minute: str, results: Sequence[CheckResult]) -> list[CheckResult]:
    """``results`` without the outcomes the service would refuse, logged as 1 error with the
    count and 1 example."""
    problems = refusals([result.to_payload() for result in results])
    refused = [(result, problem) for result, problem in zip(results, problems) if problem]
    if refused:
        result, problem = refused[0]
        logger.error(
            "Leaving %d freshness outcomes the service would refuse out of minute %s, "
            "such as indexer %s on chain %s: %s",
            len(refused),
            minute,
            for_log(result.indexer),
            for_log(result.chain),
            problem,
        )
    return [result for result, problem in zip(results, problems) if problem is None]
