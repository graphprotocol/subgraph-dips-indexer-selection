"""Settings for the freshness prober, read from the environment when the fetcher starts.

The prober is off unless FRESHNESS_PROBE_ENABLED is true. Switched on, it spends query fees on
FRESHNESS_GATEWAY_API_KEY, so a missing or unusable setting stops the fetcher from starting
rather than leaving the prober half working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Optional

from subgraph import subgraph_url_problem

# Each minute's outcomes are pushed this long after the minute ends, so every check, sent at
# most 60 seconds into the minute, has to finish within it.
PUSH_DELAY_SECONDS = 30

_TRUE = ("true", "1", "yes")
_FALSE = ("false", "0", "no", "")


@dataclass(frozen=True)
class FreshnessConfig:
    gateway_url: str
    api_key: str = field(repr=False)
    bar_seconds: float = 36.0
    min_allocation_age_days: float = 7.0
    refresh_interval: float = 900.0
    request_timeout: float = 25.0
    max_concurrency: int = 50

    @property
    def min_allocation_age_seconds(self) -> float:
        return self.min_allocation_age_days * 86_400


class _ConfigProblem(ValueError):
    pass


def _number(environ: Mapping[str, str], name: str, default: float, *, at_least: float) -> float:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise _ConfigProblem(f"{name} must be a number") from None
    if not value >= at_least or value == float("inf"):
        raise _ConfigProblem(f"{name} must be a finite number of at least {at_least:g}")
    return value


def _enabled(environ: Mapping[str, str]) -> bool:
    raw = environ.get("FRESHNESS_PROBE_ENABLED", "").strip().lower()
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    raise _ConfigProblem("FRESHNESS_PROBE_ENABLED must be true or false")


def _read(environ: Mapping[str, str]) -> FreshnessConfig:
    gateway_url = environ.get("FRESHNESS_GATEWAY_URL", "").strip()
    url_problem = subgraph_url_problem(gateway_url)
    if url_problem:
        raise _ConfigProblem(f"FRESHNESS_GATEWAY_URL {url_problem}")
    api_key = environ.get("FRESHNESS_GATEWAY_API_KEY", "").strip()
    if not api_key:
        raise _ConfigProblem("FRESHNESS_GATEWAY_API_KEY is required")

    request_timeout = _number(environ, "FRESHNESS_REQUEST_TIMEOUT", 25.0, at_least=1)
    if request_timeout >= PUSH_DELAY_SECONDS:
        raise _ConfigProblem(
            f"FRESHNESS_REQUEST_TIMEOUT must be under {PUSH_DELAY_SECONDS} seconds, the wait "
            "before each minute is pushed"
        )
    return FreshnessConfig(
        gateway_url=gateway_url.rstrip("/"),
        api_key=api_key,
        bar_seconds=_number(environ, "FRESHNESS_BAR_SECONDS", 36.0, at_least=1),
        min_allocation_age_days=_number(
            environ, "FRESHNESS_MIN_ALLOCATION_AGE_DAYS", 7.0, at_least=0
        ),
        refresh_interval=_number(environ, "FRESHNESS_REFRESH_INTERVAL", 900.0, at_least=60),
        request_timeout=request_timeout,
        max_concurrency=int(_number(environ, "FRESHNESS_MAX_CONCURRENCY", 50, at_least=1)),
    )


def load_freshness_config(
    environ: Mapping[str, str],
) -> tuple[Optional[FreshnessConfig], Optional[str]]:
    """Return (config, None) when the prober is on, (None, None) when it is off, and
    (None, problem) when it can't run. The problem never repeats the API key or the URL."""
    try:
        if not _enabled(environ):
            return None, None
        return _read(environ), None
    except _ConfigProblem as problem:
        return None, str(problem)
