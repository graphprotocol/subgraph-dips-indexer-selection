"""Settings for the freshness prober, read from the environment when the fetcher starts.

The prober is off unless FRESHNESS_PROBE_ENABLED is true. Switched on, it spends query fees on
FRESHNESS_GATEWAY_API_KEY, so a missing or unusable setting stops the fetcher from starting
rather than leaving the prober half working.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Mapping, Optional

from freshness_rules import is_chain_name
from subgraph import subgraph_url_problem

# Each minute's outcomes are pushed this long after the minute ends, so every check, sent at
# most 60 seconds into the minute, has to finish within it.
PUSH_DELAY_SECONDS = 30

_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_TRUE = ("true", "1", "yes")
_FALSE = ("false", "0", "no", "")

# The Graph's networks registry, at the 2 addresses dipper reads it from, in that order.
DEFAULT_NETWORKS_REGISTRY_URL = (
    "https://networks-registry.thegraph.com/TheGraphNetworksRegistry_v0_7_x.json"
)
FALLBACK_NETWORKS_REGISTRY_URL = (
    "https://raw.githubusercontent.com/graphprotocol/networks-registry/refs/heads/main/public/"
    "TheGraphNetworksRegistry_v0_7_x.json"
)


@dataclass(frozen=True)
class FreshnessConfig:
    gateway_url: str
    api_key: str = field(repr=False)
    bar_seconds: float = 36.0
    min_allocation_age_days: float = 7.0
    refresh_interval: float = 900.0
    request_timeout: float = 25.0
    # Only chains named here are checked: the registry's network ids plus these extra names.
    networks_registry_url: str = DEFAULT_NETWORKS_REGISTRY_URL
    additional_networks: tuple[str, ...] = ()
    max_concurrency: int = 50
    # The addresses the gateway pays indexers from, whose escrow decides whose fault a refused
    # payment is.
    gateway_payers: tuple[str, ...] = ()
    # When set, only the payers' escrow accounts under this collector count.
    gateway_collector: Optional[str] = None

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


def _payers(environ: Mapping[str, str]) -> tuple[str, ...]:
    entries = [entry.strip() for entry in environ.get("FRESHNESS_GATEWAY_PAYERS", "").split(",")]
    payers = [entry for entry in entries if entry]
    if not payers:
        raise _ConfigProblem("FRESHNESS_GATEWAY_PAYERS is required")
    if not all(_ADDRESS.match(payer) for payer in payers):
        raise _ConfigProblem("FRESHNESS_GATEWAY_PAYERS must be 0x addresses separated by commas")
    return tuple(dict.fromkeys(payer.lower() for payer in payers))


def _collector(environ: Mapping[str, str]) -> Optional[str]:
    collector = environ.get("FRESHNESS_GATEWAY_COLLECTOR", "").strip()
    if not collector:
        return None
    if not _ADDRESS.match(collector):
        raise _ConfigProblem("FRESHNESS_GATEWAY_COLLECTOR must be a 0x address")
    return collector.lower()


def _enabled(environ: Mapping[str, str]) -> bool:
    raw = environ.get("FRESHNESS_PROBE_ENABLED", "").strip().lower()
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    raise _ConfigProblem("FRESHNESS_PROBE_ENABLED must be true or false")


def _registry_url(environ: Mapping[str, str]) -> str:
    url = environ.get("FRESHNESS_NETWORKS_REGISTRY_URL", "").strip()
    url_problem = subgraph_url_problem(url) if url else None
    if url_problem:
        raise _ConfigProblem(f"FRESHNESS_NETWORKS_REGISTRY_URL {url_problem}")
    return url or DEFAULT_NETWORKS_REGISTRY_URL


def _additional_networks(environ: Mapping[str, str]) -> tuple[str, ...]:
    entries = environ.get("FRESHNESS_ADDITIONAL_NETWORKS", "").split(",")
    names = [entry.strip() for entry in entries if entry.strip()]
    if not all(is_chain_name(name) for name in names):
        raise _ConfigProblem(
            "FRESHNESS_ADDITIONAL_NETWORKS must be chain names of 1 to 64 printable characters "
            "without spaces, separated by commas"
        )
    return tuple(dict.fromkeys(names))


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
        networks_registry_url=_registry_url(environ),
        additional_networks=_additional_networks(environ),
        max_concurrency=int(_number(environ, "FRESHNESS_MAX_CONCURRENCY", 50, at_least=1)),
        gateway_payers=_payers(environ),
        gateway_collector=_collector(environ),
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
