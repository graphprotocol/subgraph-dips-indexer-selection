"""Which chain names are worth checking: the network ids in The Graph's networks registry, plus
the names in FRESHNESS_ADDITIONAL_NETWORKS for test chains the registry lacks.

Dipper resolves every chain it asks IISA about to 1 of those, so no other chain can ever be
selected. Indexers choose the chain names they list, so checking only these also keeps made-up
names from crowding out the chains that matter.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests
from freshness_config import DEFAULT_NETWORKS_REGISTRY_URL, FALLBACK_NETWORKS_REGISTRY_URL
from freshness_rules import is_chain_name

logger = logging.getLogger(__name__)

# The file changes rarely, so a copy that loaded is used this long before it is read again.
RELOAD_SECONDS = 6 * 3600
FETCH_TIMEOUT_SECONDS = 10


class RegistryUnavailable(ValueError):
    """No copy of the networks registry has loaded yet, so no chain is known to be real."""


def fetch_registry(url: str) -> Any:
    """The registry file at ``url``, read once with a short timeout."""
    response = requests.get(url, timeout=FETCH_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def registry_ids(document: object) -> frozenset[str]:
    """The usable network ids in a registry file. Raises ValueError when there are none."""
    networks = document.get("networks") if isinstance(document, dict) else None
    if not isinstance(networks, list):
        raise ValueError("the registry file has no list of networks")
    ids = frozenset(
        entry["id"]
        for entry in networks
        if isinstance(entry, dict) and is_chain_name(entry.get("id"))
    )
    if not ids:
        raise ValueError("the registry file has no usable network ids")
    return ids


@dataclass(frozen=True)
class _Copy:
    networks: frozenset[str]
    registry_ids: int
    loaded_at: float


class NetworksRegistry:
    """The chain names worth checking, from a copy of the registry read at most RELOAD_SECONDS
    ago, or from the last copy that loaded while the registry can't be read."""

    def __init__(
        self,
        url: str,
        additional: Iterable[str] = (),
        *,
        fetch: Callable[[str], Any] = fetch_registry,
        clock: Callable[[], float] = time.time,
    ) -> None:
        # Like dipper, fall back to the copy on GitHub only for the default address.
        self._urls = (
            (url, FALLBACK_NETWORKS_REGISTRY_URL)
            if url == DEFAULT_NETWORKS_REGISTRY_URL
            else (url,)
        )
        self._additional = frozenset(additional)
        self._fetch = fetch
        self._clock = clock
        self._copy: Optional[_Copy] = None

    def networks(self) -> frozenset[str]:
        """The chain names worth checking, reading the registry first when no copy has loaded or
        the last one is RELOAD_SECONDS old. Raises RegistryUnavailable when no copy ever loaded.
        """
        copy = self._copy
        if copy is None or self._clock() - copy.loaded_at >= RELOAD_SECONDS:
            copy = self._reload(copy)
        return copy.networks

    def health(self) -> dict[str, Any]:
        """Fields for the fetcher's /health body."""
        copy = self._copy
        loaded_at = datetime.fromtimestamp(copy.loaded_at, timezone.utc) if copy else None
        return {
            "networks_registry_loaded_at": loaded_at.isoformat() if loaded_at else None,
            "networks_registry_networks": copy.registry_ids if copy else None,
        }

    def _reload(self, previous: Optional[_Copy]) -> _Copy:
        try:
            ids = self._read()
        except ValueError as e:
            if previous is None:
                raise RegistryUnavailable(
                    f"no copy of the networks registry has loaded yet: {e}"
                ) from e
            logger.warning(
                "Couldn't read the networks registry, so the copy loaded %.0fs ago is used "
                "until the next refresh: %s",
                self._clock() - previous.loaded_at,
                e,
            )
            return previous
        self._copy = _Copy(ids | self._additional, len(ids), self._clock())
        logger.info("Loaded the networks registry: %d networks", len(ids))
        return self._copy

    def _read(self) -> frozenset[str]:
        problems = []
        for url in self._urls:
            try:
                return registry_ids(self._fetch(url))
            except (requests.RequestException, ValueError) as e:
                problems.append(f"{url}: {type(e).__name__}: {e}")
        raise ValueError("; ".join(problems))
