"""
Loads indexer sync status from the IISA service's own cache file.

A fetcher in the CronJob polls each indexer's /status endpoint and POSTs a
snapshot of the synced+healthy deployments per indexer to the IISA service;
the push handler persists it to sync_status.json (no shared filesystem). This
module reads that cache file and builds a reverse index so the IISA can answer
"which indexers are already synced for deployment X?" in O(1).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

__all__ = ["SyncStatusData", "SyncStatusLoader"]

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """The current time; a function of its own so tests can move it."""
    return datetime.now(timezone.utc)


def _parse_fetched_at(indexer: str, fetched_at_str: Any) -> Optional[datetime]:
    """Parse an entry's fetched_at as an aware datetime, taking naive values as UTC.

    Returns None when it is missing, and also when it is not an ISO timestamp,
    after logging a warning.
    """
    if fetched_at_str is None:
        return None
    try:
        fetched_at = datetime.fromisoformat(fetched_at_str)
    except (ValueError, TypeError):
        logger.warning(
            "sync_status: invalid fetched_at for %s: %s",
            indexer[:10],
            fetched_at_str,
        )
        return None
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    return fetched_at


def _is_fresh(fetched_at: datetime, now: datetime, staleness_threshold_hours: float) -> bool:
    """True when fetched_at is no more than staleness_threshold_hours before now."""
    age_hours = (now - fetched_at).total_seconds() / 3600
    return age_hours <= staleness_threshold_hours


def _readable_deployments(indexer: str, deployments: Any) -> list[str]:
    """Return the deployment ids an entry lists, or none when it lists none.

    Anything other than a list of text ids also yields none, after logging a warning.
    """
    if deployments is None:
        return []
    if isinstance(deployments, list) and all(isinstance(d, str) for d in deployments):
        return deployments
    logger.warning(
        "sync_status: ignoring unreadable deployments for %s: %.200r", indexer[:10], deployments
    )
    return []


class SyncStatusData:
    """Reverse index: deployment -> set[indexer] for synced+healthy deployments.

    An indexer's entry counts only while its fetched_at is within staleness_threshold_hours of
    now, judged on every lookup rather than once when the snapshot is built, so a snapshot stops
    counting once it is too old even when no newer push arrives to replace it.
    """

    def __init__(
        self,
        raw: dict[str, Any],
        staleness_threshold_hours: float = 6.0,
    ):
        now = _utc_now()
        self._staleness_threshold_hours = staleness_threshold_hours
        self._deployment_index: dict[str, set[str]] = {}
        # When each indexed indexer's entry was fetched, the newest if it appears more than once.
        self._fetched_at: dict[str, datetime] = {}

        for indexer, entry in raw.items():
            if not isinstance(entry, dict):
                logger.warning(
                    "sync_status: ignoring entry for %s that is not an object: %.200r",
                    indexer[:10],
                    entry,
                )
                continue

            fetched_at = _parse_fetched_at(indexer, entry.get("fetched_at"))
            if fetched_at is None or not _is_fresh(fetched_at, now, staleness_threshold_hours):
                continue

            deployments = _readable_deployments(indexer, entry.get("deployments"))
            if not deployments:
                continue

            indexer_lower = indexer.lower()
            self._fetched_at[indexer_lower] = max(
                fetched_at, self._fetched_at.get(indexer_lower, fetched_at)
            )
            for deployment_id in deployments:
                self._deployment_index.setdefault(deployment_id, set()).add(indexer_lower)

        stale_count = len(raw) - len(self._fetched_at)
        if stale_count > 0:
            logger.info(
                "sync_status: loaded %d indexers, %d deployments (%d stale entries filtered)",
                len(self._fetched_at),
                len(self._deployment_index),
                stale_count,
            )
        else:
            logger.info(
                "sync_status: loaded %d indexers, %d deployments",
                len(self._fetched_at),
                len(self._deployment_index),
            )

    def _counting_indexers(self) -> set[str]:
        """The indexers whose entry is still young enough to count."""
        now = _utc_now()
        return {
            indexer
            for indexer, fetched_at in self._fetched_at.items()
            if _is_fresh(fetched_at, now, self._staleness_threshold_hours)
        }

    def synced_indexers_for(self, deployment_id: str) -> set[str]:
        """Return the indexers synced+healthy for this deployment whose entry still counts."""
        return self._deployment_index.get(deployment_id, set()) & self._counting_indexers()

    @property
    def total_indexers(self) -> int:
        """How many indexers have an entry that still counts."""
        return len(self._counting_indexers())

    @property
    def total_deployments(self) -> int:
        """How many deployments have at least 1 indexer whose entry still counts."""
        counting = self._counting_indexers()
        return sum(1 for indexers in self._deployment_index.values() if indexers & counting)


class SyncStatusLoader:
    """Reads sync_status.json from disk."""

    def __init__(self, file_path: str):
        self._file_path = file_path

    def load(self, staleness_threshold_hours: float = 6.0) -> Optional[SyncStatusData]:
        """Read and parse sync status file. Returns None on any failure."""
        try:
            with open(self._file_path) as f:
                raw = json.load(f)
        except FileNotFoundError:
            logger.debug("sync_status: file not found: %s", self._file_path)
            return None
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("sync_status: failed to read %s: %s", self._file_path, e)
            return None

        if not isinstance(raw, dict):
            logger.warning("sync_status: expected dict, got %s", type(raw).__name__)
            return None

        return SyncStatusData(raw, staleness_threshold_hours)
