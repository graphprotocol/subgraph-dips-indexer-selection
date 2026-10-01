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


class SyncStatusData:
    """Reverse index: deployment -> set[indexer] for synced+healthy deployments."""

    def __init__(
        self,
        raw: dict[str, dict],
        staleness_threshold_hours: float = 6.0,
    ):
        now = datetime.now(timezone.utc)
        self._deployment_index: dict[str, set[str]] = {}
        self._indexer_count = 0

        for indexer, entry in raw.items():
            fetched_at = _parse_fetched_at(indexer, entry.get("fetched_at"))
            if fetched_at is None or not _is_fresh(fetched_at, now, staleness_threshold_hours):
                continue

            deployments = entry.get("deployments", [])
            if not deployments:
                continue

            self._indexer_count += 1
            indexer_lower = indexer.lower()
            for deployment_id in deployments:
                self._deployment_index.setdefault(deployment_id, set()).add(indexer_lower)

        stale_count = len(raw) - self._indexer_count
        if stale_count > 0:
            logger.info(
                "sync_status: loaded %d indexers, %d deployments (%d stale entries filtered)",
                self._indexer_count,
                len(self._deployment_index),
                stale_count,
            )
        else:
            logger.info(
                "sync_status: loaded %d indexers, %d deployments",
                self._indexer_count,
                len(self._deployment_index),
            )

    def synced_indexers_for(self, deployment_id: str) -> set[str]:
        """Return indexer addresses synced+healthy for this deployment."""
        return set(self._deployment_index.get(deployment_id, set()))

    @property
    def total_indexers(self) -> int:
        return self._indexer_count

    @property
    def total_deployments(self) -> int:
        return len(self._deployment_index)


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
