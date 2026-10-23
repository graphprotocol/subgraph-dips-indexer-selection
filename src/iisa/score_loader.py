"""
Loads pre-computed indexer scores from the IISA service's own cache file.

Scores are computed daily by a CronJob (cronjobs/compute_scores/) and POSTed to
the IISA service; the push handler persists them to a JSON file (no shared
filesystem). IISA reads that cache file on startup using DataManager.load_scores().
"""

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

import pandas as pd

from . import score_columns as cols

__all__ = [
    "FileScoreLoader",
    "DataManager",
    "ScoresSnapshot",
    "ScoresPayloadError",
    "REQUIRED_SCORE_COLUMNS",
    "missing_required_columns",
]


@dataclass(frozen=True, eq=False)
class ScoresSnapshot:
    """Immutable pair of (scores DataFrame, computed_at) swapped atomically.

    Bundled so a reader sees both fields from the same push: two separate
    attribute writes open a window where a reader lands on (new computed_at,
    old data). A single reference swap is atomic in CPython, so readers always
    observe one generation.
    eq=False because DataFrame.__eq__ returns an element-wise frame, not a
    bool, and raises on truth-testing; identity is the right primitive here.
    """

    data: Optional[pd.DataFrame]
    computed_at: Optional[datetime]


# Staleness thresholds
STALE_SCORES_WARNING_HOURS = 48
STALE_SCORES_CRITICAL_HOURS = 168  # 7 days

logger = logging.getLogger(__name__)


SCORES_FILE_PATH = os.environ.get("SCORES_FILE_PATH", "/app/scores/indexer_scores.json")


class ScoresPayloadError(ValueError):
    """A pushed scores payload was missing required columns or held an indexer that is not text.

    Separate type so the push endpoint can answer 422 (bad body) rather than
    500 (service broke). ``errors`` lists each problem in the shape of FastAPI's 422 errors.
    """

    def __init__(self, message: str, errors: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.errors = errors


# Identity plus the quality/economic metrics the selector scores on. A push
# missing any of these still parses, but the served scores quietly collapse to
# zeros, so the boundary rejects it instead.
REQUIRED_SCORE_COLUMNS: tuple[str, ...] = (
    cols.INDEXER,
    cols.COMPUTED_AT,
    cols.LAT_NORMALIZED_SCORE,
    cols.UPTIME_SCORE,
    cols.SUCCESS_RATE,
    cols.STAKE_TO_FEES,
)


def missing_required_columns(df: pd.DataFrame) -> list[str]:
    """Return the required columns absent from a pushed scores frame."""
    return [c for c in REQUIRED_SCORE_COLUMNS if c not in df.columns]


def _check_indexers_are_text(df: pd.DataFrame) -> None:
    """Raise ScoresPayloadError naming each record whose indexer is not text, since selection
    can only match and return an indexer by its address. pandas reads a null or absent
    indexer as NaN, so the error reports that input as null.
    """
    errors = [
        {
            "type": "string_type",
            "loc": ("body", position, cols.INDEXER),
            "msg": "Input should be a valid string",
            "input": None if isinstance(indexer, float) and math.isnan(indexer) else indexer,
        }
        for position, indexer in enumerate(df[cols.INDEXER])
        if not isinstance(indexer, str)
    ]
    if errors:
        raise ScoresPayloadError(
            f"pushed scores payload has {len(errors)} indexer(s) that are not text, "
            f"first in record {errors[0]['loc'][1]}",
            errors,
        )


class FileScoreLoader:
    """
    Reads pre-computed indexer scores from the IISA service's own cache file.

    The CronJob POSTs scores to the service via RedpandaProvider.write_scores();
    the push handler persists them to the cache file, and this class reads them back.
    """

    def __init__(self, scores_file_path: Optional[str] = None) -> None:
        # Read the module-level default at call time so tests can monkeypatch
        # score_loader.SCORES_FILE_PATH and have new instances pick it up; a
        # def-time default would lock in whatever value was present at import.
        self._path = scores_file_path if scores_file_path is not None else SCORES_FILE_PATH

    @property
    def path(self) -> str:
        """The filesystem path this loader reads from and the push handler writes to."""
        return self._path

    def fetch_indexer_scores(self) -> Tuple[pd.DataFrame, Optional[datetime]]:
        """
        Read the scores JSON file and return a (DataFrame, computed_at) tuple.

        Returns (empty DataFrame, None) if the file doesn't exist or is unreadable.
        """
        logger.info(f"Reading pre-computed indexer scores from {self._path}")

        try:
            with open(self._path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.warning(f"Scores file not found: {self._path}")
            return pd.DataFrame(), None
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Failed to read scores file {self._path}: {e}")
            return pd.DataFrame(), None

        if not data:
            logger.warning("Scores file is empty")
            return pd.DataFrame(), None

        df = pd.DataFrame(data)

        if "computed_at" not in df.columns:
            logger.warning("Scores file has no computed_at column")
            return df, None

        df["computed_at"] = pd.to_datetime(df["computed_at"], utc=True, errors="coerce")
        computed_at = df["computed_at"].iloc[0]
        if pd.isna(computed_at):
            computed_at = None

        logger.info(f"Loaded {len(df)} indexer scores from file (computed at {computed_at})")
        return df, computed_at


class DataManager:
    """
    Loads pre-computed indexer scores from the configured provider.

    Scores are computed daily by a CronJob and include latency regression
    coefficients, uptime, success rate, and economic security metrics.
    Accepts any object with a fetch_indexer_scores() method — currently
    FileScoreLoader.
    """

    def __init__(self, provider) -> None:
        self._provider = provider
        self._snapshot: ScoresSnapshot = ScoresSnapshot(data=None, computed_at=None)

    @property
    def snapshot(self) -> ScoresSnapshot:
        """Atomic snapshot of (data, computed_at) — read once, consistent pair."""
        return self._snapshot

    @property
    def scores_file_path(self) -> Optional[str]:
        """The on-disk path of the file-backed provider, or None.

        None when the provider has no .path attribute (a future provider that
        doesn't persist to a single file); push-path callers must guard for it
        before writing to disk.
        """
        return getattr(self._provider, "path", None)

    def load_scores(self) -> bool:
        """
        Load pre-computed indexer scores from the configured provider.

        :return: True if scores were loaded successfully, False otherwise.
        """
        logger.info("Loading pre-computed indexer scores")

        scores_df, computed_at = self._provider.fetch_indexer_scores()

        if scores_df.empty:
            logger.warning("No pre-computed scores available")
            self._snapshot = ScoresSnapshot(data=None, computed_at=None)
            return False

        self._check_scores_staleness(computed_at)
        transformed = self._transform_scores_to_perf_history(scores_df)
        # Atomic swap — a reader concurrent with this line sees either
        # the previous snapshot or the new one, never a mix.
        self._snapshot = ScoresSnapshot(data=transformed, computed_at=computed_at)

        logger.info(f"Loaded scores for {len(transformed)} indexers")
        return True

    def transform_scores_df(self, scores_df: pd.DataFrame) -> pd.DataFrame:
        """Pure column-mapping transform, returned without touching state.

        The push path dry-runs this first, so a payload with no rows (ValueError), missing
        required columns (ScoresPayloadError, also for rows holding no fields, which pandas
        calls empty) or an indexer that is not text (ScoresPayloadError) fails here with no
        side effects and can never poison the cache.
        """
        if len(scores_df.index) == 0:
            raise ValueError("transform_scores_df called with empty DataFrame")
        missing = missing_required_columns(scores_df)
        if missing:
            message = f"pushed scores payload missing required columns: {', '.join(missing)}"
            error = {"type": "missing", "loc": ("body",), "msg": message, "input": None}
            raise ScoresPayloadError(message, [error])
        _check_indexers_are_text(scores_df)
        return self._transform_scores_to_perf_history(scores_df)

    def commit_scores(
        self,
        transformed_df: pd.DataFrame,
        computed_at: Optional[datetime],
    ) -> None:
        """Commit an already-transformed scores frame to in-memory state.

        Callers must have run transform_scores_df() first; this does the atomic
        snapshot swap and logs staleness. Split from load_scores_from_df so the
        push handler can validate, write disk, then commit as separate steps.
        """
        self._check_scores_staleness(computed_at)
        # Atomic swap — see ScoresSnapshot docstring.
        self._snapshot = ScoresSnapshot(data=transformed_df, computed_at=computed_at)
        logger.info("Committed %d scores to in-memory state", len(transformed_df))

    def load_scores_from_df(
        self,
        scores_df: pd.DataFrame,
        computed_at: Optional[datetime],
    ) -> bool:
        """Transform and commit an already-parsed frame in one step.

        Facade over transform_scores_df + commit_scores for the non-push path
        (the push handler uses the split methods directly). Returns False and
        clears the snapshot if the frame is empty, else True.
        """
        if scores_df.empty:
            logger.warning("load_scores_from_df called with empty DataFrame")
            self._snapshot = ScoresSnapshot(data=None, computed_at=None)
            return False

        transformed = self.transform_scores_df(scores_df)
        self.commit_scores(transformed, computed_at)
        return True

    def _transform_scores_to_perf_history(self, scores_df: pd.DataFrame) -> pd.DataFrame:
        """Rename CronJob score columns to the names IndexerSelector expects.

        Maps lat_coefficient_upper_bound -> latency-CI column, success_rate ->
        average_status, lat_normalized_score -> norm_lat_lin_reg_coefficient,
        uptime_score (x100) -> "% up_x", and dst_lat/dst_lon -> destination_loc.
        """
        df = scores_df.copy()

        df = df.rename(columns=cols.CRONJOB_TO_SELECTOR_RENAME)

        if cols.UPTIME_SCORE in df.columns:
            df[cols.SEL_UPTIME_PERCENT] = df[cols.UPTIME_SCORE] * 100

        if cols.DST_LAT in df.columns and cols.DST_LON in df.columns:
            df[cols.SEL_DESTINATION_LOC] = (
                df[cols.DST_LAT].fillna(0).astype(str)
                + ","
                + df[cols.DST_LON].fillna(0).astype(str)
            )

        return df

    def _check_scores_staleness(self, computed_at: Optional[datetime]) -> None:
        """Log warnings if pre-computed scores are stale."""
        if computed_at is None:
            logger.warning("Scores have no computation timestamp")
            return

        now = datetime.now(timezone.utc)
        if computed_at.tzinfo is None:
            computed_at = computed_at.replace(tzinfo=timezone.utc)

        age_hours = (now - computed_at).total_seconds() / 3600

        if age_hours > STALE_SCORES_CRITICAL_HOURS:
            logger.error(
                f"Scores are critically stale ({age_hours:.1f}h old, "
                f"threshold: {STALE_SCORES_CRITICAL_HOURS}h). CronJob may have failed."
            )
        elif age_hours > STALE_SCORES_WARNING_HOURS:
            logger.warning(
                f"Scores are stale ({age_hours:.1f}h old, "
                f"threshold: {STALE_SCORES_WARNING_HOURS}h). Consider checking CronJob status."
            )

    def get_scores_age(self) -> Optional[timedelta]:
        """Return the age of the current scores."""
        snap = self._snapshot
        if snap.computed_at is None:
            return None

        now = datetime.now(timezone.utc)
        computed_at = snap.computed_at
        if computed_at.tzinfo is None:
            computed_at = computed_at.replace(tzinfo=timezone.utc)

        return now - computed_at

    def get_data(self) -> Optional[pd.DataFrame]:
        """Return the loaded scores data."""
        return self._snapshot.data
