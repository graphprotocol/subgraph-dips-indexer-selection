"""When each run measured quality, and carrying an earlier run's measurements over.

Latency, uptime and success are measured from the replay of gateway queries. The scores carry
the time of that measurement to iisa beside them, the way they carry the time stake was read.
On a day the replay fails, the run has nothing of its own to push, so it carries over what an
earlier run measured, from the scores iisa holds: each indexer's quality, stake and fees, where
both the quality and the stake are recent enough. An indexer without both is left out, as no
score for it would be a measurement.
"""

import logging
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

import pandas as pd
from stake_reads import (
    REUSE_LIMIT,
    STAKE_COLUMNS,
    STAKE_READ_AT,
    held_rows,
    read_time,
    reusable_stake,
)

logger = logging.getLogger(__name__)

# When the quality scores in a row were measured from the replay. Missing in the fallback
# scores, which measure nothing and give every indexer the same.
QUALITY_MEASURED_AT = "quality_measured_at"

# What a run measures from the replay, by the names the job pushes.
QUALITY_COLUMNS = [
    "lat_lin_reg_coefficient",
    "lat_coefficient_std_error",
    "lat_coefficient_upper_bound",
    "lat_normalized_score",
    "uptime_score",
    "observed_duration_seconds",
    "uptime_duration_seconds",
    "success_rate",
    "uptime_by_chain",
    "success_rate_by_chain",
    "queries_by_chain",
    "org",
    "dst_lat",
    "dst_lon",
    "query_count",
    "scoring_mode",
]

# iisa hands 3 of them back under the names its selector uses, with the values unchanged.
_NAMES_IISA_SERVES = {
    "lat_coefficient_upper_bound": "Latency Coefficient + Error Confidence Interval",
    "success_rate": "average_status",
    "lat_normalized_score": "norm_lat_lin_reg_coefficient",
}

CARRIED_COLUMNS = ["indexer", *QUALITY_COLUMNS, QUALITY_MEASURED_AT, *STAKE_COLUMNS, STAKE_READ_AT]


def carried_over(reply: Any, indexers: Iterable[str], now: datetime) -> pd.DataFrame:
    """The rows of iisa's GET /scores reply a run may carry over at now, 1 for each of today's
    indexers that has both quality and stake recent enough, in CARRIED_COLUMNS.

    Quality counts with a measure time less than REUSE_LIMIT before now, and stake by the same
    rule on its read time, so neither the fallback scores nor scores pushed before these times
    were recorded are carried over. Both times are kept as they were, so carrying over cannot
    chain for ever. An indexer listed more than once is left out, since its rows could disagree.
    """
    quality = _reusable_quality(reply, now)
    stake = reusable_stake(reply, now)
    rows = [quality[indexer] for indexer in indexers if indexer in quality]
    carried = pd.DataFrame(rows, columns=["indexer", *QUALITY_COLUMNS, QUALITY_MEASURED_AT])
    carried = carried.join(stake, on="indexer", how="inner").reset_index(drop=True)
    carried[QUALITY_MEASURED_AT] = pd.to_datetime(carried[QUALITY_MEASURED_AT], utc=True)
    logger.info(
        "Scores iisa holds: recent quality for %d indexer(s) and recent stake for %d; %d of "
        "today's indexers have both",
        len(quality),
        len(stake),
        len(carried),
    )
    return carried[CARRIED_COLUMNS]


def _reusable_quality(reply: Any, now: datetime) -> dict[str, dict]:
    """Each indexer's held quality that may still be reused at now, by indexer."""
    checked = [_quality_row(row, now) for row in held_rows(reply)]
    usable = [row for row in checked if row is not None]
    listed = Counter(row["indexer"] for row in usable)
    return {row["indexer"]: row for row in usable if listed[row["indexer"]] == 1}


def _quality_row(row: dict, now: datetime) -> Optional[dict]:
    """The quality in 1 held row under the names the job pushes, or None if it may not be
    reused: no indexer, no measure time, or measured too long ago or in the future.
    """
    indexer = row.get("indexer")
    measured_at = read_time(row.get(QUALITY_MEASURED_AT))
    if not isinstance(indexer, str) or measured_at is None:
        return None
    if not timedelta(0) <= now - measured_at < REUSE_LIMIT:
        return None
    quality = {column: _held_value(row, column) for column in QUALITY_COLUMNS}
    return {"indexer": indexer, **quality, QUALITY_MEASURED_AT: measured_at}


def _held_value(row: dict, column: str) -> Any:
    """A quality value from a held row, under the pushed name or the name iisa serves it by."""
    if column in row:
        return row[column]
    return row.get(_NAMES_IISA_SERVES.get(column, column))
