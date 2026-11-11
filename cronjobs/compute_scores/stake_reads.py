"""When each run read stake from the network subgraph, and reusing an earlier run's reading.

A stake answer is indexed by indexer and holds the slashable stake, the query fees from the
replay, their ratio, and the time the stake was read, which the scores carry to iisa. When a
run cannot read stake it may reuse what an earlier run read: stake and fees barely move in a
day, so a reading a day or 2 old is closer to the truth than leaving the heaviest selection
signal the same for every indexer.
"""

import logging
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

STAKE_COLUMNS = ["stake_to_fees", "total_query_fees", "last_known_slashable_stake"]

# When the stake in a row was read from the network subgraph. Missing for an indexer whose
# stake the run does not have, and in the fallback scores, which never read stake.
STAKE_READ_AT = "stake_read_at"

# A stake reading or a quality measurement may be reused while it is less than this old. The
# job runs daily at 09:00 UTC, so a failed run finds one about 24 hours old, the next about 48
# and the next about 72. 60 sits halfway between the 2nd and 3rd, so exactly 2 runs in a row
# can reuse it and a slow or late run does not change which ones do.
REUSE_LIMIT = timedelta(hours=60)


def utc_now() -> datetime:
    """The current time in UTC, in 1 place so tests can set the clock the job sees."""
    return datetime.now(timezone.utc)


def no_stake() -> pd.DataFrame:
    """A stake answer with no rows, keyed by text like a real one so the merge never compares
    unlike key types.
    """
    index = pd.Index([], dtype="str", name="indexer")
    answer = pd.DataFrame(columns=STAKE_COLUMNS, index=index, dtype=float)
    answer[STAKE_READ_AT] = pd.Series(index=index, dtype="datetime64[ns, UTC]")
    return answer


def stake_to_fees(stake: pd.Series, fees: pd.Series) -> pd.Series:
    """Slashable stake over query fees; an indexer with no fees gets NaN, not infinity."""
    return stake / fees.replace(0.0, float("nan"))


def reusable_stake(reply: Any, now: datetime) -> pd.DataFrame:
    """The stake in iisa's GET /scores reply that may still be reused at now, as a stake answer.

    A row counts only with a read time less than REUSE_LIMIT before now, so neither the
    fallback scores, which never read stake, nor scores pushed before read times were recorded
    are reused. A reused row keeps its original read time, so reuse cannot chain for ever. Its
    stake and fees must be numbers of at least 0, and their ratio is worked out again from them.
    An indexer listed more than once is left out, since its rows could disagree.
    """
    rows = held_rows(reply)
    checked = [_check_row(row, now) for row in rows]
    usable = [row for row in checked if isinstance(row, dict)]
    listed = Counter(row["indexer"] for row in usable)
    usable = [row for row in usable if listed[row["indexer"]] == 1]
    left_out = Counter(reason for reason in checked if isinstance(reason, str))
    left_out["listed more than once"] = sum(count for count in listed.values() if count > 1)
    logger.info(
        "Stake iisa holds: %d of %d rows can be reused; left out: %s",
        len(usable),
        len(rows),
        ", ".join(f"{count} {reason}" for reason, count in left_out.items() if count) or "none",
    )
    return _stake_answer(usable) if usable else no_stake()


def leave_out_unread(merged: pd.DataFrame, lookup_started: datetime) -> pd.DataFrame:
    """On a run that reused an earlier stake reading, leave out the indexers that have none.

    A reading stamped before this run's lookup began can only be a reused one. An indexer with
    no reading would then go out with stake missing beside indexers with real stake, and iisa
    scores missing stake as the best: a rank nobody measured. So it waits for a run that reads
    its stake. When no indexer has a reading, every row is kept, as stake is then missing for
    all alike; and when the lookup worked, nothing was reused and every row is kept too.
    """
    if STAKE_READ_AT not in merged.columns:
        return merged
    read_at = pd.to_datetime(merged[STAKE_READ_AT], utc=True)
    unread = read_at.isna()
    if not (read_at < lookup_started).any() or not unread.any():
        return merged
    left_out = merged.loc[unread, "indexer"].tolist()
    logger.warning(
        "Left %d indexer(s) out of today's scores: this run could not read stake and iisa holds "
        "no reading under %.0f hours old for them: %s",
        len(left_out),
        REUSE_LIMIT.total_seconds() / 3600,
        ", ".join(left_out[:10]) + (" and others" if len(left_out) > 10 else ""),
    )
    return merged[~unread].reset_index(drop=True)


def held_rows(reply: Any) -> list[dict]:
    """The rows of a GET /scores reply, or none when the reply is not shaped like one."""
    records = reply.get("scores") if isinstance(reply, dict) else None
    if not isinstance(records, list):
        return []
    return [row for row in records if isinstance(row, dict)]


def _stake_answer(rows: list[dict]) -> pd.DataFrame:
    """A stake answer from checked rows, with the ratio worked out again from stake and fees."""
    answer = pd.DataFrame(rows).set_index("indexer")
    answer.index = answer.index.astype("str")
    answer["stake_to_fees"] = stake_to_fees(
        answer["last_known_slashable_stake"], answer["total_query_fees"]
    )
    answer[STAKE_READ_AT] = pd.to_datetime(answer[STAKE_READ_AT], utc=True)
    return answer[[*STAKE_COLUMNS, STAKE_READ_AT]]


def _check_row(row: dict, now: datetime) -> Union[dict, str]:
    """The parts of 1 held row a run may reuse, or why it may not."""
    indexer = row.get("indexer")
    read_at = read_time(row.get(STAKE_READ_AT))
    stake = _amount(row.get("last_known_slashable_stake"))
    fees = _amount(row.get("total_query_fees"))
    if read_at is None:
        return "without a read time"
    if not timedelta(0) <= now - read_at < REUSE_LIMIT:
        return "read too long ago or in the future"
    if not isinstance(indexer, str) or stake is None or fees is None:
        return "with an unreadable indexer, stake or fees"
    return {
        "indexer": indexer,
        "last_known_slashable_stake": stake,
        "total_query_fees": fees,
        STAKE_READ_AT: read_at,
    }


def read_time(value: Any) -> Optional[datetime]:
    """A read time as the job writes it, ISO text; anything else is not a read time."""
    if not isinstance(value, str):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(parsed) else parsed.to_pydatetime()


def _amount(value: Any) -> Optional[float]:
    """A stake or fee amount: a finite number of at least 0, or None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None
