"""When each run read stake from the network subgraph, and the shape of a stake answer.

A stake answer is indexed by indexer and holds the slashable stake, the query fees from the
replay, their ratio, and the time the stake was read, which the scores carry to iisa.
"""

from datetime import datetime, timezone

import pandas as pd

STAKE_COLUMNS = ["stake_to_fees", "total_query_fees", "last_known_slashable_stake"]

# When the stake in a row was read from the network subgraph. Missing for an indexer whose
# stake the run does not have, and in the fallback scores, which never read stake.
STAKE_READ_AT = "stake_read_at"


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
