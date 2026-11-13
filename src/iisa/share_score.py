"""How a share of good outcomes, such as checks passed or time up, becomes a score.

The score is the share raised to SHARE_EXPONENT, so it rewards the last few percent most: 0.9
scores about 0.53 and 0.8 about 0.26. A share is scored on its own, never against other
indexers, so an indexer's score doesn't move when a better one joins.
"""

from typing import TypeVar

import pandas as pd

__all__ = ["SHARE_EXPONENT", "shaped_share"]

SHARE_EXPONENT = 6

Share = TypeVar("Share", float, pd.Series)


def shaped_share(share: Share) -> Share:
    """``share``, a number from 0 to 1 or a Series of them, raised to SHARE_EXPONENT."""
    return share**SHARE_EXPONENT
