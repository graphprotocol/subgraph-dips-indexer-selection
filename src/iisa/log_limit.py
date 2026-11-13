"""Holding back a log line that would otherwise repeat on every request while a condition lasts."""

import threading
from typing import Optional

__all__ = ["LogLimit"]


class LogLimit:
    """Lets a log line through at most once per key every ``interval_minutes`` minutes, counting
    those held back in between, until the key is reset, which starts it afresh. It has a lock
    of its own, so requests running on different threads can share it.
    """

    def __init__(self, interval_minutes: int) -> None:
        self._interval = interval_minutes
        self._lock = threading.Lock()
        self._last: dict[str, int] = {}
        self._held: dict[str, int] = {}

    def allow(self, key: str, now: int) -> Optional[int]:
        """How many lines for ``key`` were held back since the last one let through, when a line
        in the minute ``now`` may be logged, or None when it is held back too.
        """
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < self._interval:
                self._held[key] = self._held.get(key, 0) + 1
                return None
            self._last[key] = now
            return self._held.pop(key, 0)

    def reset(self, key: str) -> None:
        """Forget ``key``'s last line and the count held back since, so its next line goes
        through and counts only what is held back after it.
        """
        with self._lock:
            self._last.pop(key, None)
            self._held.pop(key, None)
