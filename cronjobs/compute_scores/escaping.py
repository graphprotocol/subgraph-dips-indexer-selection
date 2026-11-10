"""Quote text an indexer wrote before it goes into a log line.

An indexer chooses its registered URL and everything its server sends back, so that text can hold
a line break that would start a fake log line, or be long enough to flood the log. This module
imports nothing heavy, so the sync-status fetcher can use it as well as the scoring job.
"""

import reprlib
from typing import Optional

_LOG_REPR = reprlib.Repr()
_LOG_REPR.maxlevel = 2
_LOG_REPR.maxstring = _LOG_REPR.maxlong = _LOG_REPR.maxother = 60
_MAX_LOGGED_CHARS = 200
# Errors get more room: a certificate error runs to about 300 characters and names the reason,
# such as an expired certificate or the wrong host name, at the very end.
_MAX_LOGGED_ERROR_CHARS = 500


def for_log(value: object) -> str:
    """Quote a value an indexer sent for a log line: escaped, so a line break in it cannot start a
    fake log line, and cut short, so a huge value cannot flood the log on every run.
    """
    text = _LOG_REPR.repr(value)
    return text if len(text) <= _MAX_LOGGED_CHARS else text[:_MAX_LOGGED_CHARS] + "..."


def error_for_log(error: Optional[BaseException]) -> str:
    """Name and text of an error for a log line, escaped like `for_log` and cut at 500 characters.

    An error's text can repeat what an indexer sent, such as its URL, so it needs the same care.
    """
    if error is None:
        return "no error recorded"
    text = f"{type(error).__name__}: {error}".encode("unicode_escape").decode("ascii")
    if len(text) <= _MAX_LOGGED_ERROR_CHARS:
        return text
    return text[:_MAX_LOGGED_ERROR_CHARS] + "..."
