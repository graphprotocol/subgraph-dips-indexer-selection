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
# URLs keep up to 200 characters, as an ordinary indexer URL can pass 60 and a cut inside the
# host name makes the indexer hard to find.
_URL_REPR = reprlib.Repr()
_URL_REPR.maxstring = _MAX_LOGGED_CHARS
# Errors get more room: a certificate error runs to about 300 characters and names the reason,
# such as an expired certificate or the wrong host name, at the very end.
_MAX_LOGGED_ERROR_CHARS = 500


def for_log(value: object) -> str:
    """Quote a value an indexer sent for a log line: escaped, so a line break in it cannot start a
    fake log line, and cut short, so a huge value cannot flood the log on every run.
    """
    text = _LOG_REPR.repr(value)
    return text if len(text) <= _MAX_LOGGED_CHARS else text[:_MAX_LOGGED_CHARS] + "..."


def url_for_log(url: object) -> str:
    """Quote an indexer's URL for a log line like `for_log`, keeping up to 200 characters of it."""
    text = _URL_REPR.repr(url)
    return text if len(text) <= _MAX_LOGGED_CHARS else text[:_MAX_LOGGED_CHARS] + "..."


def error_for_log(error: Optional[BaseException]) -> str:
    """Name and text of an error for a log line, escaped like `for_log` and cut at 500 characters.

    An error's text can repeat what an indexer sent, such as its URL, so it needs the same care.
    """
    if error is None:
        return "no error recorded"
    try:
        text = f"{type(error).__name__}: {error}".encode("unicode_escape").decode("ascii")
    except Exception:
        # The error's own text could not be read. This runs while another error is being
        # handled, so raising here would replace it. The log line names the error and says its
        # text was lost, so the gap is visible.
        name = type(error).__name__.encode("unicode_escape").decode("ascii")
        return f"{name} (its text could not be read)"
    if len(text) <= _MAX_LOGGED_ERROR_CHARS:
        return text
    return text[:_MAX_LOGGED_ERROR_CHARS] + "..."
