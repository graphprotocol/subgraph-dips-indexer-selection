"""Keeping an indexer's URL, and other text it wrote, from starting fake log lines.

Each indexer registers its own URL on-chain and runs the server behind it. aiohttp drops a line
break from a URL and still makes the request, so a URL with one in it works, and logging it raw
started a new line in the log's own format, which could claim, say, that a push succeeded.
These tests check that each log line quoting such text keeps it on 1 line.
"""

import asyncio
import logging
import sys
from pathlib import Path

import aiohttp
import pandas as pd
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import processing  # noqa: E402
import sync_status_fetcher  # noqa: E402

FAKE_LINE = "\nINFO - Pushed sync status for 500 indexers to iisa"
# A URL aiohttp refuses to parse. The error it raises repeats the URL exactly as written.
UNPARSEABLE_URL = "http://[::1" + FAKE_LINE


def _assert_on_1_line(caplog, logger_name: str, level: int) -> None:
    messages = [
        r.getMessage() for r in caplog.records if r.name == logger_name and r.levelno == level
    ]
    assert messages, f"nothing logged at {logging.getLevelName(level)} by {logger_name}"
    for message in messages:
        assert "\n" not in message, message
        # Still shown, as the 2 characters of an escaped line break.
        assert "\\n" in message, message


async def _with_server(handler, run):
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        return await run(session, str(server.make_url("/")))


# --- The sync-status fetcher ----------------------------------------------------------------


def test_fetcher_malformed_reply_warning_keeps_the_url_on_1_line(caplog):
    async def handler(request):
        return web.json_response({"data": None})

    async def run(session, base_url):
        return await sync_status_fetcher._fetch_single_status(
            session, "0xBad", base_url + FAKE_LINE, asyncio.Semaphore(1)
        )

    with caplog.at_level(logging.WARNING, logger="sync_status_fetcher"):
        assert asyncio.run(_with_server(handler, run)) is None

    _assert_on_1_line(caplog, "sync_status_fetcher", logging.WARNING)


def test_fetcher_failed_fetch_warning_keeps_the_url_on_1_line(caplog, monkeypatch):
    monkeypatch.setattr(sync_status_fetcher, "MAX_RETRIES", 1)

    async def handler(request):
        return web.Response(status=503)

    async def run(session, base_url):
        return await sync_status_fetcher._fetch_single_status(
            session, "0xDown", base_url + FAKE_LINE, asyncio.Semaphore(1)
        )

    with caplog.at_level(logging.WARNING, logger="sync_status_fetcher"):
        assert asyncio.run(_with_server(handler, run)) is None

    _assert_on_1_line(caplog, "sync_status_fetcher", logging.WARNING)


def test_fetcher_warning_keeps_an_error_repeating_the_url_on_1_line(caplog, monkeypatch):
    # 1 attempt, as in the other tests, so the test never waits between retries.
    monkeypatch.setattr(sync_status_fetcher, "MAX_RETRIES", 1)

    async def run():
        async with aiohttp.ClientSession() as session:
            return await sync_status_fetcher._fetch_single_status(
                session, "0xOdd", UNPARSEABLE_URL, asyncio.Semaphore(1)
            )

    with caplog.at_level(logging.WARNING, logger="sync_status_fetcher"):
        assert asyncio.run(run()) is None

    _assert_on_1_line(caplog, "sync_status_fetcher", logging.WARNING)


# --- The scoring job ------------------------------------------------------------------------


def test_dips_info_failure_keeps_the_url_and_error_on_1_line(caplog, monkeypatch):
    monkeypatch.setattr(processing, "DIPS_INFO_MAX_RETRIES", 1)

    async def run():
        async with aiohttp.ClientSession() as session:
            return await processing._fetch_single_dips_info_async(
                session, "0xOdd", UNPARSEABLE_URL, asyncio.Semaphore(1)
            )

    with caplog.at_level(logging.DEBUG, logger="processing"):
        assert asyncio.run(run())["dips_info_available"] is False

    _assert_on_1_line(caplog, "processing", logging.DEBUG)


def test_graph_node_version_failure_keeps_the_url_and_error_on_1_line(caplog, monkeypatch):
    monkeypatch.setattr(processing, "GRAPH_NODE_VERSION_MAX_RETRIES", 1)

    async def run():
        async with aiohttp.ClientSession() as session:
            return await processing._fetch_single_graph_node_version_async(
                session, "0xOdd", UNPARSEABLE_URL, asyncio.Semaphore(1)
            )

    with caplog.at_level(logging.DEBUG, logger="processing"):
        assert asyncio.run(run())["graph_node_version"] is None

    _assert_on_1_line(caplog, "processing", logging.DEBUG)


def test_reported_graph_node_version_stays_on_1_line(caplog):
    # The version is text the indexer's own server sent back.
    scores = pd.DataFrame({"indexer": ["0xOld"], "graph_node_version": ["0.1.0" + FAKE_LINE]})

    with caplog.at_level(logging.DEBUG, logger="processing"):
        kept = processing.filter_by_min_graph_node_version(scores, "0.36.0", strict=True)

    assert kept.empty
    _assert_on_1_line(caplog, "processing", logging.DEBUG)


def test_geoip_failure_keeps_the_url_on_1_line(caplog):
    with caplog.at_level(logging.WARNING, logger="processing"):
        processing.resolve_url_geoip(UNPARSEABLE_URL)

    _assert_on_1_line(caplog, "processing", logging.WARNING)


# --- Showing a long but ordinary URL whole --------------------------------------------------
#
# Indexer URLs of 60 to 80 characters are common. Cut to 60 with the middle left out, the cut
# often fell inside the host name, which made a failing indexer harder to find.

LONG_PATH = "indexer-arbitrum-one/some-operator-company/"


def test_fetcher_warning_shows_a_long_url_whole(caplog, monkeypatch):
    monkeypatch.setattr(sync_status_fetcher, "MAX_RETRIES", 1)
    urls = []

    async def handler(request):
        return web.Response(status=503)

    async def run(session, base_url):
        urls.append(base_url + LONG_PATH)
        return await sync_status_fetcher._fetch_single_status(
            session, "0xLong", urls[0], asyncio.Semaphore(1)
        )

    with caplog.at_level(logging.WARNING, logger="sync_status_fetcher"):
        asyncio.run(_with_server(handler, run))

    assert len(urls[0]) > 60
    warning = next(r.getMessage() for r in caplog.records if r.name == "sync_status_fetcher")
    assert repr(urls[0]) in warning, warning


def test_dips_info_failure_shows_a_long_url_whole(caplog, monkeypatch):
    monkeypatch.setattr(processing, "DIPS_INFO_MAX_RETRIES", 1)
    urls = []

    async def handler(request):
        return web.Response(status=503)

    async def run(session, base_url):
        urls.append(base_url + LONG_PATH)
        return await processing._fetch_single_dips_info_async(
            session, "0xLong", urls[0], asyncio.Semaphore(1)
        )

    with caplog.at_level(logging.DEBUG, logger="processing"):
        asyncio.run(_with_server(handler, run))

    message = next(r.getMessage() for r in caplog.records if "Failed to fetch" in r.getMessage())
    assert repr(urls[0]) in message, message


# --- The quoting itself ---------------------------------------------------------------------


def test_error_text_is_escaped_named_and_cut_short():
    from escaping import error_for_log

    text = error_for_log(ValueError("bad" + FAKE_LINE + "x" * 1_000))

    assert "\n" not in text
    assert text.startswith("ValueError: bad\\nINFO")
    assert len(text) < 600


def test_error_text_of_300_characters_keeps_its_last_words():
    # Certificate errors run to about 300 characters and give the reason, such as an expired
    # certificate, at the very end.
    from escaping import error_for_log

    text = error_for_log(OSError("x" * 280 + " certificate has expired"))

    assert text.endswith(" certificate has expired")


def test_error_text_of_600_characters_is_cut():
    from escaping import error_for_log

    text = error_for_log(OSError("x" * 580 + " certificate has expired"))

    assert "certificate has expired" not in text
    assert text.endswith("...")


class _ErrorWithBrokenText(Exception):
    def __str__(self):
        raise RuntimeError("cannot describe this error")


class _ErrorWithNonText(Exception):
    def __str__(self):
        return 5  # type: ignore[return-value]


@pytest.mark.parametrize("error", [_ErrorWithBrokenText(), _ErrorWithNonText()])
def test_error_whose_text_cannot_be_read_says_so(error):
    # This runs while handling an error. Raising here would fail the fetch for every indexer.
    from escaping import error_for_log

    text = error_for_log(error)

    # The line says the text was lost, so a reader knows the error had more to say.
    assert text == f"{type(error).__name__} (its text could not be read)"
    assert text.isascii() and text.isprintable()


def test_unreadable_error_with_a_line_break_in_its_name_stays_on_1_line():
    from escaping import error_for_log

    renamed = type("Broken\nINFO - fake", (_ErrorWithBrokenText,), {})

    text = error_for_log(renamed())

    assert text.isascii() and text.isprintable(), repr(text)
    assert text.endswith("(its text could not be read)")


def test_missing_error_is_said_plainly():
    from escaping import error_for_log

    assert error_for_log(None) == "no error recorded"


# Characters that end a line, move the cursor or flip text direction in a terminal or log viewer,
# besides the line feed every other test uses.
LINE_BREAKING_CHARACTERS = [
    pytest.param("\r", id="carriage-return"),
    pytest.param("\x1b", id="escape"),
    pytest.param("\x85", id="next-line"),
    pytest.param("\u2028", id="line-separator"),
    pytest.param("\u2029", id="paragraph-separator"),
    pytest.param("\u202e", id="right-to-left-override"),
    pytest.param("\ud800", id="lone-surrogate"),
]


@pytest.mark.parametrize("character", LINE_BREAKING_CHARACTERS)
def test_every_helper_gives_1_line_of_printable_ascii(character):
    from escaping import error_for_log, for_log, url_for_log

    value = "a" + character + "b"
    for text in (for_log(value), url_for_log(value), error_for_log(ValueError(value))):
        assert text.isascii() and text.isprintable(), repr(text)
