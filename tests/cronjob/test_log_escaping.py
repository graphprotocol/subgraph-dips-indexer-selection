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


def test_fetcher_warning_keeps_an_error_repeating_the_url_on_1_line(caplog):
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


# --- The quoting itself ---------------------------------------------------------------------


def test_error_text_is_escaped_named_and_cut_short():
    from escaping import error_for_log

    text = error_for_log(ValueError("bad" + FAKE_LINE + "x" * 1_000))

    assert "\n" not in text
    assert text.startswith("ValueError: bad\\nINFO")
    assert len(text) < 300


def test_missing_error_is_said_plainly():
    from escaping import error_for_log

    assert error_for_log(None) == "no error recorded"
