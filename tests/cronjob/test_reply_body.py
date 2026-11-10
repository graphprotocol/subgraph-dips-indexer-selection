"""Reading an indexer's reply in full, but never more of it than a set limit.

Each indexer runs the server its registered URL points at, so a reply can be any size, arrive in
any number of pieces, or never end. These tests serve replies from a local aiohttp server so the
real network reading runs.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

from reply_body import ReplyTooLargeError, read_reply_body  # noqa: E402

LIMIT = 1_024
# Long enough that a test which waited for it would fail by raising a timeout instead.
TIMEOUT_SECONDS = 5


async def _read(handler, release: Optional[asyncio.Event] = None) -> bytes:
    app = web.Application()
    app.router.add_get("/", handler)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        try:
            async with session.get(
                server.make_url("/"), timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
            ) as resp:
                return await read_reply_body(resp, LIMIT)
        finally:
            # Let a stalled handler finish, so closing the server does not wait for it.
            if release is not None:
                release.set()


def _in_pieces(*pieces: bytes, declare_length: bool):
    async def handler(request):
        resp = web.StreamResponse()
        if declare_length:
            resp.content_length = sum(len(piece) for piece in pieces)
        await resp.prepare(request)
        for piece in pieces:
            await resp.write(piece)
            # A pause so the client sees each piece arrive on its own, as over a slow link.
            await asyncio.sleep(0.05)
        await resp.write_eof()
        return resp

    return handler


DECLARED_OR_NOT = pytest.mark.parametrize(
    "declare_length", [True, False], ids=["declared-length", "no-declared-length"]
)


@DECLARED_OR_NOT
async def test_reply_sent_in_pieces_is_read_whole(declare_length):
    handler = _in_pieces(
        b'{"data": ', b'{"version": "0.36.0"}', b"}", declare_length=declare_length
    )

    assert await _read(handler) == b'{"data": {"version": "0.36.0"}}'


@DECLARED_OR_NOT
async def test_reply_exactly_at_the_limit_is_read(declare_length):
    handler = _in_pieces(b"x" * 1_000, b"x" * 24, declare_length=declare_length)

    assert await _read(handler) == b"x" * LIMIT


@DECLARED_OR_NOT
async def test_reply_1_byte_over_the_limit_is_refused(declare_length):
    handler = _in_pieces(b"x" * 1_000, b"x" * 25, declare_length=declare_length)

    with pytest.raises(ReplyTooLargeError):
        await _read(handler)


async def test_declared_length_over_the_limit_is_refused_without_waiting_for_the_body():
    # The server declares a long reply, sends a little of it, then stalls. Waiting for the rest
    # would end in a timeout, so a refusal shows the declared length alone was enough.
    release = asyncio.Event()

    async def handler(request):
        resp = web.StreamResponse()
        resp.content_length = 10_000_000
        await resp.prepare(request)
        await resp.write(b"x" * 10)
        await release.wait()
        return resp

    with pytest.raises(ReplyTooLargeError, match="10,000,000"):
        await _read(handler, release)


async def test_reply_that_never_ends_is_cut_off_at_the_limit():
    # No declared length and no end, so only counting what arrives can stop it before the timeout.
    async def handler(request):
        resp = web.StreamResponse()
        await resp.prepare(request)
        try:
            while True:
                await resp.write(b"x" * 100)
                await asyncio.sleep(0.01)
        except ConnectionResetError:
            # The client hung up, which is what the test wants.
            return resp

    with pytest.raises(ReplyTooLargeError, match="1,024"):
        await _read(handler)
