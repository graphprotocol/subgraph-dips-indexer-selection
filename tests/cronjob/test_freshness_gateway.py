"""Tests for the request the freshness prober sends to the gateway, against a fake gateway
served on 127.0.0.1 so no query leaves the machine."""

import asyncio
import json
import socket
import sys
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

from freshness_gateway import FRESHNESS_QUERY, GatewayClient  # noqa: E402
from freshness_rules import GatewayReply, GatewayUnreachable, judge  # noqa: E402

FAKE_KEY = "fake-freshness-key-0000"
DEPLOYMENT = "Qm" + "A" * 44
INDEXER = "0x" + "a" * 40
ANSWER = {"data": {"_meta": {"block": {"number": 523092130, "timestamp": 1794345600}}}}


@pytest.fixture
async def fake_gateway():
    """Serve a fake gateway; each test sets what it answers and reads what it was sent."""
    seen = []
    behaviour = {"headers": {"graph-attestation": "{}"}, "body": ANSWER, "delay": 0}

    async def handle(request):
        seen.append(
            {
                "path": request.path,
                "authorization": request.headers.get("Authorization"),
                "content_type": request.headers.get("Content-Type"),
                "body": await request.json(),
            }
        )
        await asyncio.sleep(behaviour["delay"])
        return web.json_response(behaviour["body"], headers=behaviour["headers"])

    app = web.Application()
    app.router.add_post("/api/deployments/id/{deployment}/indexers/id/{indexer}", handle)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    yield server, seen, behaviour
    await server.close()


async def test_sends_the_query_to_the_single_indexer_route_with_the_key_in_a_header(fake_gateway):
    server, seen, _ = fake_gateway
    base = str(server.make_url("/api"))

    async with GatewayClient(base + "/", FAKE_KEY, timeout=5, clock=lambda: 42.0) as gateway:
        reply = await gateway.ask(DEPLOYMENT, INDEXER)

    assert seen == [
        {
            "path": f"/api/deployments/id/{DEPLOYMENT}/indexers/id/{INDEXER}",
            "authorization": f"Bearer {FAKE_KEY}",
            "content_type": "application/json",
            "body": {"query": FRESHNESS_QUERY},
        }
    ]
    assert reply == GatewayReply(200, True, json.dumps(ANSWER).encode(), 42.0)


async def test_a_reply_without_the_attestation_header_is_marked_unattested(fake_gateway):
    server, _, behaviour = fake_gateway
    behaviour.update(headers={}, body={"errors": [{"message": "no indexers found"}]})

    async with GatewayClient(str(server.make_url("/api")), FAKE_KEY, timeout=5) as gateway:
        reply = await gateway.ask(DEPLOYMENT, INDEXER)

    assert isinstance(reply, GatewayReply)
    assert reply.attested is False


async def test_no_answer_within_the_timeout_is_unreachable(fake_gateway):
    server, _, behaviour = fake_gateway
    behaviour["delay"] = 2

    async with GatewayClient(str(server.make_url("/api")), FAKE_KEY, timeout=0.2) as gateway:
        reply = await gateway.ask(DEPLOYMENT, INDEXER)

    assert reply == GatewayUnreachable("no answer within 0.2s")


async def test_a_refused_connection_is_unreachable_and_never_shows_the_key():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        closed_port = sock.getsockname()[1]

    async with GatewayClient(f"http://127.0.0.1:{closed_port}/api", FAKE_KEY, 5) as gateway:
        reply = await gateway.ask(DEPLOYMENT, INDEXER)

    assert isinstance(reply, GatewayUnreachable)
    assert reply.detail.startswith("connection error: ")
    assert FAKE_KEY not in reply.detail


async def test_a_gateway_message_repeating_the_key_never_shows_any_of_it(fake_gateway):
    # What gets stored is cut to 300 characters, so a key lying across that cut must already
    # be gone, or a piece of it would be left behind.
    server, _, behaviour = fake_gateway
    message = "auth error: " + "x" * 278 + FAKE_KEY + " is not allowed"
    behaviour.update(headers={}, body={"errors": [{"message": message}]})

    async with GatewayClient(str(server.make_url("/api")), FAKE_KEY, timeout=5) as gateway:
        reply = await gateway.ask(DEPLOYMENT, INDEXER)

    assert isinstance(reply, GatewayReply)
    assert FAKE_KEY.encode() not in reply.body
    detail = judge(reply, 36).detail
    assert detail is not None and detail.endswith("<redacted>")
    assert FAKE_KEY[:6] not in detail
