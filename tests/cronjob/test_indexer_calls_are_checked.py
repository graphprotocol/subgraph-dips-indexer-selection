"""Every call IISA makes to a URL an indexer registered goes through the address rule.

There are 3 such calls: the scoring job's price fetch and graph-node version fetch, and the
sync-status fetcher's status fetch. Each must refuse an address that is not public before making
any connection, treat the indexer as unreachable, say so in the log, and not try again.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.client_reqrep import ConnectionKey
from aiohttp.test_utils import TestServer

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import processing  # noqa: E402
import sync_status_fetcher  # noqa: E402
from indexer_addresses import ALLOW_PRIVATE_ADDRESSES_ENV, Refused  # noqa: E402

METADATA_SERVICE = "http://169.254.169.254"
INDEXER = "0x1111111111111111111111111111111111111111"


class _Session:
    """A session that counts the calls it is asked to make and fails each with `error`."""

    def __init__(self, error=None):
        self.calls = 0
        self._error = error or AssertionError("no call should have been made")

    def _call(self, *args, **kwargs):
        self.calls += 1
        error = self._error

        class _Failing:
            async def __aenter__(self):
                raise error

            async def __aexit__(self, *exc):
                return False

        return _Failing()

    get = post = _call


def _refused_while_connecting():
    """What a call raises when the name it went to resolved only inside the network."""
    key = ConnectionKey("indexer.example", 80, False, True, None, None, None)
    return aiohttp.ClientConnectorError(key, Refused("refused indexer.example: 10.0.0.5"))


async def _price(session, url):
    record = await processing._fetch_single_dips_info_async(
        session, INDEXER, url, asyncio.Semaphore(1)
    )
    return record["dips_info_available"]


async def _version(session, url):
    record = await processing._fetch_single_graph_node_version_async(
        session, INDEXER, url, asyncio.Semaphore(1)
    )
    return record["graph_node_version"]


async def _status(session, url):
    return await sync_status_fetcher._fetch_single_status(
        session, INDEXER, url, asyncio.Semaphore(1)
    )


# Each fetch, with what it returns for an indexer it could not reach.
FETCHES = [(_price, False), (_version, None), (_status, None)]


@pytest.fixture(autouse=True)
def _default_rule(monkeypatch):
    monkeypatch.delenv(ALLOW_PRIVATE_ADDRESSES_ENV, raising=False)


@pytest.mark.parametrize("fetch, unreachable", FETCHES)
@pytest.mark.parametrize(
    "url",
    [METADATA_SERVICE, "http://10.0.0.5:7600", "http://[::1]:7600", "ftp://indexer.example"],
)
async def test_an_indexer_at_an_address_that_is_not_public_is_never_called(
    fetch, unreachable, url, caplog
):
    session = _Session()

    with caplog.at_level(logging.WARNING):
        result = await fetch(session, url)

    assert result is unreachable
    assert session.calls == 0
    refusals = [r for r in caplog.records if "refused" in r.getMessage()]
    assert len(refusals) == 1, caplog.text
    assert refusals[0].levelno == logging.WARNING


@pytest.mark.parametrize("fetch, unreachable", FETCHES)
async def test_a_call_refused_while_connecting_is_not_tried_again(fetch, unreachable, caplog):
    session = _Session(error=_refused_while_connecting())

    with caplog.at_level(logging.WARNING):
        result = await fetch(session, "http://indexer.example")

    assert result is unreachable
    assert session.calls == 1
    assert any("refused indexer.example" in r.getMessage() for r in caplog.records), caplog.text


@pytest.mark.parametrize("fetch, unreachable", FETCHES)
async def test_a_host_that_is_not_valid_punycode_leaves_the_indexer_unreachable(
    fetch, unreachable, monkeypatch
):
    # The fetches run together, so 1 that raised would fail them all, for every indexer.
    monkeypatch.setattr(processing, "DIPS_INFO_MAX_RETRIES", 1)
    monkeypatch.setattr(processing, "GRAPH_NODE_VERSION_MAX_RETRIES", 1)
    monkeypatch.setattr(sync_status_fetcher, "MAX_RETRIES", 1)
    session = _Session(error=aiohttp.ClientConnectionError("no such host"))

    assert await fetch(session, "https://xn--a.example") is unreachable


def _indexer_app(hits):
    """A stand-in indexer that answers all 3 calls and counts them."""

    async def dips_info(request):
        hits.append("dips")
        return web.json_response(
            {
                "pricing": {
                    "min_grt_per_30_days": {"mainnet": "450"},
                    "min_grt_per_billion_entities_per_30_days": "200",
                },
                "supported_networks": ["mainnet"],
            }
        )

    async def status(request):
        hits.append("status")
        query = (await request.json())["query"]
        if "version" in query:
            return web.json_response({"data": {"version": {"version": "0.40.0", "commit": "abc"}}})
        return web.json_response(
            {
                "data": {
                    "indexingStatuses": [
                        {"subgraph": "QmDeployment", "synced": True, "health": "healthy"}
                    ]
                }
            }
        )

    app = web.Application()
    app.router.add_get("/dips/info", dips_info)
    app.router.add_post("/status", status)
    return app


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
async def test_the_real_fetches_leave_a_server_inside_the_network_alone(host):
    # The stand-in indexer listens on this machine's loopback address, which is not public. The
    # address is refused before any call. The name passes that first check, so only the sessions
    # the 3 fetches open for themselves can keep it from being reached.
    hits = []
    async with TestServer(_indexer_app(hits), host="127.0.0.1") as server:
        urls = {INDEXER: f"http://{host}:{server.port}/"}

        prices = await processing._fetch_all_dips_info_async(urls)
        versions = await processing._fetch_all_graph_node_versions_async(urls)
        with pytest.raises(sync_status_fetcher.NoIndexerRespondedError):
            await sync_status_fetcher._fetch_all_statuses(urls)

    assert hits == []
    assert prices[0]["dips_info_available"] is False
    assert versions[0]["graph_node_version"] is None


async def test_a_local_network_can_switch_the_rule_off(local_indexers_allowed):
    hits = []
    async with TestServer(_indexer_app(hits)) as server:
        urls = {INDEXER: str(server.make_url("/"))}

        prices = await processing._fetch_all_dips_info_async(urls)
        versions = await processing._fetch_all_graph_node_versions_async(urls)
        statuses = await sync_status_fetcher._fetch_all_statuses(urls)

    assert sorted(hits) == ["dips", "status", "status"]
    assert prices[0]["dips_info_available"] is True
    assert json.loads(prices[0]["dips_min_grt_per_30_days"]) == {"mainnet": 450.0}
    assert versions[0]["graph_node_version"] == "0.40.0"
    assert statuses[INDEXER]["deployments"] == ["QmDeployment"]


@pytest.mark.parametrize("fetch, unreachable", FETCHES)
async def test_a_fetch_follows_at_most_3_redirects(
    fetch, unreachable, local_indexers_allowed, monkeypatch
):
    # 1 try each, so the count below is of 1 call's redirects, not of retries.
    monkeypatch.setattr(processing, "DIPS_INFO_MAX_RETRIES", 1)
    monkeypatch.setattr(processing, "GRAPH_NODE_VERSION_MAX_RETRIES", 1)
    monkeypatch.setattr(sync_status_fetcher, "MAX_RETRIES", 1)
    hits = []

    async def bounce(request):
        hits.append(request.path)
        # A temporary redirect keeps the method, so a POST stays a POST.
        raise web.HTTPTemporaryRedirect(request.path)

    app = web.Application()
    app.router.add_get("/dips/info", bounce)
    app.router.add_post("/status", bounce)
    async with TestServer(app) as server:
        async with aiohttp.ClientSession() as session:
            result = await fetch(session, str(server.make_url("/")))

    assert result is unreachable
    # The call itself and the 3 redirects it may follow. Without the limit it would follow 10.
    assert len(hits) == 4
