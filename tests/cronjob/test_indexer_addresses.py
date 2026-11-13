"""IISA calls indexers only at public addresses.

An indexer chooses the URL it registers. These tests check that IISA refuses one that points,
directly, by a name or by a redirect, at an address inside its own network, and that the refusal
happens before any connection is made.
"""

import ipaddress
import socket
import sys
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import indexer_addresses  # noqa: E402
from indexer_addresses import (  # noqa: E402
    AIOHTTP_MAX_REDIRECTS,
    ALLOW_PRIVATE_ADDRESSES_ENV,
    AddressRule,
    Refused,
    is_public,
    refusal_behind,
    refused_url,
)

NOT_PUBLIC = [
    "0.0.0.0",
    "10.0.0.5",
    "100.64.0.1",
    "127.0.0.1",
    "168.63.129.16",
    "169.254.169.254",
    "172.16.0.1",
    "172.31.255.255",
    "192.0.0.1",
    "192.0.2.1",
    "192.168.1.1",
    "198.18.0.1",
    "198.51.100.1",
    "203.0.113.1",
    "224.0.0.1",
    "240.0.0.1",
    "255.255.255.255",
    "::",
    "::1",
    "fe80::1",
    "fc00::1",
    "ff02::1",
    "2001::1",
    "2001:db8::1",
    "3fff::1",
    # IPv6 forms that are delivered to an IPv4 address: mapped, NAT64 and 6to4, here to 10.0.0.1
    # and to the metadata service.
    "::ffff:10.0.0.1",
    "::ffff:169.254.169.254",
    "64:ff9b::a00:1",
    "2002:a00:1::",
    "2002:a9fe:a9fe::1",
]

PUBLIC = [
    "1.1.1.1",
    "8.8.8.8",
    "172.15.255.255",
    "172.32.0.1",
    "168.63.129.17",
    "2606:4700:4700::1111",
    "::ffff:8.8.8.8",
    "64:ff9b::808:808",
    "2002:808:808::1",
]


@pytest.mark.parametrize("address", NOT_PUBLIC)
def test_an_address_that_is_not_public_is_refused(address):
    assert not is_public(ipaddress.ip_address(address))


@pytest.mark.parametrize("address", PUBLIC)
def test_a_public_address_is_allowed(address):
    assert is_public(ipaddress.ip_address(address))


def test_the_list_of_ipv4_ranges_matches_dippers():
    # Dipper refuses the same 15 ranges. A range added on 1 side should be added on the other.
    assert sorted(str(network) for network in indexer_addresses._NOT_PUBLIC_V4) == sorted(
        [
            "0.0.0.0/8",
            "10.0.0.0/8",
            "100.64.0.0/10",
            "127.0.0.0/8",
            "168.63.129.16/32",
            "169.254.0.0/16",
            "172.16.0.0/12",
            "192.0.0.0/24",
            "192.0.2.0/24",
            "192.168.0.0/16",
            "198.18.0.0/15",
            "198.51.100.0/24",
            "203.0.113.0/24",
            "224.0.0.0/4",
            "240.0.0.0/4",
        ]
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://10.0.0.5:7600/dips/info",
        "http://169.254.169.254/latest/meta-data/",
        "https://127.0.0.1/status",
        "http://[::1]:7600/status",
        "http://[::ffff:10.0.0.1]/status",
        "http://[fe80::1%25eth0]/status",
        "ftp://indexer.example/status",
        "file:///etc/passwd",
        "indexer.example/status",
        "http:///status",
    ],
)
def test_a_url_iisa_must_not_call_is_refused_before_any_connection(url):
    with pytest.raises(Refused):
        AddressRule().check_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://indexer.example/dips/info",
        "https://indexer.example:7600/status",
        "http://8.8.8.8/status",
        "https://[2606:4700:4700::1111]/status",
    ],
)
def test_a_url_at_a_name_or_a_public_address_passes_the_first_check(url):
    AddressRule().check_url(url)


def test_a_url_that_cannot_be_read_is_left_for_the_http_library_to_refuse():
    # aiohttp reads URLs with the same parser, so it fails on this one before any connection.
    unreadable = "http://[::1"

    AddressRule().check_url(unreadable)
    with pytest.raises(ValueError):
        aiohttp.client.URL(unreadable)


# A label that starts "xn--" but is not valid punycode. Decoding it fails, so a check that
# decoded the host would raise on a URL any indexer can register.
NOT_VALID_PUNYCODE = ["https://xn--a.example", "http://xn--/", "https://indexer.xn--zz"]


@pytest.mark.parametrize("allow_private_addresses", [False, True])
@pytest.mark.parametrize("url", NOT_VALID_PUNYCODE)
def test_a_host_that_is_not_valid_punycode_is_judged_as_written(url, allow_private_addresses):
    AddressRule(allow_private_addresses=allow_private_addresses).check_url(url)


@pytest.mark.parametrize("url", NOT_VALID_PUNYCODE)
def test_the_first_check_never_raises_on_a_registered_url(url, monkeypatch):
    monkeypatch.delenv(ALLOW_PRIVATE_ADDRESSES_ENV, raising=False)

    assert refused_url(url) is None


def test_a_refusal_says_how_a_local_network_lifts_it():
    with pytest.raises(Refused) as refused:
        AddressRule().check_url("http://10.0.0.5/status")

    assert "10.0.0.5" in str(refused.value)
    assert "only a local network" in str(refused.value)
    assert ALLOW_PRIVATE_ADDRESSES_ENV in str(refused.value)


def test_private_addresses_are_refused_unless_the_setting_says_true(monkeypatch):
    monkeypatch.delenv(ALLOW_PRIVATE_ADDRESSES_ENV, raising=False)
    assert not AddressRule.from_env().allow_private_addresses
    for off in ("", "false", "0", "no", "maybe"):
        monkeypatch.setenv(ALLOW_PRIVATE_ADDRESSES_ENV, off)
        assert not AddressRule.from_env().allow_private_addresses, off
    for on in ("true", "TRUE", " 1 ", "yes", "on"):
        monkeypatch.setenv(ALLOW_PRIVATE_ADDRESSES_ENV, on)
        assert AddressRule.from_env().allow_private_addresses, on


def test_a_local_network_may_call_private_addresses_but_still_only_over_http():
    rule = AddressRule(allow_private_addresses=True)

    rule.check_url("http://10.0.0.5:7600/status")
    assert rule.allows(ipaddress.ip_address("169.254.169.254"))
    with pytest.raises(Refused):
        rule.check_url("ftp://10.0.0.5/status")


def _resolved(*hosts):
    return [{"hostname": "indexer.example", "host": host, "port": 80} for host in hosts]


def test_a_name_that_resolves_only_inside_the_network_is_refused():
    with pytest.raises(Refused) as refused:
        AddressRule().keep_allowed("indexer.example", _resolved("10.0.0.5", "169.254.169.254"))

    assert "indexer.example" in str(refused.value)
    assert "10.0.0.5" in str(refused.value)


def test_a_name_keeps_only_its_public_addresses():
    kept = AddressRule().keep_allowed("indexer.example", _resolved("10.0.0.5", "8.8.8.8"))

    assert [entry["host"] for entry in kept] == ["8.8.8.8"]


def test_a_refusal_is_found_behind_the_http_librarys_connection_error():
    refusal = Refused("refused 10.0.0.5")

    class _Wrapped(Exception):
        os_error = refusal

    assert refusal_behind(refusal) is refusal
    assert refusal_behind(_Wrapped()) is refusal
    assert refusal_behind(ConnectionResetError("reset")) is None
    assert refusal_behind(None) is None


class _Hits:
    """Counts the requests a test server received."""

    def __init__(self):
        self.count = 0

    def handler(self, body="ok"):
        async def handle(request):
            self.count += 1
            return web.Response(text=body)

        return handle


async def test_a_session_never_connects_to_an_address_that_is_not_public():
    # The URL names the server's loopback address outright, so no name lookup happens: the check
    # made on the address about to be connected to is what stops it.
    hits = _Hits()
    app = web.Application()
    app.router.add_get("/", hits.handler())

    async with TestServer(app) as server, AddressRule().session() as session:
        with pytest.raises(aiohttp.ClientConnectorError) as error:
            await session.get(str(server.make_url("/")))

    assert refusal_behind(error.value) is not None
    assert "127.0.0.1" in str(refusal_behind(error.value))
    assert hits.count == 0


async def test_a_session_never_connects_to_a_name_that_resolves_inside_the_network():
    hits = _Hits()
    app = web.Application()
    app.router.add_get("/", hits.handler())

    async with TestServer(app) as server, AddressRule().session() as session:
        with pytest.raises(aiohttp.ClientConnectorError) as error:
            await session.get(f"http://localhost:{server.port}/")

    refusal = refusal_behind(error.value)
    assert refusal is not None
    assert "localhost" in str(refusal)
    assert hits.count == 0


async def test_a_session_refuses_a_redirect_to_an_address_that_is_not_public(monkeypatch):
    # The test server stands in for a public indexer, so for this test its loopback address
    # counts as public. It answers with a redirect to the cloud metadata service.
    monkeypatch.setattr(
        indexer_addresses, "is_public", lambda ip: str(ip) == "127.0.0.1", raising=True
    )
    hits = _Hits()

    async def redirect(request):
        hits.count += 1
        raise web.HTTPFound("http://169.254.169.254/latest/meta-data/")

    app = web.Application()
    app.router.add_get("/", redirect)

    async with TestServer(app) as server, AddressRule().session() as session:
        with pytest.raises(aiohttp.ClientConnectorError) as error:
            await session.get(str(server.make_url("/")), max_redirects=AIOHTTP_MAX_REDIRECTS)

    assert hits.count == 1
    refusal = refusal_behind(error.value)
    assert refusal is not None
    assert "169.254.169.254" in str(refusal)


async def test_a_session_with_private_addresses_allowed_reaches_a_local_server():
    hits = _Hits()
    app = web.Application()
    app.router.add_get("/", hits.handler("hello"))

    rule = AddressRule(allow_private_addresses=True)
    async with TestServer(app) as server, rule.session() as session:
        async with session.get(str(server.make_url("/"))) as reply:
            assert await reply.text() == "hello"

    assert hits.count == 1


def test_the_socket_check_opens_a_socket_only_for_an_allowed_address():
    rule = AddressRule()
    public = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", 443))
    private = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.5", 443))
    linked = (
        socket.AF_INET6,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        "",
        ("fe80::1%lo0", 443, 0, 1),
    )

    opened = rule.socket_for(public)
    opened.close()
    for addr_info in (private, linked):
        with pytest.raises(Refused):
            rule.socket_for(addr_info)
