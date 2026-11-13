"""Which addresses IISA may call an indexer at.

An indexer chooses the URL it registers, so unchecked it could point IISA, directly, by a name or
by a redirect, at a service inside IISA's own network, such as the cloud metadata service. IISA
refuses every address that is not public. Dipper applies the same rule, with the same list of
addresses, to its own calls to indexers.

This module imports nothing heavy at load, so the sync-status fetcher can use it as well as the
scoring job.
"""

from __future__ import annotations

import errno
import functools
import ipaddress
import logging
import os
import socket
from typing import TYPE_CHECKING, Any, Optional, Union

from escaping import error_for_log, for_log, url_for_log

if TYPE_CHECKING:
    import aiohttp

logger = logging.getLogger(__name__)

IpAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

# The setting that lifts the rule. A local network's indexers sit at private addresses, so it
# needs this; anywhere else it would let an indexer point IISA at IISA's own network.
ALLOW_PRIVATE_ADDRESSES_ENV = "INDEXER_URLS_ALLOW_PRIVATE_ADDRESSES"
_TRUE_VALUES = frozenset({"true", "1", "yes", "on"})

# Ends each refusal the setting can lift, so an operator learns of it without reading it as advice
# to turn it on anywhere but a local network.
ONLY_FOR_LOCAL_NETWORKS = (
    f"only a local network should allow private addresses, with {ALLOW_PRIVATE_ADDRESSES_ENV}=true"
)

# The most redirects IISA follows in 1 call to an indexer. Indexer software answers in place, so a
# redirect comes from a server in front of it, such as one from http to https.
MAX_REDIRECTS = 3
# What to pass as aiohttp's `max_redirects` to follow that many. aiohttp gives up at the redirect
# that reaches its number, so it follows 1 fewer than it is given.
AIOHTTP_MAX_REDIRECTS = MAX_REDIRECTS + 1

# IPv4 ranges that are not public, from IANA's special-purpose address registry plus 1 cloud
# address. Kept the same as dipper's list.
_NOT_PUBLIC_V4 = tuple(
    ipaddress.IPv4Network(network)
    for network in (
        "0.0.0.0/8",  # "This network".
        "10.0.0.0/8",  # Private networks.
        "100.64.0.0/10",  # Shared by an internet provider's customers behind its own translation.
        "127.0.0.0/8",  # The machine itself.
        # Azure's host endpoint for health probes, name lookups and its machine agent. It lies in
        # public space, but every Azure machine reaches it inside Azure.
        "168.63.129.16/32",
        "169.254.0.0/16",  # A single network link, home to the cloud metadata service.
        "172.16.0.0/12",  # Private networks.
        "192.0.0.0/24",  # Protocol assignments.
        "192.0.2.0/24",  # Documentation.
        "192.168.0.0/16",  # Private networks.
        "198.18.0.0/15",  # Benchmarking.
        "198.51.100.0/24",  # Documentation.
        "203.0.113.0/24",  # Documentation.
        "224.0.0.0/4",  # Multicast.
        "240.0.0.0/4",  # Reserved, and the broadcast address.
    )
)

# The IPv6 range that holds every public address. The rest of IPv6 is unassigned, or kept for a
# single network or machine, such as ::1, fe80::/10 and fc00::/7.
_GLOBAL_UNICAST_V6 = ipaddress.IPv6Network("2000::/3")

# Ranges inside the public IPv6 range that are not public either.
_NOT_PUBLIC_V6 = tuple(
    ipaddress.IPv6Network(network)
    for network in (
        "2001::/23",  # Protocol assignments, such as tunnels and benchmarking.
        "2001:db8::/32",  # Documentation.
        "3fff::/20",  # Documentation.
    )
)

# ::ffff:a.b.c.d, which a socket connects to over IPv4, at a.b.c.d.
_MAPPED_V4 = ipaddress.IPv6Network("::ffff:0:0/96")
# 64:ff9b::a.b.c.d, which an IPv6 to IPv4 gateway forwards to a.b.c.d.
_NAT64 = ipaddress.IPv6Network("64:ff9b::/96")
# 2002:aabb:ccdd::/48, which is tunnelled over IPv4 to aa.bb.cc.dd.
_SIX_TO_FOUR = ipaddress.IPv6Network("2002::/16")


class Refused(OSError):
    """IISA's refusal to connect to an indexer, at an address that is not public or by a URL it
    can't judge.

    An OSError, so that raised while a connection is being made it reaches the caller the way any
    failed connection does, and `refusal_behind` can then tell it apart.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(errno.EACCES, reason)
        self.reason = reason

    def __str__(self) -> str:
        return self.reason


def is_public(ip: IpAddress) -> bool:
    """Whether `ip` is public: reachable across the internet, rather than kept for a private
    network, the machine itself, documentation or another special purpose.
    """
    if isinstance(ip, ipaddress.IPv4Address):
        return not any(ip in network for network in _NOT_PUBLIC_V4)
    # An address that carries an IPv4 address inside, and is delivered to it, is as public as that
    # IPv4 address.
    packed = ip.packed
    if ip in _MAPPED_V4 or ip in _NAT64:
        return is_public(ipaddress.IPv4Address(packed[12:16]))
    if ip in _SIX_TO_FOUR:
        return is_public(ipaddress.IPv4Address(packed[2:6]))
    return ip in _GLOBAL_UNICAST_V6 and not any(ip in network for network in _NOT_PUBLIC_V6)


def _as_address(text: str) -> Optional[IpAddress]:
    """`text` as an address, or None when it is a name. An IPv6 zone, as in `fe80::1%eth0`, is
    dropped: it names a network link, and the address before it decides."""
    try:
        return ipaddress.ip_address(text.split("%", 1)[0])
    except ValueError:
        return None


class AddressRule:
    """The addresses IISA may call indexers at: public ones only, unless a local network's
    setting allows any."""

    def __init__(self, allow_private_addresses: bool = False) -> None:
        self.allow_private_addresses = allow_private_addresses

    @classmethod
    def from_env(cls) -> "AddressRule":
        """The rule the environment asks for. Read at each use, so a test or an operator changing
        the setting needs no reload."""
        raw = os.environ.get(ALLOW_PRIVATE_ADDRESSES_ENV, "").strip().lower()
        return cls(allow_private_addresses=raw in _TRUE_VALUES)

    def allows(self, ip: IpAddress) -> bool:
        """Whether IISA may connect to `ip`."""
        return self.allow_private_addresses or is_public(ip)

    def check_url(self, url: str) -> None:
        """Raise `Refused` if `url` is not http or https, or its host is written as an address
        this rule doesn't allow. A host written as a name passes: it is judged as IISA connects.

        A URL that can't be read at all passes too. The HTTP library reads URLs the same way, so
        it fails on that URL itself, before any connection. This check runs outside each call's
        own error handling, so it must never raise anything but `Refused`.
        """
        # The parser the HTTP library uses. The host is taken as written, which is what the
        # library connects to; decoding it as an international name could fail where the
        # connection would not, such as on a label that starts "xn--" but is not valid.
        from yarl import URL

        try:
            parsed = URL(url)
            host = parsed.raw_host
        except (ValueError, TypeError):
            return
        # Neither rule allows another scheme: only http and https calls go through the checks
        # made as IISA connects.
        if parsed.scheme not in ("http", "https"):
            raise Refused(
                f"refused a {parsed.scheme or 'schemeless'} URL: IISA calls indexers only over "
                "http or https"
            )
        if not host:
            raise Refused("refused a URL with no host")
        ip = _as_address(host)
        if ip is not None and not self.allows(ip):
            raise Refused(f"refused {ip}: not a public address; {ONLY_FOR_LOCAL_NETWORKS}")

    def socket_for(self, addr_info: Any) -> socket.socket:
        """The socket for 1 connection attempt, refused if its address is not allowed.

        The last check before any connection, on the address about to be used. It covers what the
        others can't see: a host written as an address in a redirect, which skips the name lookup.
        """
        family, type_, proto, _, address = addr_info
        ip = _as_address(str(address[0]))
        if ip is None or not self.allows(ip):
            raise Refused(f"refused {address[0]}: not a public address; {ONLY_FOR_LOCAL_NETWORKS}")
        return socket.socket(family=family, type=type_, proto=proto)

    def keep_allowed(self, name: str, found: list[Any]) -> list[Any]:
        """The addresses `name` resolved to that this rule allows. Raises `Refused` when none is
        left, so a name that points only inside IISA's network is never connected to."""
        allowed: list[Any] = []
        refused: list[Any] = []
        for entry in found:
            ip = _as_address(str(entry["host"]))
            (allowed if ip is not None and self.allows(ip) else refused).append(entry)
        if not allowed and refused:
            addresses = ", ".join(str(entry["host"]) for entry in refused)
            raise Refused(
                f"refused {name}: it resolves only to addresses that are not public "
                f"({addresses}); {ONLY_FOR_LOCAL_NETWORKS}"
            )
        if refused:
            logger.debug(
                "Connecting only to the public addresses %r resolves to; left out %d",
                name,
                len(refused),
            )
        return allowed

    def session(self) -> "aiohttp.ClientSession":
        """An HTTP session for calls to indexers that connects only to addresses this rule allows.

        It judges each name by what it resolves to as it connects, so a name can't pass a check
        and then point somewhere else, and it judges the address of every connection it opens,
        redirects included. It does not judge the URL a call starts at: check that with
        `check_url` first, to refuse it without a connection attempt.
        """
        import aiohttp

        connector = aiohttp.TCPConnector(
            resolver=_resolver_class()(self), socket_factory=self.socket_for
        )
        # Proxy settings in the environment are ignored: the check is worth something only if
        # IISA makes the connection itself, since a proxy would look the indexer's name up in its
        # place.
        return aiohttp.ClientSession(connector=connector, trust_env=False)


@functools.lru_cache(maxsize=1)
def _resolver_class() -> Any:
    """The name resolver the session uses, built on first use so that loading this module does
    not load aiohttp."""
    from aiohttp.abc import AbstractResolver
    from aiohttp.resolver import DefaultResolver

    class _CheckedResolver(AbstractResolver):
        """Looks names up for a connection to an indexer and keeps the addresses a rule allows."""

        def __init__(self, rule: AddressRule) -> None:
            self._rule = rule
            self._inner = DefaultResolver()

        async def resolve(
            self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
        ) -> list[Any]:
            found = await self._inner.resolve(host, port, family)
            return self._rule.keep_allowed(host, list(found))

        async def close(self) -> None:
            await self._inner.close()

    return _CheckedResolver


def refusal_behind(error: Optional[BaseException]) -> Optional[Refused]:
    """The refusal behind `error`, if IISA refused the address a call went to.

    A refusal made while connecting arrives wrapped in the HTTP library's own connection error.
    """
    for _ in range(8):
        if error is None:
            return None
        if isinstance(error, Refused):
            return error
        error = getattr(error, "os_error", None) or error.__cause__ or error.__context__
    return None


def refused_url(url: str) -> Optional[Refused]:
    """Why IISA will not call `url` under the rule the environment sets, or None when it may."""
    try:
        AddressRule.from_env().check_url(url)
    except Refused as refusal:
        return refusal
    return None


def warn_refused(
    indexer: str, url: str, refusal: Refused, *, while_connecting: bool = False
) -> None:
    """Say in the log that a call to an indexer was refused, and why. The indexer chose the URL,
    and a refusal can repeat a name from it, so both are quoted for the log.

    A refusal `while_connecting` came from a name or a redirect, after the call had started, so
    the indexer may have been reached first and the URL it registered may be public.
    """
    if while_connecting:
        message = (
            "Stopped a call to indexer %s at %s: a name or a redirect led to a refused address (%s)"
        )
    else:
        message = "Not calling indexer %s at %s: %s"
    logger.warning(message, for_log(indexer), url_for_log(url), error_for_log(refusal))
