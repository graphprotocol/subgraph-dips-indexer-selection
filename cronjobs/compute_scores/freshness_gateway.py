"""Sends the freshness query to 1 named indexer on 1 named deployment through the gateway.

The gateway forwards it with a paid receipt, as it would a customer's query, so the answer is
what a customer would have received. The API key goes in the Authorization header: this route
doesn't take it in the path.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Callable, Optional

import aiohttp
from freshness_rules import ATTESTATION_HEADER, GatewayReply, GatewayResult, GatewayUnreachable
from subgraph import redact_url

FRESHNESS_QUERY = "{ _meta { block { number timestamp } } }"
_BODY = json.dumps({"query": FRESHNESS_QUERY}).encode()


class GatewayClient:
    """Use as ``async with GatewayClient(...) as gateway`` so 1 connection pool serves every
    check."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self._api_key = api_key
        self._timeout = timeout
        self._clock = clock
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "GatewayClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def route(self, deployment: str, indexer: str) -> str:
        return f"{self._base_url}/deployments/id/{deployment}/indexers/id/{indexer}"

    def _unreachable(self, detail: str) -> GatewayUnreachable:
        # Error text can carry the request URL; keep any key out of what gets pushed and shown.
        return GatewayUnreachable(redact_url(detail).replace(self._api_key, "<redacted>"))

    async def ask(self, deployment: str, indexer: str) -> GatewayResult:
        """Send the query once and return what came back, or why nothing did."""
        if self._session is None:
            raise RuntimeError("GatewayClient used outside 'async with'")
        try:
            async with self._session.post(
                self.route(deployment, indexer),
                data=_BODY,
                headers=self._headers,
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            ) as response:
                body = await response.read()
                return GatewayReply(
                    status=response.status,
                    attested=ATTESTATION_HEADER in response.headers,
                    body=body,
                    arrived_at=self._clock(),
                )
        except asyncio.TimeoutError:
            return self._unreachable(f"no answer within {self._timeout:g}s")
        except aiohttp.ClientError as e:
            return self._unreachable(f"connection error: {type(e).__name__}: {e}")
