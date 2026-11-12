"""Checks an indexer's claim to have synced a deployment before selection relies on it.

Selection prefers an indexer that says it has already synced the deployment, so queries can be
served straight after the agreement is accepted. That claim is the indexer's own word. Each time
a selection weighs such claims, the service first asks every indexer whose claim it weighed,
through the gateway, for the deployment's latest block, as a paying customer would. A claim is
confirmed only if the indexer signed the answer and the block is recent. Each check is 1 paid
query on the gateway API key, so none is made for a group that needs nobody new.
"""

import asyncio
import json
import logging
import math
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http.client import HTTPException
from typing import Any, Callable, Optional, Sequence
from urllib.parse import quote

logger = logging.getLogger(__name__)

# The gateway puts the indexer's signature on answers the indexer gave and leaves it off its
# own messages, so only this header says an answer came from the indexer.
ATTESTATION_HEADER = "graph-attestation"
QUERY = "{ _meta { block { number timestamp } } }"
_BODY = json.dumps({"query": QUERY}).encode()

# The answer is a few dozen bytes; anything much larger is not an answer to this query.
MAX_REPLY_BYTES = 64 * 1024
# A block dated up to this long after now still counts as 0 seconds old, allowing for clocks
# that disagree a little; any later and the timestamp cannot be trusted.
FUTURE_TOLERANCE_SECONDS = 60
# The most claims 1 selection will pay to check. Beyond it the first listed are checked and the
# rest are set aside unchecked, so a flood of claims cannot run up the bill.
MAX_CHECKS_PER_SELECTION = 50
# Enough threads for every check 1 selection may make, so they all start at once. Selections in
# progress at the same moment share them, and a check that finds none free waits its turn.
CHECK_THREADS = MAX_CHECKS_PER_SELECTION
# Sent with each query. Some gateways' front doors refuse the HTTP library's own default name.
USER_AGENT = "iisa-sync-check"


@dataclass(frozen=True)
class GatewayReply:
    """What came back from the gateway: its status, whether the indexer signed it, the body."""

    status: int
    attested: bool
    body: bytes


Send = Callable[[str, dict[str, str], bytes, float], GatewayReply]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuses every redirect, so the API key is never sent on to another address."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def post_once(url: str, headers: dict[str, str], body: bytes, timeout: float) -> GatewayReply:
    """POST body to url once and return the answer. Connecting, and each wait for more of the
    answer, may take up to timeout seconds, and the body must arrive within timeout of the
    start; the caller's own time limit is what bounds the whole check.

    Raises an OSError, such as a timeout or a refused connection, when no answer arrives, and
    ValueError when the answer is larger than MAX_REPLY_BYTES.
    """
    deadline = time.monotonic() + timeout
    request = urllib.request.Request(
        url, data=body, headers={"User-Agent": USER_AGENT, **headers}, method="POST"
    )
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            attested = ATTESTATION_HEADER in response.headers
            return GatewayReply(response.status, attested, _read(response, deadline))
    except urllib.error.HTTPError as e:
        # An answer other than 2xx, which includes a redirect this client refused to follow.
        with e:
            return GatewayReply(e.code, False, b"")


def _read(response: Any, deadline: float) -> bytes:
    """Read the whole body, stopping at the size limit or the deadline."""
    chunks: list[bytes] = []
    size = 0
    while chunk := response.read1(8192):
        size += len(chunk)
        if size > MAX_REPLY_BYTES:
            raise ValueError(f"answer is over the {MAX_REPLY_BYTES:,} byte limit")
        if time.monotonic() > deadline:
            raise TimeoutError("answer did not finish arriving in time")
        chunks.append(chunk)
    return b"".join(chunks)


class SyncClaimChecker:
    """Asks the gateway whether an indexer is serving a deployment at a recent block."""

    def __init__(
        self,
        gateway_url: str,
        api_key: str,
        timeout_seconds: float,
        bar_seconds: float,
        send: Send = post_once,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._gateway_url = gateway_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self._timeout = timeout_seconds
        self._bar = bar_seconds
        self._send = send
        self._clock = clock
        self._threads = ThreadPoolExecutor(CHECK_THREADS, thread_name_prefix="sync-check")

    async def unconfirmed(self, deployment_id: str, indexers: Sequence[str]) -> dict[str, str]:
        """Check each indexer's claim on deployment_id, all at once, and return each one that
        was not confirmed with the reason. A check still unanswered after the time limit is
        given up, so this never waits much longer than that limit however many are asked.

        The checks are handed over in the order given. When other selections hold some of the
        threads, the first listed start first, and one that never got to start says so.
        """
        asked = list(dict.fromkeys(indexers))
        if not asked:
            return {}
        sent = [
            self._threads.submit(self.why_not_confirmed, deployment_id, indexer)
            for indexer in asked
        ]
        answers = [asyncio.wrap_future(check) for check in sent]
        _done, late = await asyncio.wait(answers, timeout=self._timeout)
        reasons: dict[str, str] = {}
        for indexer, check, answer in zip(asked, sent, answers):
            if answer in late:
                # Cancelling only succeeds for a check that had not started.
                never_started = check.cancel()
                answer.cancel()
                reasons[indexer] = (
                    f"the check was not sent, the service was busy for {self._timeout:g}s"
                    if never_started
                    else f"no answer within {self._timeout:g}s"
                )
            elif (reason := self._outcome(answer, deployment_id, indexer)) is not None:
                reasons[indexer] = reason
        return reasons

    @staticmethod
    def _outcome(
        check: "asyncio.Future[Optional[str]]", deployment_id: str, indexer: str
    ) -> Optional[str]:
        """A finished check's answer. A check that broke counts as unconfirmed, and is logged,
        so a fault in the check can never fail the selection it was made for.
        """
        try:
            return check.result()
        except Exception:
            logger.exception("Check of the synced claim of %s on %s broke", indexer, deployment_id)
            return "the check itself failed"

    def why_not_confirmed(self, deployment_id: str, indexer: str) -> Optional[str]:
        """Ask the gateway once. None when the claim is confirmed, otherwise why it is not."""
        url = (
            f"{self._gateway_url}/deployments/id/{quote(deployment_id, safe='')}"
            f"/indexers/id/{quote(indexer, safe='')}"
        )
        try:
            reply = self._send(url, self._headers, _BODY, self._timeout)
        except (OSError, ValueError, HTTPException) as e:
            # Only the kind of failure is kept: its text can carry the address that was asked.
            return f"the gateway could not be read ({type(e).__name__})"
        return self._judge(reply)

    def _judge(self, reply: GatewayReply) -> Optional[str]:
        if reply.status != 200:
            return f"the gateway answered {reply.status}"
        if not reply.attested:
            return "the answer was not signed by the indexer"
        timestamp = _block_timestamp(reply.body)
        if timestamp is None:
            return "the answer has no block timestamp"
        age = self._clock() - timestamp
        if age < -FUTURE_TOLERANCE_SECONDS:
            return f"the latest block is dated {-age:.0f}s in the future"
        if age > self._bar:
            return f"the latest block is {age:.0f}s old, over the {self._bar:g}s allowed"
        return None


def _block_timestamp(body: bytes) -> Optional[float]:
    """The latest block's unix time in an answer to QUERY, or None when it holds none."""
    try:
        value: Any = json.loads(body)
    except (ValueError, RecursionError):
        return None
    for key in ("data", "_meta", "block", "timestamp"):
        value = value.get(key) if isinstance(value, dict) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value >= 0 else None
