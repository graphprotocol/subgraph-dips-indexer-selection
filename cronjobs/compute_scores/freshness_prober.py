"""The freshness prober: once a minute, ask each DIPs indexer 1 real query per chain through the
gateway, judge how old the answer's block is, and push the minute's outcomes to IISA.

It runs on its own thread inside the sync-status fetcher, with its own event loop, and only when
FRESHNESS_PROBE_ENABLED is true (freshness_config.py). The rules it applies are in
freshness_rules.py, and what it checks comes from freshness_sources.py.
"""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from functools import partial
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

import requests
from freshness_config import PUSH_DELAY_SECONDS, FreshnessConfig
from freshness_contract import without_refused
from freshness_gateway import GatewayClient
from freshness_rules import (
    KEY_OUT_OF_FUNDS,
    NO_ELIGIBLE_SUBGRAPH,
    NO_EVIDENCE,
    NOT_SENT,
    PAIRING_UNKNOWN,
    VOID,
    CheckResult,
    GatewayResult,
    Judgement,
    Targets,
    apply_general_fault,
    format_minute,
    judge,
    minute_payload,
)
from freshness_sources import TargetLoader, read_first_allocations, read_open_allocations
from iisa_client import IISAPushError, get_scores, post_freshness
from subgraph import SubgraphResponseError

logger = logging.getLogger(__name__)

MINUTE = 60
PUSH_TIMEOUT_SECONDS = 10
# A minute the loop reaches later than this is skipped: its checks could no longer be spread
# across it, and a minute that never arrives is void for everyone.
LATE_MINUTE_LIMIT_SECONDS = 5
KEY_ALARM_INTERVAL_SECONDS = 600
STOP_POLL_SECONDS = 1.0
# Room left between the last check's timeout and the push.
SEND_SLACK_SECONDS = 1.0

_EXPECTED_REFRESH_ERRORS = (
    requests.RequestException,
    SubgraphResponseError,
    IISAPushError,
    ValueError,
)

Ask = Callable[[str, str], Awaitable[GatewayResult]]


def minute_start(t: float) -> float:
    return float(int(t // MINUTE) * MINUTE)


class RateLimitedAlarm:
    """Logs an error at most once per interval and says how many it held back."""

    def __init__(self, interval: float, clock: Callable[[], float]) -> None:
        self._interval = interval
        self._clock = clock
        self._last: Optional[float] = None
        self._held = 0

    def report(self, message: str, *args: object) -> None:
        now = self._clock()
        if self._last is not None and now - self._last < self._interval:
            self._held += 1
            return
        logger.error(message + " (%d more since the last report)", *args, self._held)
        self._last = now
        self._held = 0


class FreshnessProber:
    """Refreshes what to check every ``config.refresh_interval`` and checks every minute.

    ``load_targets`` blocks (it reads GET /scores and the network subgraph), so it runs on a
    worker thread, as does ``push``. ``ask`` sends 1 query through the gateway.
    """

    def __init__(
        self,
        config: FreshnessConfig,
        *,
        load_targets: Callable[[], Targets],
        ask: Ask,
        push: Callable[[dict], Any],
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._config = config
        self._load_targets = load_targets
        self._ask = ask
        self._push_payload = push
        self._clock = clock
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self._key_alarm = RateLimitedAlarm(KEY_ALARM_INTERVAL_SECONDS, clock)
        self._targets: Optional[Targets] = None
        # (indexer, deployment) pairings the gateway said it doesn't know, until the next refresh
        self._dropped: set[tuple[str, str]] = set()
        self._refreshing = False
        self.last_push: Optional[datetime] = None
        self.running = False
        # Set once the prober's thread has ended, for whatever reason; the fetcher's /health
        # fails on it unless the fetcher is shutting down.
        self.stopped = False

    def health(self, now: datetime) -> dict[str, Any]:
        """Fields for the fetcher's /health body."""
        last = self.last_push
        return {
            "running": self.running,
            "stopped": self.stopped,
            "last_push": last.isoformat() if last else None,
            "last_push_age_seconds": int((now - last).total_seconds()) if last else None,
        }

    # -- the loop ----------------------------------------------------------

    async def run(self, stop: threading.Event) -> None:
        """Refresh and check until ``stop`` is set; a minute still in progress is not pushed."""
        self.running = True
        tasks: set[asyncio.Task] = set()
        next_refresh = self._clock()
        next_minute = minute_start(self._clock()) + MINUTE
        try:
            while not stop.is_set():
                now = self._clock()
                if now >= next_refresh:
                    if not self._refreshing:
                        tasks.add(asyncio.create_task(self.refresh()))
                    next_refresh = now + self._config.refresh_interval
                if now >= next_minute:
                    self._start_minute(next_minute, now, tasks)
                    next_minute = minute_start(now) + MINUTE
                tasks = {task for task in tasks if not task.done()}
                wake = min(next_refresh, next_minute) - self._clock()
                await self._sleep(min(STOP_POLL_SECONDS, max(0.0, wake)))
        finally:
            self.running = False
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _start_minute(self, start: float, now: float, tasks: set[asyncio.Task]) -> None:
        if self._targets is None:
            logger.debug("No freshness targets yet; not checking minute %s", format_minute(start))
            return
        if now - start > LATE_MINUTE_LIMIT_SECONDS:
            logger.warning(
                "Freshness prober reached minute %s %.0fs late; skipping it",
                format_minute(start),
                now - start,
            )
            return
        tasks.add(asyncio.create_task(self.run_minute(start)))

    async def refresh(self) -> None:
        """Replace the targets; on failure keep the previous ones until the next refresh."""
        self._refreshing = True
        try:
            targets = await asyncio.to_thread(self._load_targets)
        except Exception as e:
            logger.error(
                "Freshness refresh failed, keeping the previous lists until the next one: %s: %s",
                type(e).__name__,
                e,
                exc_info=not isinstance(e, _EXPECTED_REFRESH_ERRORS),
            )
            return
        finally:
            self._refreshing = False
        self._targets = targets
        self._dropped = set()
        logger.info(
            "Freshness targets refreshed: %d (indexer, chain) pairs, %d with eligible deployments",
            len(targets.pairs),
            sum(1 for pair in targets.pairs if targets.eligible.get(pair)),
        )

    # -- 1 minute ----------------------------------------------------------

    async def run_minute(self, start: float) -> None:
        """Check every pair at a random moment in the minute, then push the outcomes once."""
        try:
            results = apply_general_fault(await self._check_minute(start))
            await self._sleep_until(start + MINUTE + PUSH_DELAY_SECONDS)
            await self._push(start, results)
        except Exception:
            logger.exception("Freshness minute %s failed; it won't be pushed", format_minute(start))

    async def _check_minute(self, start: float) -> list[CheckResult]:
        targets = self._targets
        if targets is None:
            return []
        push_at = start + MINUTE + PUSH_DELAY_SECONDS
        # A check sent later than this might not be answered before the push.
        send_by = push_at - self._config.request_timeout - SEND_SLACK_SECONDS
        tasks = {
            pair: asyncio.create_task(
                self._check(pair, start + self._rng.uniform(0, MINUTE), send_by)
            )
            for pair in targets.pairs
        }
        if tasks:
            timeout = max(0.0, push_at - SEND_SLACK_SECONDS - self._clock())
            await asyncio.wait(tasks.values(), timeout=timeout)
        return [self._result_of(pair, task) for pair, task in tasks.items()]

    @staticmethod
    def _result_of(pair: tuple[str, str], task: asyncio.Task[CheckResult]) -> CheckResult:
        if task.done() and not task.cancelled() and task.exception() is None:
            return task.result()
        task.cancel()
        return CheckResult(*pair, VOID, NOT_SENT, "check did not finish before the push")

    async def _check(self, pair: tuple[str, str], fire_at: float, send_by: float) -> CheckResult:
        indexer, chain = pair
        try:
            await self._sleep_until(fire_at)
            return await self._check_now(indexer, chain, send_by)
        except Exception:
            logger.exception("Freshness check of %s on %s failed in the prober", indexer, chain)
            return CheckResult(indexer, chain, VOID, NOT_SENT, "prober error")

    def _eligible(self, indexer: str, chain: str) -> tuple[str, ...]:
        targets = self._targets
        return targets.eligible.get((indexer, chain), ()) if targets else ()

    @staticmethod
    def _nothing_to_ask(indexer: str, chain: str, eligible: tuple[str, ...]) -> CheckResult:
        if eligible:
            # The gateway said it doesn't know any of them; that clears at the next refresh and
            # isn't evidence against the indexer.
            detail = "the gateway doesn't know this indexer on any eligible subgraph"
            return CheckResult(indexer, chain, VOID, PAIRING_UNKNOWN, detail)
        return CheckResult(indexer, chain, NO_EVIDENCE, NO_ELIGIBLE_SUBGRAPH)

    async def _check_now(self, indexer: str, chain: str, send_by: float) -> CheckResult:
        eligible = self._eligible(indexer, chain)
        choices = [d for d in eligible if (indexer, d) not in self._dropped]
        if not choices:
            return self._nothing_to_ask(indexer, chain, eligible)
        deployment = self._rng.choice(choices)
        async with self._semaphore:
            if self._clock() > send_by:
                return CheckResult(
                    indexer, chain, VOID, NOT_SENT, "too many checks waiting to be sent", deployment
                )
            reply = await self._ask(deployment, indexer)
        judgement = judge(reply, self._config.bar_seconds)
        self._note(indexer, deployment, judgement)
        return CheckResult(
            indexer=indexer,
            chain=chain,
            outcome=judgement.outcome,
            reason=judgement.reason,
            detail=self._scrub(judgement.detail),
            deployment=deployment,
            age_seconds=judgement.age_seconds,
            attested=judgement.attested,
            asked=True,
        )

    def _scrub(self, detail: Optional[str]) -> Optional[str]:
        return detail.replace(self._config.api_key, "<redacted>") if detail else detail

    def _note(self, indexer: str, deployment: str, judgement: Judgement) -> None:
        if judgement.reason == PAIRING_UNKNOWN:
            self._dropped.add((indexer, deployment))
            logger.info(
                "Gateway doesn't know %s on %s (%s); leaving it out until the next refresh",
                indexer,
                deployment,
                judgement.detail,
            )
        elif judgement.reason == KEY_OUT_OF_FUNDS:
            self._key_alarm.report(
                "Freshness API key is out of funds or over its spend limit, so checks are void "
                "until it is topped up: %s",
                self._scrub(judgement.detail),
            )

    async def _sleep_until(self, deadline: float) -> None:
        delay = deadline - self._clock()
        if delay > 0:
            await self._sleep(delay)

    async def _push(self, start: float, results: list[CheckResult]) -> None:
        minute = format_minute(start)
        payload = minute_payload(start, results)
        payload["outcomes"] = without_refused(minute, payload["outcomes"])
        counts = Counter(outcome["outcome"] for outcome in payload["outcomes"])
        try:
            response = await asyncio.to_thread(self._push_payload, payload)
        except (IISAPushError, ValueError) as e:
            logger.error("Failed to push freshness minute %s (%s): %s", minute, dict(counts), e)
            return
        self.last_push = datetime.fromtimestamp(self._clock(), timezone.utc)
        applied = response.get("applied") if isinstance(response, dict) else None
        log = logger.info if applied is not False else logger.warning
        log("Pushed freshness minute %s (%s); applied=%s", minute, dict(counts), applied)


def _run_in_thread(prober: FreshnessProber, gateway: GatewayClient, stop: threading.Event) -> None:
    async def main() -> None:
        async with gateway:
            await prober.run(stop)

    try:
        asyncio.run(main())
    except Exception:
        logger.critical(
            "Freshness prober stopped unexpectedly; /health now fails so the pod is restarted",
            exc_info=True,
        )
    finally:
        prober.stopped = True


def start_freshness_prober(
    config: FreshnessConfig,
    stop: threading.Event,
    *,
    iisa_api_url: str,
    token: Optional[str],
    subgraph_url: str,
) -> tuple[FreshnessProber, threading.Thread]:
    """Start the prober on a daemon thread that ends once ``stop`` is set."""
    gateway = GatewayClient(config.gateway_url, config.api_key, config.request_timeout)
    loader = TargetLoader(
        fetch_scores=partial(get_scores, iisa_api_url, token),
        read_allocations=partial(read_open_allocations, subgraph_url),
        read_first_allocations=partial(read_first_allocations, subgraph_url),
        min_allocation_age_seconds=config.min_allocation_age_seconds,
    )
    prober = FreshnessProber(
        config,
        load_targets=loader,
        ask=gateway.ask,
        push=partial(post_freshness, iisa_api_url, token, timeout=PUSH_TIMEOUT_SECONDS),
    )
    logger.info(
        "Freshness prober starting: gateway host=%s, bar=%gs, min allocation age=%g days, "
        "refresh=%gs, timeout=%gs, concurrency=%d",
        urlparse(config.gateway_url).hostname,
        config.bar_seconds,
        config.min_allocation_age_days,
        config.refresh_interval,
        config.request_timeout,
        config.max_concurrency,
    )
    thread = threading.Thread(
        target=_run_in_thread, args=(prober, gateway, stop), name="freshness-prober", daemon=True
    )
    thread.start()
    return prober, thread
