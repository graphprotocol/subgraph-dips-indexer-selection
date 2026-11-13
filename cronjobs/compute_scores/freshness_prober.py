"""The freshness prober: once a minute, ask each DIPs indexer 1 real query per chain through the
gateway, judge how old the answer's block is, and push the minute's outcomes to IISA.

It runs on its own thread inside the sync-status fetcher, with its own event loop, and only when
FRESHNESS_PROBE_ENABLED is true (freshness_config.py). The rules it applies are in
freshness_rules.py, what it checks comes from freshness_sources.py, and second opinions on a
stale answer come from freshness_blame.py.
"""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from functools import partial
from typing import Any, Awaitable, Callable, Hashable, Optional, Sequence
from urllib.parse import urlparse

import requests
from freshness_blame import BrokenSubgraphs, DeploymentAges, RecentPasses
from freshness_config import PUSH_DELAY_SECONDS, FreshnessConfig
from freshness_contract import without_refused
from freshness_gateway import GatewayClient
from freshness_registry import NetworksRegistry
from freshness_rules import (
    KEY_OUT_OF_FUNDS,
    NO_ELIGIBLE_SUBGRAPH,
    NO_EVIDENCE,
    NOT_SENT,
    PAIRING_UNKNOWN,
    PAYMENT_REFUSED,
    SUBGRAPH_BROKEN,
    VOID,
    CheckResult,
    GatewayResult,
    Judgement,
    Targets,
    apply_general_fault,
    format_minute,
    is_behind,
    judge,
    minute_payload,
    truncate_detail,
)
from freshness_sources import (
    TargetLoader,
    read_allocated_indexers,
    read_deployments_on_chain,
    read_first_allocations,
    read_open_allocations,
)
from iisa_client import IISAPushError, get_scores, post_freshness
from subgraph import SubgraphResponseError

logger = logging.getLogger(__name__)

MINUTE = 60
PUSH_TIMEOUT_SECONDS = 10
# A minute the loop reaches later than this is skipped: its checks could no longer be spread
# across it, and a minute that never arrives is void for everyone.
LATE_MINUTE_LIMIT_SECONDS = 5
ALARM_INTERVAL_SECONDS = 600
STOP_POLL_SECONDS = 1.0
# Room left between the last check's timeout and the push.
SEND_SLACK_SECONDS = 1.0
# How long before that a check stops waiting for second opinions on a stale answer.
DECIDE_MARGIN_SECONDS = 0.5
# Network subgraph reads for second opinions get a pool this small of their own, so they can't
# crowd out the refresh, and the push has a thread of its own for the same reason.
SECOND_OPINION_READ_THREADS = 2

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
    """Logs at most once per interval for each key, at ``level`` (an error unless told
    otherwise), and says how many it held back."""

    def __init__(self, interval: float, clock: Callable[[], float]) -> None:
        self._interval = interval
        self._clock = clock
        self._last: dict[Hashable, float] = {}
        self._held: dict[Hashable, int] = {}

    def report(
        self, key: Hashable, message: str, *args: object, level: int = logging.ERROR
    ) -> None:
        now = self._clock()
        last = self._last.get(key)
        if last is not None and now - last < self._interval:
            self._held[key] = self._held.get(key, 0) + 1
            return
        held = self._held.pop(key, 0)
        logger.log(level, message + " (%d more since the last report)", *args, held)
        self._last[key] = now


class FreshnessProber:
    """Refreshes what to check every ``config.refresh_interval`` and checks every minute.

    ``load_targets`` blocks (it reads GET /scores and the network subgraph), so it runs on a
    worker thread. ``push`` runs on a thread of its own and the 2 network subgraph reads second
    opinions need on a small pool of their own. ``ask`` sends 1 query through the gateway.
    """

    def __init__(
        self,
        config: FreshnessConfig,
        *,
        load_targets: Callable[[], Targets],
        ask: Ask,
        push: Callable[[dict], Any],
        read_allocated_indexers: Callable[[str], list[str]],
        read_deployments: Callable[[Sequence[str], str], DeploymentAges],
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: Optional[random.Random] = None,
        registry: Optional[NetworksRegistry] = None,
    ) -> None:
        self._config = config
        self._load_targets = load_targets
        self._ask = ask
        self._push_payload = push
        self._clock = clock
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self._alarms = RateLimitedAlarm(ALARM_INTERVAL_SECONDS, clock)
        self._push_executor = ThreadPoolExecutor(1, thread_name_prefix="freshness-push")
        self._read_executor = ThreadPoolExecutor(
            SECOND_OPINION_READ_THREADS, thread_name_prefix="freshness-opinions"
        )
        self._recent = RecentPasses(clock)
        self._broken = BrokenSubgraphs(
            ask=self._ask_for_second_opinion,
            judge=partial(judge, bar_seconds=config.bar_seconds),
            read_allocated_indexers=read_allocated_indexers,
            read_deployments=read_deployments,
            recent=self._recent,
            min_allocation_age_seconds=config.min_allocation_age_seconds,
            clock=clock,
            rng=self._rng,
            executor=self._read_executor,
        )
        self._targets: Optional[Targets] = None
        # (indexer, deployment) pairings the gateway said it doesn't know, until the next refresh
        self._dropped: set[tuple[str, str]] = set()
        self._refreshing = False
        self.last_push: Optional[datetime] = None
        self.running = False
        # Set once the prober's thread has ended, for whatever reason; the fetcher's /health
        # fails on it unless the fetcher is shutting down.
        self.stopped = False
        self._registry = registry

    def health(self, now: datetime) -> dict[str, Any]:
        """Fields for the fetcher's /health body."""
        last = self.last_push
        return {
            "running": self.running,
            "stopped": self.stopped,
            "last_push": last.isoformat() if last else None,
            "last_push_age_seconds": int((now - last).total_seconds()) if last else None,
            **(self._registry.health() if self._registry else {}),
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
            self._push_executor.shutdown(wait=False, cancel_futures=True)
            self._read_executor.shutdown(wait=False, cancel_futures=True)

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
        tasks = {
            pair: asyncio.create_task(
                self._check(pair, start + self._rng.uniform(0, MINUTE), push_at)
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

    async def _check(self, pair: tuple[str, str], fire_at: float, push_at: float) -> CheckResult:
        indexer, chain = pair
        try:
            await self._sleep_until(fire_at)
            return await self._check_now(indexer, chain, push_at)
        except Exception:
            logger.exception("Freshness check of %s on %s failed in the prober", indexer, chain)
            return CheckResult(indexer, chain, VOID, NOT_SENT, "prober error")

    def _eligible(self, indexer: str, chain: str) -> tuple[str, ...]:
        targets = self._targets
        return targets.eligible.get((indexer, chain), ()) if targets else ()

    def _choices(self, indexer: str, chain: str) -> tuple[list[str], list[str]]:
        """The deployments the indexer may be asked about now, and those left out as broken."""
        known = [d for d in self._eligible(indexer, chain) if (indexer, d) not in self._dropped]
        broken = [d for d in known if self._broken.is_broken(d)]
        return [d for d in known if d not in broken], broken

    def _nothing_to_ask(self, indexer: str, chain: str, broken: list[str]) -> CheckResult:
        if broken:
            detail = "left out as broken for every indexer: " + ", ".join(broken)
            return CheckResult(indexer, chain, VOID, SUBGRAPH_BROKEN, truncate_detail(detail))
        if self._eligible(indexer, chain):
            # The gateway said it doesn't know any of them; that clears at the next refresh and
            # isn't evidence against the indexer.
            detail = "the gateway doesn't know this indexer on any eligible subgraph"
            return CheckResult(indexer, chain, VOID, PAIRING_UNKNOWN, detail)
        return CheckResult(indexer, chain, NO_EVIDENCE, NO_ELIGIBLE_SUBGRAPH)

    async def _check_now(self, indexer: str, chain: str, push_at: float) -> CheckResult:
        choices, broken = self._choices(indexer, chain)
        if not choices:
            return self._nothing_to_ask(indexer, chain, broken)
        deployment = self._rng.choice(choices)
        async with self._semaphore:
            # A check sent later than this might not be answered before the push.
            if self._clock() > push_at - self._config.request_timeout - SEND_SLACK_SECONDS:
                return CheckResult(
                    indexer, chain, VOID, NOT_SENT, "too many checks waiting to be sent", deployment
                )
            reply = await self._ask(deployment, indexer)
        answer = judge(reply, self._config.bar_seconds)
        self._recent.record(indexer, chain, deployment, answer)
        self._note(indexer, deployment, answer)
        judgement = await self._assign_blame(indexer, chain, deployment, answer, choices, push_at)
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

    async def _ask_for_second_opinion(self, deployment: str, indexer: str) -> GatewayResult:
        async with self._semaphore:
            return await self._ask(deployment, indexer)

    async def _assign_blame(
        self,
        indexer: str,
        chain: str,
        deployment: str,
        judgement: Judgement,
        choices: list[str],
        push_at: float,
    ) -> Judgement:
        """Turn a failure that isn't the indexer's fault into a void one."""
        if is_behind(judgement):
            decide_by = push_at - SEND_SLACK_SECONDS - DECIDE_MARGIN_SECONDS
            if await self._broken.judge_stale(
                indexer, chain, deployment, judgement, choices, decide_by
            ):
                return replace(judgement, outcome=VOID, reason=SUBGRAPH_BROKEN)
        return judgement

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
        elif judgement.reason == PAYMENT_REFUSED:
            self._alarms.report(
                ("payment_refused", indexer),
                "Indexer %s refuses the gateway's payments, so its freshness checks are void",
                indexer,
                level=logging.WARNING,
            )
        elif judgement.reason == KEY_OUT_OF_FUNDS:
            self._alarms.report(
                "api_key",
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
            response = await asyncio.get_running_loop().run_in_executor(
                self._push_executor, self._push_payload, payload
            )
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
    registry = NetworksRegistry(config.networks_registry_url, config.additional_networks)
    loader = TargetLoader(
        networks=registry.networks,
        fetch_scores=partial(get_scores, iisa_api_url, token),
        read_allocations=partial(read_open_allocations, subgraph_url),
        read_first_allocations=partial(read_first_allocations, subgraph_url),
        min_allocation_age_seconds=config.min_allocation_age_seconds,
    )
    prober = FreshnessProber(
        config,
        registry=registry,
        load_targets=loader,
        ask=gateway.ask,
        push=partial(post_freshness, iisa_api_url, token, timeout=PUSH_TIMEOUT_SECONDS),
        read_allocated_indexers=partial(read_allocated_indexers, subgraph_url),
        read_deployments=partial(read_deployments_on_chain, subgraph_url),
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
