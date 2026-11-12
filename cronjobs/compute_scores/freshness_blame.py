"""Second opinions on a stale answer: is the subgraph broken for every indexer on it?

A bug in a subgraph stops every indexer at the same block, and a new heavy subgraph can take
every indexer more than 7 days to sync; neither is any indexer's fault. So when a check comes
back behind on subgraph S of chain X, up to 3 other indexers allocated to S are asked the same
question. An indexer supports "S is broken" when it is behind on S but fresh on another subgraph
of X, which separates a stuck subgraph from an RPC provider outage that makes indexers behind on
everything. S is left out of every indexer's checks for an hour when at least 2 indexers support
that and none answered fresh on S. S isn't asked about again for 10 minutes once it is found not
broken, nor while someone answered fresh on it in the last 10 minutes.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import Awaitable, Callable, Coroutine, Optional, Sequence

from freshness_rules import (
    PASS,
    GatewayResult,
    GatewayUnreachable,
    Judgement,
    is_behind,
    is_broken_for_everyone,
    supports_broken_verdict,
)
from freshness_sources import EXPECTED_READ_ERRORS

logger = logging.getLogger(__name__)

SECOND_OPINIONS = 3
MAX_INVESTIGATIONS_PER_MINUTE = 20
BROKEN_FOR_SECONDS = 3600
# A "not broken" verdict is kept this long, so stale answers on a healthy subgraph don't each buy
# a new investigation.
NOT_BROKEN_FOR_SECONDS = 600
RECENT_WINDOW_SECONDS = 600
# Second opinions get less time than a regular check, so a verdict can land before the push.
SECOND_OPINION_TIMEOUT_SECONDS = 10

Ask = Callable[[str, str], Awaitable[GatewayResult]]
# Each indexer's open deployments on a chain, each with when its oldest open allocation there was
# created.
DeploymentAges = dict[str, dict[str, int]]


class RecentPasses:
    """Which deployments each (indexer, chain) passed on in the last 10 minutes, and when any
    indexer last passed on each deployment."""

    def __init__(self, clock: Callable[[], float], window: float = RECENT_WINDOW_SECONDS) -> None:
        self._clock = clock
        self._window = window
        self._passes: dict[tuple[str, str], deque[tuple[float, str]]] = {}
        self._last_pass_on: dict[str, float] = {}

    def _prune(self, key: tuple[str, str]) -> deque[tuple[float, str]]:
        passes = self._passes.setdefault(key, deque())
        cutoff = self._clock() - self._window
        while passes and passes[0][0] < cutoff:
            passes.popleft()
        return passes

    def record(self, indexer: str, chain: str, deployment: str, judgement: Judgement) -> None:
        if judgement.outcome == PASS:
            self._prune((indexer, chain)).append((self._clock(), deployment))
            self._last_pass_on[deployment] = self._clock()

    def anyone_passed_on(self, deployment: str) -> bool:
        last = self._last_pass_on.get(deployment)
        return last is not None and self._clock() - last <= self._window

    def passed_elsewhere(self, indexer: str, chain: str, deployment: str) -> bool:
        return any(d != deployment for _, d in self._prune((indexer, chain)))


class _ReadOnFirstUse:
    """1 read shared by every caller, started only when the first of them asks for it."""

    def __init__(self, start: Callable[[], Coroutine[object, object, DeploymentAges]]) -> None:
        self._start = start
        self._task: Optional[asyncio.Task[DeploymentAges]] = None

    def result(self) -> asyncio.Task[DeploymentAges]:
        if self._task is None:
            self._task = asyncio.create_task(self._start())
        return self._task


@dataclass(frozen=True)
class Opinion:
    """What 1 indexer said about the subgraph, and about another subgraph of the same chain."""

    indexer: str
    on_subgraph: Optional[Judgement]
    elsewhere: Optional[Judgement] = None

    @property
    def supports(self) -> bool:
        return supports_broken_verdict(self.on_subgraph, self.elsewhere)

    @property
    def fresh_on_subgraph(self) -> bool:
        return self.on_subgraph is not None and self.on_subgraph.outcome == PASS

    def summary(self) -> str:
        def said(judgement: Optional[Judgement]) -> str:
            if judgement is None:
                return "not asked"
            return judgement.outcome + (f" ({judgement.reason})" if judgement.reason else "")

        return f"{self.indexer}: {said(self.on_subgraph)}, elsewhere {said(self.elsewhere)}"


class BrokenSubgraphs:
    """Decides which subgraphs are broken for everyone, and leaves them out for an hour.

    ``ask`` sends 1 query through the gateway and ``judge`` turns the reply into a Judgement.
    ``read_allocated_indexers`` and ``read_deployments`` block, so they run on ``executor``.
    """

    def __init__(
        self,
        *,
        ask: Ask,
        judge: Callable[[GatewayResult], Judgement],
        read_allocated_indexers: Callable[[str], list[str]],
        read_deployments: Callable[[Sequence[str], str], DeploymentAges],
        recent: RecentPasses,
        min_allocation_age_seconds: float,
        clock: Callable[[], float],
        rng: random.Random,
        executor: Executor,
    ) -> None:
        self._ask = ask
        self._judge = judge
        self._read_allocated_indexers = read_allocated_indexers
        self._read_deployments = read_deployments
        self._recent = recent
        self._min_age = min_allocation_age_seconds
        self._clock = clock
        self._rng = rng
        self._executor = executor
        self._broken_until: dict[str, float] = {}
        self._not_broken_until: dict[str, float] = {}
        self._in_progress: dict[str, asyncio.Task[bool]] = {}
        self._minute = -1
        self._started_this_minute = 0

    def is_broken(self, deployment: str) -> bool:
        return self._still_marked(self._broken_until, deployment)

    def _known_not_broken(self, deployment: str) -> bool:
        return self._still_marked(self._not_broken_until, deployment)

    def _still_marked(self, marks: dict[str, float], deployment: str) -> bool:
        until = marks.get(deployment)
        if until is not None and until <= self._clock():
            del marks[deployment]
            return False
        return until is not None

    def _remember_not_broken(self, deployment: str) -> None:
        self._not_broken_until[deployment] = self._clock() + NOT_BROKEN_FOR_SECONDS

    async def judge_stale(
        self,
        indexer: str,
        chain: str,
        deployment: str,
        judgement: Judgement,
        alternatives: Sequence[str],
        decide_by: float,
    ) -> bool:
        """True when ``deployment`` is broken for everyone, so a check behind on it is void.

        ``alternatives`` are the indexer's other eligible deployments on the chain. Waits for the
        verdict until ``decide_by``; a verdict that comes later still leaves the subgraph out of
        later checks, but this one's fail stands, as it does when the investigation cap is hit.
        """
        if self.is_broken(deployment):
            return True
        if self._recent.anyone_passed_on(deployment) or self._known_not_broken(deployment):
            # A recent pass means the verdict would be "not broken" whatever the second
            # opinions said, and a recent "not broken" verdict still stands.
            return False
        task = self._in_progress.get(deployment) or self._start(
            indexer, chain, deployment, judgement, alternatives
        )
        remaining = decide_by - self._clock()
        if task is None or remaining <= 0:
            return False
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=remaining)
        except asyncio.TimeoutError:
            logger.info("No verdict on %s in time for this minute; the fail stands", deployment)
            return False

    def _start(
        self,
        indexer: str,
        chain: str,
        deployment: str,
        judgement: Judgement,
        alternatives: Sequence[str],
    ) -> Optional[asyncio.Task[bool]]:
        minute = int(self._clock() // 60)
        if minute != self._minute:
            self._minute, self._started_this_minute = minute, 0
        if self._started_this_minute >= MAX_INVESTIGATIONS_PER_MINUTE:
            logger.info(
                "Already %d second-opinion investigations this minute; the fail on %s stands",
                MAX_INVESTIGATIONS_PER_MINUTE,
                deployment,
            )
            return None
        self._started_this_minute += 1
        own = Opinion(indexer, judgement)
        task = asyncio.create_task(self._investigate(own, chain, deployment, alternatives))
        self._in_progress[deployment] = task
        task.add_done_callback(lambda _done: self._in_progress.pop(deployment, None))
        return task

    async def _investigate(
        self, own: Opinion, chain: str, deployment: str, alternatives: Sequence[str]
    ) -> bool:
        try:
            others = await asyncio.get_running_loop().run_in_executor(
                self._executor, self._read_allocated_indexers, deployment
            )
        except Exception as e:
            logger.warning(
                "Couldn't read who else is allocated to %s, so the fail stands: %s: %s",
                deployment,
                type(e).__name__,
                e,
                exc_info=not isinstance(e, EXPECTED_READ_ERRORS),
            )
            return False
        others = [indexer for indexer in others if indexer != own.indexer]
        if not others:
            logger.info("No other indexer is allocated to %s; the fail stands", deployment)
            self._remember_not_broken(deployment)
            return False

        sample = self._rng.sample(others, min(SECOND_OPINIONS, len(others)))
        mine, *theirs = await asyncio.gather(
            self._own_opinion(own, chain, deployment, alternatives),
            *self._opinions_of(sample, chain, deployment),
        )
        return self._verdict(deployment, chain, [mine, *theirs])

    def _verdict(self, deployment: str, chain: str, opinions: list[Opinion]) -> bool:
        # A pass on it in the last 10 minutes, from any check, means someone is fresh on it.
        recent_pass = self._recent.anyone_passed_on(deployment)
        broken = is_broken_for_everyone(
            sum(opinion.supports for opinion in opinions),
            recent_pass or any(opinion.fresh_on_subgraph for opinion in opinions),
        )
        if broken:
            self._broken_until[deployment] = self._clock() + BROKEN_FOR_SECONDS
        else:
            self._remember_not_broken(deployment)
        logger.info(
            "Subgraph %s on %s %s; a pass on it in the last 10 minutes: %s; asked %s",
            deployment,
            chain,
            "is broken for every indexer, leaving it out for 1 hour"
            if broken
            else "isn't broken for everyone, so the fail stands",
            "yes" if recent_pass else "no",
            "; ".join(opinion.summary() for opinion in opinions),
        )
        return broken

    async def _ask_judged(self, deployment: str, indexer: str) -> Judgement:
        try:
            reply = await asyncio.wait_for(
                self._ask(deployment, indexer), timeout=SECOND_OPINION_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            reply = GatewayUnreachable(
                f"no second opinion within {SECOND_OPINION_TIMEOUT_SECONDS:g}s"
            )
        return self._judge(reply)

    async def _own_opinion(
        self, own: Opinion, chain: str, deployment: str, alternatives: Sequence[str]
    ) -> Opinion:
        if self._recent.passed_elsewhere(own.indexer, chain, deployment):
            return Opinion(own.indexer, own.on_subgraph, Judgement(PASS))
        choices = [d for d in alternatives if d != deployment and not self.is_broken(d)]
        if not choices:
            return own
        other = self._rng.choice(choices)
        elsewhere = await self._ask_judged(other, own.indexer)
        self._recent.record(own.indexer, chain, other, elsewhere)
        return Opinion(own.indexer, own.on_subgraph, elsewhere)

    def _opinions_of(
        self, indexers: Sequence[str], chain: str, deployment: str
    ) -> list[Awaitable[Opinion]]:
        # The read is only needed for an indexer found behind, which usually none of them is.
        deployments = _ReadOnFirstUse(lambda: self._deployments_of(indexers, chain))
        return [self._opinion_of(i, chain, deployment, deployments) for i in indexers]

    async def _opinion_of(
        self,
        indexer: str,
        chain: str,
        deployment: str,
        deployments: _ReadOnFirstUse,
    ) -> Opinion:
        on_subgraph = await self._ask_judged(deployment, indexer)
        self._recent.record(indexer, chain, deployment, on_subgraph)
        if not is_behind(on_subgraph):
            return Opinion(indexer, on_subgraph)
        other = self._other_deployment(indexer, deployment, await deployments.result())
        if other is None:
            return Opinion(indexer, on_subgraph)
        elsewhere = await self._ask_judged(other, indexer)
        self._recent.record(indexer, chain, other, elsewhere)
        return Opinion(indexer, on_subgraph, elsewhere)

    async def _deployments_of(self, indexers: Sequence[str], chain: str) -> DeploymentAges:
        try:
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._read_deployments, list(indexers), chain
            )
        except Exception as e:
            logger.warning(
                "Couldn't read the open deployments of %s on %s, so they can't back a verdict: "
                "%s: %s",
                ", ".join(indexers),
                chain,
                type(e).__name__,
                e,
                exc_info=not isinstance(e, EXPECTED_READ_ERRORS),
            )
            return {}

    def _other_deployment(
        self, indexer: str, deployment: str, deployments: DeploymentAges
    ) -> Optional[str]:
        """1 other deployment on the chain to ask the indexer about, preferring ones it has had
        long enough to have synced."""
        candidates = {
            other: created_at
            for other, created_at in deployments.get(indexer, {}).items()
            if other != deployment and not self.is_broken(other)
        }
        now = self._clock()
        settled = [
            other for other, created_at in candidates.items() if now - created_at >= self._min_age
        ]
        pool = sorted(settled or candidates)
        return self._rng.choice(pool) if pool else None
