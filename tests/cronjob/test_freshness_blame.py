"""Tests for the 2 rules that keep an indexer from being marked down for something that isn't its
fault: refused payments judged by the gateway's escrow, and subgraphs broken for every indexer.
The gateway, the network subgraph and the push to IISA are all fakes."""

import asyncio
import json
import logging
import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import freshness_blame  # noqa: E402
from freshness_blame import (  # noqa: E402
    BROKEN_FOR_SECONDS,
    MAX_INVESTIGATIONS_PER_MINUTE,
    NOT_BROKEN_FOR_SECONDS,
    BrokenSubgraphs,
    RecentPasses,
)
from freshness_config import FreshnessConfig  # noqa: E402
from freshness_prober import FreshnessProber  # noqa: E402
from freshness_rules import (  # noqa: E402
    Allocation,
    GatewayReply,
    GatewayUnreachable,
    Judgement,
    Targets,
    escrow_is_funded,
    judge,
)

FAKE_KEY = "fake-freshness-key-0000"
PAYER = "0x" + "9" * 40
CONFIG = FreshnessConfig(
    gateway_url="https://gateway.example.com/api", api_key=FAKE_KEY, gateway_payers=(PAYER,)
)
# 2026-11-10T22:31:00Z
MINUTE = 1_794_349_860.0
CHAIN = "mainnet"
DAY = 86_400
STALE = Judgement("fail", "stale", age_seconds=1800.0, attested=True)


def _indexer(n):
    return f"0x{n:040x}"


def _deployment(letter):
    return "Qm" + letter * 44


A, B, J1, J2, J3 = (_indexer(n) for n in range(1, 6))
S, T, U, V = (_deployment(letter) for letter in "STUV")


def _answer(age):
    def reply(arrived_at):
        block = {"number": 7, "timestamp": int(arrived_at) - age}
        body = json.dumps({"data": {"_meta": {"block": block}}}).encode()
        return GatewayReply(200, True, body, arrived_at)

    return reply


FRESH = _answer(0)
BEHIND = _answer(1800)


def _refused(indexer, status):
    message = f"bad indexers: {{{indexer}: BadResponse({status})}}"
    body = json.dumps({"errors": [{"message": message}]}).encode()
    return lambda arrived_at: GatewayReply(200, False, body, arrived_at)


class FakeClock:
    """Time that only moves when something sleeps, so a whole minute runs in an instant."""

    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now

    async def sleep(self, delay):
        self.now += max(0.0, delay)
        await asyncio.sleep(0)


class FakeGateway:
    def __init__(self, clock, replies):
        self.clock = clock
        self.replies = replies
        self.asked = []

    async def ask(self, deployment, indexer):
        self.asked.append((deployment, indexer))
        return self.replies[(deployment, indexer)](self.clock())


class FirstChoice(random.Random):
    """Picks the first of whatever it is offered, so a test decides which subgraph is asked."""

    def choice(self, seq):
        return seq[0]

    def sample(self, population, k, **_kwargs):
        return list(population)[:k]


class FakeNetworkSubgraph:
    def __init__(self, allocated_to=None, allocations=()):
        self.allocated_to = allocated_to or {}
        self.allocations = list(allocations)
        self.allocated_reads = []
        self.deployment_reads = []
        self.read_threads = []

    def read_allocated_indexers(self, deployment):
        self.read_threads.append(threading.current_thread().name)
        self.allocated_reads.append(deployment)
        return list(self.allocated_to.get(deployment, []))

    def read_deployments(self, indexers, chain):
        """Each indexer's open deployments on ``chain``, with its oldest allocation on each."""
        self.read_threads.append(threading.current_thread().name)
        self.deployment_reads.append((list(indexers), chain))
        found = {indexer: {} for indexer in indexers}
        for a in self.allocations:
            if a.indexer in found and a.chain == chain:
                ages = found[a.indexer]
                ages[a.ipfs_hash] = min(a.created_at, ages.get(a.ipfs_hash, a.created_at))
        return found


def _allocation(indexer, deployment, days_old=30):
    return Allocation(
        indexer, CHAIN, "0x" + deployment[2].lower() * 64, deployment, int(MINUTE - days_old * DAY)
    )


def _prober(clock, gateway, targets, pushed, network=None):
    network = network or FakeNetworkSubgraph()
    prober = FreshnessProber(
        CONFIG,
        load_targets=lambda: targets,
        ask=gateway.ask,
        push=lambda payload: pushed.append(payload) or {"applied": True},
        read_allocated_indexers=network.read_allocated_indexers,
        read_deployments=network.read_deployments,
        clock=clock,
        sleep=clock.sleep,
        rng=FirstChoice(),
    )
    prober._targets = targets
    return prober


def _outcomes(payload):
    return {(o["indexer"], o["chain"]): (o["outcome"], o["reason"]) for o in payload["outcomes"]}


def _targets(eligible, escrow_funded=None):
    return Targets(
        pairs=tuple(sorted(eligible)),
        eligible={(indexer, CHAIN): deployments for (indexer, _), deployments in eligible.items()},
        escrow_funded=escrow_funded or {},
    )


# ---------------------------------------------------------------------------
# Refused payments
# ---------------------------------------------------------------------------


def _escrow_account(balance, thawing="0"):
    return {
        "payer": {"id": PAYER},
        "collector": "0x1",
        "balance": balance,
        "totalAmountThawing": thawing,
    }


ESCROWS = {
    "funded": ([_escrow_account("1000")], ("fail", "payment_refused")),
    "empty": ([_escrow_account("0")], ("void", "escrow_empty")),
    "fully thawing": ([_escrow_account("1000", "1000")], ("void", "escrow_empty")),
    "unknown": (None, ("fail", "payment_refused")),
}


@pytest.mark.parametrize("status", [400, 402])
@pytest.mark.parametrize("escrow", list(ESCROWS))
async def test_a_refused_payment_is_judged_by_the_gateways_escrow(escrow, status):
    accounts, expected = ESCROWS[escrow]
    funded = {} if accounts is None else {A: escrow_is_funded(accounts, {PAYER})}
    clock = FakeClock(MINUTE)
    gateway = FakeGateway(clock, {(S, A): _refused(A, status)})
    pushed = []

    await _prober(clock, gateway, _targets({(A, CHAIN): (S,)}, funded), pushed).run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {(A, CHAIN): expected}


async def test_funded_refusals_are_still_void_when_most_indexers_refuse_at_once():
    indexers = [A, B, J1]
    clock = FakeClock(MINUTE)
    gateway = FakeGateway(clock, {(S, i): _refused(i, 402) for i in indexers})
    targets = _targets({(i, CHAIN): (S,) for i in indexers}, dict.fromkeys(indexers, True))
    pushed = []

    await _prober(clock, gateway, targets, pushed).run_minute(MINUTE)

    assert set(_outcomes(pushed[0]).values()) == {("void", "general_fault")}
    assert {o["detail"].split(":")[0] for o in pushed[0]["outcomes"]} == {"payment_refused"}


async def test_an_empty_escrow_is_logged_once_per_interval_naming_the_indexer(caplog):
    clock = FakeClock(MINUTE)
    gateway = FakeGateway(clock, {(S, A): _refused(A, 402)})
    prober = _prober(clock, gateway, _targets({(A, CHAIN): (S,)}, {A: False}), [])

    with caplog.at_level(logging.ERROR, logger="freshness_prober"):
        for minute in range(3):
            await prober.run_minute(MINUTE + 60 * minute)

    errors = [r.getMessage() for r in caplog.records if "escrow" in r.getMessage()]
    assert len(errors) == 1
    assert A in errors[0]


# ---------------------------------------------------------------------------
# Subgraphs broken for every indexer
# ---------------------------------------------------------------------------


async def _run_minutes(prober, clock, count):
    for _ in range(count):
        await prober.run_minute(clock.now - clock.now % 60)


async def test_a_subgraph_2_indexers_find_stuck_is_left_out_for_an_hour(caplog):
    """A and B are behind on S; J1 and J2, allocated to S, are behind on it too but fresh on
    another subgraph of the chain, so S is broken for everyone."""
    clock = FakeClock(MINUTE)
    replies = {
        (S, A): BEHIND,
        (S, B): BEHIND,
        (T, B): FRESH,
        (S, J1): BEHIND,
        (U, J1): FRESH,
        (S, J2): BEHIND,
        (V, J2): FRESH,
    }
    gateway = FakeGateway(clock, replies)
    network = FakeNetworkSubgraph(
        {S: [A, B, J1, J2]}, [_allocation(J1, U), _allocation(J2, V), _allocation(J1, S)]
    )
    targets = _targets({(A, CHAIN): (S,), (B, CHAIN): (S, T)})
    pushed = []
    prober = _prober(clock, gateway, targets, pushed, network)

    with caplog.at_level(logging.INFO, logger="freshness_blame"):
        await prober.run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {
        (A, CHAIN): ("void", "subgraph_broken"),
        (B, CHAIN): ("void", "subgraph_broken"),
    }
    assert network.allocated_reads == [S]
    verdict = next(r.getMessage() for r in caplog.records if "is broken" in r.getMessage())
    assert S in verdict and J1 in verdict and J2 in verdict

    # While S is left out, A has nothing else to be asked about and B is asked about T.
    asked_before = len(gateway.asked)
    await prober.run_minute(MINUTE + 60)
    assert _outcomes(pushed[1]) == {
        (A, CHAIN): ("void", "subgraph_broken"),
        (B, CHAIN): ("pass", None),
    }
    assert pushed[1]["outcomes"][0]["deployment"] is None
    assert S in pushed[1]["outcomes"][0]["detail"]
    assert gateway.asked[asked_before:] == [(T, B)]

    # An hour later S can be picked and judged again.
    clock.now += BROKEN_FOR_SECONDS
    await _run_minutes(prober, clock, 1)
    assert (S, A) in gateway.asked[asked_before + 1 :]


async def test_a_shared_rpc_outage_is_not_called_a_broken_subgraph():
    """Behind on S and behind elsewhere too: the indexers' chain data is what's stuck."""
    clock = FakeClock(MINUTE)
    replies = {
        (S, A): BEHIND,
        (T, A): BEHIND,
        (S, J1): BEHIND,
        (U, J1): BEHIND,
        (S, J2): BEHIND,
        (V, J2): BEHIND,
    }
    network = FakeNetworkSubgraph({S: [A, J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    pushed = []
    prober = _prober(
        clock, FakeGateway(clock, replies), _targets({(A, CHAIN): (S, T)}), pushed, network
    )

    await prober.run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {(A, CHAIN): ("fail", "stale")}
    assert pushed[0]["outcomes"][0]["deployment"] == S


async def test_a_subgraph_only_1_indexer_is_on_is_not_called_broken():
    clock = FakeClock(MINUTE)
    gateway = FakeGateway(clock, {(S, A): BEHIND})
    pushed = []
    prober = _prober(
        clock, gateway, _targets({(A, CHAIN): (S,)}), pushed, FakeNetworkSubgraph({S: [A]})
    )

    await prober.run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {(A, CHAIN): ("fail", "stale")}
    assert gateway.asked == [(S, A)]


async def test_a_fresh_second_opinion_keeps_the_fail():
    """A and J2 would support the verdict, but J1 answers fresh on S, so S isn't broken."""
    clock = FakeClock(MINUTE)
    replies = {
        (S, A): BEHIND,
        (T, A): FRESH,
        (S, J1): FRESH,
        (S, J2): BEHIND,
        (V, J2): FRESH,
        (S, J3): BEHIND,
    }
    network = FakeNetworkSubgraph({S: [A, J1, J2, J3]}, [_allocation(J2, V)])
    pushed = []
    prober = _prober(
        clock, FakeGateway(clock, replies), _targets({(A, CHAIN): (S, T)}), pushed, network
    )

    await prober.run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {(A, CHAIN): ("fail", "stale")}


async def test_the_indexer_under_check_supports_with_its_own_recent_pass():
    clock = FakeClock(MINUTE)
    replies = {(T, A): FRESH, (S, A): BEHIND, (S, J1): BEHIND, (U, J1): FRESH}
    gateway = FakeGateway(clock, replies)
    network = FakeNetworkSubgraph({S: [A, J1]}, [_allocation(J1, U)])
    pushed = []
    prober = _prober(clock, gateway, _targets({(A, CHAIN): (T,)}), pushed, network)

    await prober.run_minute(MINUTE)
    prober._targets = _targets({(A, CHAIN): (S, T)})
    await prober.run_minute(MINUTE + 60)

    assert _outcomes(pushed[1]) == {(A, CHAIN): ("void", "subgraph_broken")}
    assert gateway.asked.count((T, A)) == 1


async def test_the_indexer_under_check_is_asked_about_another_subgraph_when_it_has_no_recent_pass():
    clock = FakeClock(MINUTE)
    replies = {(S, A): BEHIND, (T, A): FRESH, (S, J1): BEHIND, (U, J1): FRESH}
    gateway = FakeGateway(clock, replies)
    network = FakeNetworkSubgraph({S: [A, J1]}, [_allocation(J1, U)])
    pushed = []
    prober = _prober(clock, gateway, _targets({(A, CHAIN): (S, T)}), pushed, network)

    await prober.run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {(A, CHAIN): ("void", "subgraph_broken")}
    assert (T, A) in gateway.asked


def _would_be_broken():
    """Replies where A is behind on S and J1 and J2 support a broken verdict."""
    return {
        (S, A): BEHIND,
        (S, J1): BEHIND,
        (U, J1): FRESH,
        (S, J2): BEHIND,
        (V, J2): FRESH,
    }


@pytest.mark.parametrize(
    ("minutes_later", "outcome"), [(1, ("fail", "stale")), (11, ("void", "subgraph_broken"))]
)
async def test_a_pass_on_the_subgraph_in_the_last_10_minutes_keeps_the_fail(minutes_later, outcome):
    """B passed on S in a regular check, so S isn't broken for everyone for 10 minutes after."""
    clock = FakeClock(MINUTE)
    gateway = FakeGateway(clock, {(S, B): FRESH, **_would_be_broken()})
    network = FakeNetworkSubgraph({S: [A, J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    pushed = []
    prober = _prober(clock, gateway, _targets({(B, CHAIN): (S,)}), pushed, network)

    await prober.run_minute(MINUTE)
    prober._targets = _targets({(A, CHAIN): (S,)})
    clock.now = MINUTE + 60 * minutes_later
    await prober.run_minute(clock.now)

    assert _outcomes(pushed[0]) == {(B, CHAIN): ("pass", None)}
    assert _outcomes(pushed[1]) == {(A, CHAIN): outcome}


async def test_a_pass_on_the_subgraph_in_an_earlier_second_opinion_keeps_the_fail():
    clock = FakeClock(MINUTE)
    replies = {**_would_be_broken(), (S, J1): FRESH}
    gateway = FakeGateway(clock, replies)
    network = FakeNetworkSubgraph({S: [A, J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    pushed = []
    prober = _prober(clock, gateway, _targets({(A, CHAIN): (S,)}), pushed, network)

    await prober.run_minute(MINUTE)
    replies[(S, J1)] = BEHIND
    await prober.run_minute(MINUTE + 60)

    assert [_outcomes(p) for p in pushed] == [{(A, CHAIN): ("fail", "stale")}] * 2
    # J1's pass on S in the first minute means the second minute starts no investigation.
    assert gateway.asked.count((S, J1)) == 1
    assert (U, J1) not in gateway.asked


def _broken_subgraphs(clock, network, gateway, rng=None):
    return BrokenSubgraphs(
        ask=gateway.ask,
        judge=partial(judge, bar_seconds=36.0),
        read_allocated_indexers=network.read_allocated_indexers,
        read_deployments=network.read_deployments,
        recent=RecentPasses(clock),
        min_allocation_age_seconds=7 * DAY,
        clock=clock,
        rng=rng or FirstChoice(),
        executor=ThreadPoolExecutor(2),
    )


async def test_at_most_20_investigations_start_in_a_minute():
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph()
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, {}))
    deployments = [f"Qm{n}" for n in range(MAX_INVESTIGATIONS_PER_MINUTE + 1)]

    verdicts = await asyncio.gather(
        *(broken.judge_stale(A, CHAIN, d, STALE, [], MINUTE + 80) for d in deployments)
    )

    assert verdicts == [False] * len(deployments)
    assert len(network.allocated_reads) == MAX_INVESTIGATIONS_PER_MINUTE

    clock.now += 60
    await broken.judge_stale(A, CHAIN, deployments[-1], STALE, [], clock.now + 20)
    assert len(network.allocated_reads) == MAX_INVESTIGATIONS_PER_MINUTE + 1


def _stuck_for_everyone():
    """Replies where J1 and J2 are behind on S but fresh on another subgraph of the chain."""
    return {(S, J1): BEHIND, (U, J1): FRESH, (S, J2): BEHIND, (V, J2): FRESH}


async def test_a_recent_pass_on_the_subgraph_starts_no_investigation():
    """Its verdict would be "not broken" whatever the second opinions said, so every query it
    paid for would be wasted."""
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    gateway = FakeGateway(clock, _stuck_for_everyone())
    broken = _broken_subgraphs(clock, network, gateway)
    broken._recent.record(B, CHAIN, S, Judgement("pass"))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert network.allocated_reads == []
    assert gateway.asked == []


async def test_without_a_recent_pass_the_investigation_starts():
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    gateway = FakeGateway(clock, _stuck_for_everyone())
    broken = _broken_subgraphs(clock, network, gateway)

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is True
    assert network.allocated_reads == [S]


def _outage_for_everyone():
    """Replies where J1 and J2 are behind on S and elsewhere too: not broken, and nobody passed."""
    return {(S, J1): BEHIND, (U, J1): BEHIND, (S, J2): BEHIND, (V, J2): BEHIND}


async def test_a_not_broken_verdict_is_remembered_for_10_minutes():
    """Otherwise every stale answer on a healthy subgraph buys a new investigation, and a popular
    one can use up the 20 a minute."""
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    gateway = FakeGateway(clock, _outage_for_everyone())
    broken = _broken_subgraphs(clock, network, gateway)
    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    asked = len(gateway.asked)

    clock.now = MINUTE + NOT_BROKEN_FOR_SECONDS - 1
    assert await broken.judge_stale(B, CHAIN, S, STALE, [], clock.now + 80) is False
    assert network.allocated_reads == [S]
    assert len(gateway.asked) == asked

    clock.now = MINUTE + NOT_BROKEN_FOR_SECONDS
    assert await broken.judge_stale(B, CHAIN, S, STALE, [], clock.now + 80) is False
    assert network.allocated_reads == [S, S]


async def test_no_other_indexer_allocated_is_remembered_as_not_broken():
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph({S: [A]})
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, {}))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert network.allocated_reads == [S]


class _UnreachableNetwork(FakeNetworkSubgraph):
    def read_allocated_indexers(self, deployment):
        self.allocated_reads.append(deployment)
        raise ValueError("network subgraph unreachable")


async def test_an_investigation_whose_read_failed_is_tried_again():
    clock = FakeClock(MINUTE)
    network = _UnreachableNetwork()
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, {}))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert network.allocated_reads == [S, S]


async def test_an_investigation_the_cap_turned_away_is_tried_again():
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph()
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, {}))
    others = [f"Qm{n}" for n in range(MAX_INVESTIGATIONS_PER_MINUTE)]
    await asyncio.gather(*(broken.judge_stale(A, CHAIN, d, STALE, [], MINUTE + 80) for d in others))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert S not in network.allocated_reads

    clock.now += 60
    await broken.judge_stale(A, CHAIN, S, STALE, [], clock.now + 20)
    assert S in network.allocated_reads


def test_the_other_deployment_asked_about_is_never_the_judged_one_or_a_broken_one():
    clock = FakeClock(MINUTE)
    broken = _broken_subgraphs(clock, FakeNetworkSubgraph(), FakeGateway(clock, {}))
    broken._broken_until[T] = MINUTE + BROKEN_FOR_SECONDS
    settled = int(MINUTE - 30 * DAY)

    assert broken._other_deployment(J1, S, {J1: {S: settled, T: settled, U: settled}}) == U
    assert broken._other_deployment(J1, S, {J1: {S: settled, T: settled}}) is None
    assert broken._other_deployment(J2, S, {J1: {U: settled}}) is None


def test_the_other_deployment_is_one_the_indexer_has_had_long_enough_to_sync_when_it_can_be():
    clock = FakeClock(MINUTE)
    broken = _broken_subgraphs(clock, FakeNetworkSubgraph(), FakeGateway(clock, {}))
    young, settled = int(MINUTE - DAY), int(MINUTE - 30 * DAY)

    assert broken._other_deployment(J1, S, {J1: {U: young, V: settled}}) == V
    assert broken._other_deployment(J1, S, {J1: {U: young}}) == U


async def test_only_1_investigation_runs_per_subgraph():
    clock = FakeClock(MINUTE)
    replies = {(S, J1): BEHIND, (U, J1): FRESH, (S, J2): BEHIND, (V, J2): FRESH}
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, replies))

    verdicts = await asyncio.gather(
        broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80),
        broken.judge_stale(B, CHAIN, S, STALE, [], MINUTE + 80),
    )

    assert verdicts == [True, True]
    assert network.allocated_reads == [S]


async def test_second_opinions_that_all_answer_fresh_read_no_allocations():
    """That read is a paid query on a small shared pool, so it waits until an indexer is behind."""
    clock = FakeClock(MINUTE)
    replies = {(S, J1): FRESH, (S, J2): FRESH}
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, replies))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is False
    assert network.deployment_reads == []


async def test_1_second_opinion_behind_reads_allocations_once():
    clock = FakeClock(MINUTE)
    replies = {
        (T, A): FRESH,
        (S, J1): BEHIND,
        (U, J1): FRESH,
        (S, J2): lambda _arrived_at: GatewayUnreachable("no answer"),
    }
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, replies))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [T], MINUTE + 80) is True
    assert network.deployment_reads == [([J1, J2], CHAIN)]


async def test_2_second_opinions_behind_at_once_share_1_allocation_read():
    clock = FakeClock(MINUTE)
    replies = {(S, J1): BEHIND, (U, J1): FRESH, (S, J2): BEHIND, (V, J2): FRESH}
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, replies))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], MINUTE + 80) is True
    assert network.deployment_reads == [([J1, J2], CHAIN)]


async def test_a_verdict_too_late_for_the_minute_keeps_the_fail_but_still_counts_later():
    clock = FakeClock(MINUTE)
    replies = {(S, J1): BEHIND, (U, J1): FRESH, (S, J2): BEHIND, (V, J2): FRESH}
    network = FakeNetworkSubgraph({S: [J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    broken = _broken_subgraphs(clock, network, FakeGateway(clock, replies))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], decide_by=MINUTE - 1) is False

    await asyncio.gather(*broken._in_progress.values())
    assert broken.is_broken(S)


class SlowGateway(FakeGateway):
    """Takes 5 real seconds to answer for the indexers in ``slow``."""

    def __init__(self, clock, replies, slow):
        super().__init__(clock, replies)
        self.slow = slow

    async def ask(self, deployment, indexer):
        if indexer in self.slow:
            await asyncio.sleep(5)
        return await super().ask(deployment, indexer)


async def test_a_slow_second_opinion_is_given_up_on_so_the_verdict_comes_in_time(monkeypatch):
    """J1 never answers in time; J2 and J3 still decide the verdict before the minute's deadline."""
    assert freshness_blame.SECOND_OPINION_TIMEOUT_SECONDS == 10
    monkeypatch.setattr(freshness_blame, "SECOND_OPINION_TIMEOUT_SECONDS", 0.05)
    clock = FakeClock(MINUTE)
    replies = {
        (S, J1): BEHIND,
        (S, J2): BEHIND,
        (V, J2): FRESH,
        (S, J3): BEHIND,
        (U, J3): FRESH,
    }
    network = FakeNetworkSubgraph({S: [J1, J2, J3]}, [_allocation(J2, V), _allocation(J3, U)])
    broken = _broken_subgraphs(clock, network, SlowGateway(clock, replies, slow={J1}))

    assert await broken.judge_stale(A, CHAIN, S, STALE, [], decide_by=MINUTE + 1) is True


async def test_second_opinion_reads_run_on_their_own_small_pool():
    """Network subgraph reads for second opinions keep off the pool the refresh uses."""
    clock = FakeClock(MINUTE)
    network = FakeNetworkSubgraph({S: [A, J1, J2]}, [_allocation(J1, U), _allocation(J2, V)])
    pushed = []
    prober = _prober(
        clock, FakeGateway(clock, _would_be_broken()), _targets({(A, CHAIN): (S,)}), pushed, network
    )

    await prober.run_minute(MINUTE)

    assert _outcomes(pushed[0]) == {(A, CHAIN): ("void", "subgraph_broken")}
    assert len(network.read_threads) == 2
    assert all(name.startswith("freshness-opinions") for name in network.read_threads)
