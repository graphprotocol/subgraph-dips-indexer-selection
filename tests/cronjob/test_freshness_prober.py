"""Tests for the freshness prober's minute and loop, with the gateway, GET /scores, the network
subgraph and the push to IISA all replaced by fakes."""

import asyncio
import json
import logging
import math
import random
import re
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import freshness_prober  # noqa: E402
from freshness_config import FreshnessConfig  # noqa: E402
from freshness_prober import FreshnessProber  # noqa: E402
from freshness_registry import NetworksRegistry, RegistryUnavailable  # noqa: E402
from freshness_rules import CheckResult, GatewayReply, GatewayUnreachable, Targets  # noqa: E402
from iisa_client import IISAPushError  # noqa: E402

FAKE_KEY = "fake-freshness-key-0000"
CONFIG = FreshnessConfig(gateway_url="https://gateway.example.com/api", api_key=FAKE_KEY)
# 2026-11-10T22:31:00Z
MINUTE = 1_794_349_860.0


def _indexer(n):
    return f"0x{n:040x}"


def _deployment(letter):
    return "Qm" + letter * 44


def _answer(timestamp, *, attested=True):
    body = {"data": {"_meta": {"block": {"number": 7, "timestamp": timestamp}}}}
    return lambda arrived_at: GatewayReply(200, attested, json.dumps(body).encode(), arrived_at)


def _gateway_says(message):
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
    """Answers each (deployment, indexer) as told, and records what it was asked."""

    def __init__(self, clock, replies):
        self.clock = clock
        self.replies = replies
        self.asked = []

    async def ask(self, deployment, indexer):
        self.asked.append((deployment, indexer))
        reply = self.replies[(deployment, indexer)]
        return reply if isinstance(reply, GatewayUnreachable) else reply(self.clock())


# For a prober whose stale answers find no other indexer to ask.
NO_SECOND_OPINIONS = {
    "read_allocated_indexers": lambda _deployment: [],
    "read_deployments": lambda indexers, _chain: dict.fromkeys(indexers, {}),
}


def _prober(
    targets,
    gateway,
    clock,
    pushed,
    config=CONFIG,
    load_targets=None,
    allocated_to=None,
    allocations=(),
):
    """A prober on fakes. ``allocated_to`` maps a deployment to the indexers allocated to it and
    ``allocations`` holds every indexer's open allocations, for second opinions."""
    return FreshnessProber(
        config,
        load_targets=load_targets or (lambda: targets),
        ask=gateway.ask,
        push=lambda payload: pushed.append(payload) or {"applied": True},
        read_allocated_indexers=lambda deployment: list((allocated_to or {}).get(deployment, [])),
        read_deployments=lambda indexers, chain: {
            indexer: {
                a.ipfs_hash: a.created_at
                for a in allocations
                if a.indexer == indexer and a.chain == chain
            }
            for indexer in indexers
        },
        clock=clock,
        sleep=clock.sleep,
        rng=random.Random(7),
    )


# Every code POST /freshness accepts, by outcome (CONTRACT.md).
CONTRACT_REASONS = {
    "pass": {None},
    "fail": {
        None,
        "stale",
        "no_timestamp",
        "indexer_error",
        "indexer_timeout",
        "indexer_unavailable",
        "payment_refused",
    },
    "no_evidence": {None, "no_eligible_subgraph"},
    "void": {
        None,
        "gateway_error",
        "key_out_of_funds",
        "pairing_unknown",
        "general_fault",
        "payment_refused",
        "escrow_empty",
        "subgraph_broken",
        "not_sent",
    },
}
OUTCOME_FIELDS = {"indexer", "chain", "outcome", "reason", "detail", "deployment", "age_seconds"}


def assert_matches_contract(payload):
    """Check a pushed body against every rule CONTRACT.md sets for POST /freshness, and the
    stricter ones the service's validation adds."""
    # The service refuses text that UTF-8 can't hold, such as half of a surrogate pair.
    json.dumps(payload, allow_nan=False, ensure_ascii=False).encode("utf-8")
    assert set(payload) == {"minute", "outcomes"}
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:00Z", payload["minute"])
    minute = datetime.fromisoformat(payload["minute"])
    assert minute.tzinfo is not None
    assert (minute.second, minute.microsecond) == (0, 0)
    outcomes = payload["outcomes"]
    assert len(outcomes) <= 50_000
    assert len({(o["indexer"], o["chain"]) for o in outcomes}) == len(outcomes)
    for o in outcomes:
        assert set(o) == OUTCOME_FIELDS
        assert re.fullmatch(r"0x[0-9a-f]{40}", o["indexer"])
        assert 1 <= len(o["chain"]) <= 64
        assert o["reason"] in CONTRACT_REASONS[o["outcome"]]
        assert o["reason"] is None or re.fullmatch(r"[a-z][a-z0-9_]*", o["reason"])
        assert o["detail"] is None or (isinstance(o["detail"], str) and len(o["detail"]) <= 300)
        assert o["deployment"] is None or len(o["deployment"]) <= 64
        age = o["age_seconds"]
        assert age is None or (isinstance(age, float) and math.isfinite(age) and age >= 0)


# ---------------------------------------------------------------------------
# 1 minute
# ---------------------------------------------------------------------------


async def test_a_whole_minute_end_to_end():
    clock = FakeClock(MINUTE)
    ts = int(MINUTE)
    eligible = {
        (_indexer(1), "arbitrum-one"): (_deployment("A"),),
        (_indexer(1), "mainnet"): (_deployment("G"),),
        (_indexer(2), "bsc"): (_deployment("B"),),
        (_indexer(4), "base"): (_deployment("C"),),
        (_indexer(5), "mainnet"): (_deployment("D"),),
        (_indexer(6), "mainnet"): (_deployment("E"),),
        (_indexer(7), "mainnet"): (_deployment("F"),),
    }
    targets = Targets(pairs=tuple(sorted([*eligible, (_indexer(3), "matic")])), eligible=eligible)
    gateway = FakeGateway(
        clock,
        {
            (_deployment("A"), _indexer(1)): _answer(ts + 90),
            (_deployment("G"), _indexer(1)): _answer(None),
            (_deployment("B"), _indexer(2)): _answer(ts - 1800),
            (_deployment("C"), _indexer(4)): _gateway_says("internal error: oops"),
            (_deployment("D"), _indexer(5)): _gateway_says(
                f"bad indexers: {{{_indexer(5)}: Timeout}}"
            ),
            (_deployment("E"), _indexer(6)): _gateway_says("no indexers found"),
            (_deployment("F"), _indexer(7)): GatewayUnreachable("no answer within 25s"),
        },
    )
    pushed = []
    prober = _prober(targets, gateway, clock, pushed)
    prober._targets = targets

    await prober.run_minute(MINUTE)

    assert len(pushed) == 1
    payload = pushed[0]
    assert_matches_contract(payload)
    assert payload["minute"] == "2026-11-10T22:31:00Z"
    by_pair = {(o["indexer"], o["chain"]): o for o in payload["outcomes"]}
    assert {pair: (o["outcome"], o["reason"]) for pair, o in by_pair.items()} == {
        (_indexer(1), "arbitrum-one"): ("pass", None),
        (_indexer(1), "mainnet"): ("fail", "no_timestamp"),
        (_indexer(2), "bsc"): ("fail", "stale"),
        (_indexer(3), "matic"): ("no_evidence", "no_eligible_subgraph"),
        (_indexer(4), "base"): ("void", "gateway_error"),
        (_indexer(5), "mainnet"): ("fail", "indexer_timeout"),
        (_indexer(6), "mainnet"): ("void", "pairing_unknown"),
        (_indexer(7), "mainnet"): ("void", "gateway_error"),
    }
    assert by_pair[(_indexer(2), "bsc")]["deployment"] == _deployment("B")
    assert by_pair[(_indexer(2), "bsc")]["age_seconds"] > 1800
    assert by_pair[(_indexer(3), "matic")]["deployment"] is None
    assert by_pair[(_indexer(4), "base")]["detail"] == "internal error: oops"
    # Every check was sent within the minute and the push came 30 seconds after it ended.
    assert clock.now == MINUTE + 90
    assert len(gateway.asked) == 7
    assert prober.last_push is not None


async def test_the_pushed_minute_is_the_one_just_ended_never_one_ahead_of_the_clock():
    """The service rejects a minute more than 5 minutes ahead of its clock, or off a whole
    minute, so the prober pushes the minute it checked, 90 seconds after it began."""
    clock = FakeClock(MINUTE)
    pushed = []
    prober = _prober(None, FakeGateway(clock, {}), clock, pushed)
    prober._targets = Targets(pairs=(), eligible={})

    await prober.run_minute(MINUTE)

    assert_matches_contract(pushed[0])
    assert datetime.fromisoformat(pushed[0]["minute"]).timestamp() == MINUTE
    assert clock.now == MINUTE + 90


async def test_a_minute_the_service_already_applied_counts_as_pushed(caplog):
    """An answer of applied: false means the service already has that minute; it isn't a
    failure, so it is neither retried nor held against the prober's health."""
    clock = FakeClock(MINUTE)
    pushed = []
    prober = _prober(None, FakeGateway(clock, {}), clock, pushed)
    prober._targets = Targets(pairs=(), eligible={})
    prober._push_payload = lambda payload: (
        pushed.append(payload)
        or {"status": "success", "minute": payload["minute"], "applied": False, "pairs": 0}
    )

    with caplog.at_level(logging.INFO, logger="freshness_prober"):
        await prober.run_minute(MINUTE)

    assert len(pushed) == 1
    assert prober.last_push is not None
    assert "applied=False" in caplog.text
    assert "Failed to push" not in caplog.text


async def test_a_minute_where_no_indexer_answers_is_void_for_all():
    clock = FakeClock(MINUTE)
    eligible = {(_indexer(n), "mainnet"): (_deployment("A"),) for n in range(3)}
    replies = {
        (_deployment("A"), _indexer(n)): _gateway_says(f"bad indexers: {{{_indexer(n)}: Timeout}}")
        for n in range(3)
    }
    pushed = []
    prober = _prober(None, FakeGateway(clock, replies), clock, pushed)
    prober._targets = Targets(pairs=tuple(sorted(eligible)), eligible=eligible)

    await prober.run_minute(MINUTE)

    assert [(o["outcome"], o["reason"]) for o in pushed[0]["outcomes"]] == [
        ("void", "general_fault")
    ] * 3
    assert_matches_contract(pushed[0])


async def test_a_pairing_the_gateway_doesnt_know_is_dropped_until_the_next_refresh():
    """While every eligible deployment is dropped, the pair is void rather than no_evidence: a
    gateway that briefly lost track of an allocation is not the indexer's fault."""
    clock = FakeClock(MINUTE)
    pair = (_indexer(1), "mainnet")
    targets = Targets(pairs=(pair,), eligible={pair: (_deployment("A"),)})
    gateway = FakeGateway(
        clock, {(_deployment("A"), _indexer(1)): _gateway_says("no indexers found")}
    )
    pushed = []
    prober = _prober(targets, gateway, clock, pushed)
    await prober.refresh()

    await prober.run_minute(MINUTE)
    await prober.run_minute(MINUTE + 60)
    await prober.refresh()
    await prober.run_minute(MINUTE + 120)

    outcomes = [(p["outcomes"][0]["outcome"], p["outcomes"][0]["reason"]) for p in pushed]
    assert outcomes == [("void", "pairing_unknown")] * 3
    assert pushed[1]["outcomes"][0]["deployment"] is None
    assert len(gateway.asked) == 2


async def test_a_pair_with_no_eligible_deployment_at_all_is_no_evidence():
    clock = FakeClock(MINUTE)
    pair = (_indexer(1), "mainnet")
    pushed = []
    prober = _prober(None, FakeGateway(clock, {}), clock, pushed)
    prober._targets = Targets(pairs=(pair,), eligible={})

    await prober.run_minute(MINUTE)

    assert _single_outcome(pushed[0]) == ("no_evidence", "no_eligible_subgraph")


def _single_outcome(payload):
    (outcome,) = payload["outcomes"]
    return outcome["outcome"], outcome["reason"]


async def test_an_indexers_error_text_holding_half_a_surrogate_pair_is_pushed_clean():
    clock = FakeClock(MINUTE)
    pair = (_indexer(1), "mainnet")
    targets = Targets(pairs=(pair,), eligible={pair: (_deployment("A"),)})
    body = b'{"errors": [{"message": "store error \\ud800"}]}'

    def reply(arrived_at):
        return GatewayReply(200, True, body, arrived_at)

    pushed = []
    prober = _prober(
        targets, FakeGateway(clock, {(_deployment("A"), _indexer(1)): reply}), clock, pushed
    )
    prober._targets = targets

    await prober.run_minute(MINUTE)

    (outcome,) = pushed[0]["outcomes"]
    assert (outcome["outcome"], outcome["reason"]) == ("fail", "indexer_error")
    assert outcome["detail"] == "store error \\ud800"
    assert_matches_contract(pushed[0])


async def test_each_outcome_is_turned_into_its_payload_once(monkeypatch):
    """The last check before the push reads the same payloads that are then sent."""
    clock = FakeClock(MINUTE)
    built = []
    to_payload = CheckResult.to_payload
    monkeypatch.setattr(
        CheckResult, "to_payload", lambda result: built.append(result) or to_payload(result)
    )
    pushed = []
    prober = _prober(None, FakeGateway(clock, {}), clock, pushed)
    prober._targets = Targets(pairs=tuple((_indexer(n), "mainnet") for n in range(3)), eligible={})

    await prober.run_minute(MINUTE)

    assert len(pushed[0]["outcomes"]) == 3
    assert len(built) == 3


async def test_an_outcome_the_service_would_refuse_is_left_out_and_logged(caplog):
    """The service refuses a whole push for 1 bad outcome, so the rest of the minute must still
    get through."""
    clock = FakeClock(MINUTE)
    pairs = ((_indexer(1), "mainnet"), (_indexer(2), "main\n" + "x" * 60), (_indexer(3), "\ud800"))
    pushed = []
    prober = _prober(None, FakeGateway(clock, {}), clock, pushed)
    prober._targets = Targets(pairs=pairs, eligible={})

    with caplog.at_level(logging.ERROR):
        await prober.run_minute(MINUTE)

    assert [(o["indexer"], o["chain"]) for o in pushed[0]["outcomes"]] == [pairs[0]]
    assert_matches_contract(pushed[0])
    (error,) = [r for r in caplog.records if r.levelno == logging.ERROR]
    message = error.getMessage()
    assert "2 freshness outcomes" in message
    assert _indexer(2) in message
    assert "\n" not in message and "main\\n" in message
    message.encode("utf-8")


async def test_checks_still_waiting_when_they_could_no_longer_be_answered_are_not_sent():
    clock = FakeClock(MINUTE)
    eligible = {(_indexer(n), "mainnet"): (_deployment("A"),) for n in range(2)}

    def slow_answer(arrived_at):
        clock.now += 70
        return _answer(int(MINUTE))(arrived_at)

    replies = {(_deployment("A"), _indexer(n)): slow_answer for n in range(2)}
    pushed = []
    prober = _prober(
        None,
        FakeGateway(clock, replies),
        clock,
        pushed,
        config=FreshnessConfig(
            gateway_url="https://g.example.com", api_key=FAKE_KEY, max_concurrency=1
        ),
    )
    prober._targets = Targets(pairs=tuple(sorted(eligible)), eligible=eligible)

    await prober.run_minute(MINUTE)

    outcomes = sorted((o["outcome"], o["reason"] or "") for o in pushed[0]["outcomes"])
    assert outcomes == [("pass", ""), ("void", "not_sent")]
    assert_matches_contract(pushed[0])


async def test_an_out_of_funds_key_is_logged_once_per_interval(caplog):
    clock = FakeClock(MINUTE)
    pair = (_indexer(1), "mainnet")
    targets = Targets(pairs=(pair,), eligible={pair: (_deployment("A"),)})
    reply = _gateway_says(f"auth error: key {FAKE_KEY} is out of funds")
    pushed = []
    prober = _prober(
        targets, FakeGateway(clock, {(_deployment("A"), _indexer(1)): reply}), clock, pushed
    )
    prober._targets = targets

    with caplog.at_level(logging.ERROR, logger="freshness_prober"):
        for minute in range(3):
            await prober.run_minute(MINUTE + 60 * minute)

    errors = [r for r in caplog.records if "out of funds" in r.getMessage()]
    assert len(errors) == 1
    assert FAKE_KEY not in caplog.text
    assert [p["outcomes"][0]["reason"] for p in pushed] == ["key_out_of_funds"] * 3
    assert all(FAKE_KEY not in json.dumps(p) for p in pushed)


async def test_a_failed_push_is_logged_and_not_retried(caplog):
    clock = FakeClock(MINUTE)
    attempts = []

    def failing_push(payload):
        attempts.append(payload)
        raise IISAPushError("POST http://iisa:8080/freshness failed: connection refused")

    prober = FreshnessProber(
        CONFIG,
        load_targets=lambda: None,
        ask=None,
        push=failing_push,
        **NO_SECOND_OPINIONS,
        clock=clock,
        sleep=clock.sleep,
    )
    prober._targets = Targets(pairs=(), eligible={})

    with caplog.at_level(logging.ERROR, logger="freshness_prober"):
        await prober.run_minute(MINUTE)

    assert len(attempts) == 1
    assert attempts[0] == {"minute": "2026-11-10T22:31:00Z", "outcomes": []}
    assert "Failed to push freshness minute 2026-11-10T22:31:00Z" in caplog.text
    assert "connection refused" in caplog.text
    assert prober.last_push is None


async def test_a_failed_refresh_keeps_the_previous_lists(caplog):
    pair = (_indexer(1), "mainnet")
    first = Targets(pairs=(pair,), eligible={pair: (_deployment("A"),)})
    loads = [first, ValueError("GET /scores returned no score records")]

    def load():
        result = loads.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    prober = _prober(
        None, FakeGateway(FakeClock(MINUTE), {}), FakeClock(MINUTE), [], load_targets=load
    )

    await prober.refresh()
    with caplog.at_level(logging.ERROR, logger="freshness_prober"):
        await prober.refresh()

    assert prober._targets is first
    assert "keeping the previous lists" in caplog.text
    assert "no score records" in caplog.text


def test_health_says_the_prober_has_not_stopped_while_its_thread_runs_or_before_it_starts():
    clock = FakeClock(MINUTE)
    prober = _prober(None, FakeGateway(clock, {}), clock, [])

    fields = prober.health(datetime.fromtimestamp(MINUTE, timezone.utc))

    assert (fields["running"], fields["stopped"]) == (False, False)


class _GatewayThatFails:
    async def __aenter__(self):
        raise RuntimeError("no connection pool")

    async def __aexit__(self, *_exc):
        return None


def test_a_prober_thread_that_ends_on_an_error_is_marked_stopped(caplog):
    """The fetcher's /health fails on this, so the pod is restarted."""
    clock = FakeClock(MINUTE)
    prober = _prober(None, FakeGateway(clock, {}), clock, [])

    with caplog.at_level(logging.CRITICAL, logger="freshness_prober"):
        freshness_prober._run_in_thread(prober, _GatewayThatFails(), threading.Event())

    assert prober.stopped is True
    assert prober.health(datetime.fromtimestamp(MINUTE, timezone.utc))["stopped"] is True
    assert "Freshness prober stopped unexpectedly" in caplog.text


def test_health_shows_when_the_networks_registry_loaded_and_how_many_networks_it_holds():
    clock = FakeClock(MINUTE)
    prober = _prober(None, FakeGateway(clock, {}), clock, [])
    prober._registry = NetworksRegistry(
        "https://registry.example.com/networks.json",
        fetch=lambda url: {"networks": [{"id": "mainnet"}, {"id": "base"}]},
        clock=clock,
    )
    prober._registry.networks()

    fields = prober.health(datetime.fromtimestamp(MINUTE, timezone.utc))

    assert fields["networks_registry_loaded_at"] == "2026-11-10T22:31:00+00:00"
    assert fields["networks_registry_networks"] == 2


async def test_with_no_copy_of_the_networks_registry_a_refresh_logs_an_error_and_checks_nothing(
    caplog,
):
    clock = FakeClock(MINUTE)

    def load():
        raise RegistryUnavailable("no copy of the networks registry has loaded yet: refused")

    pushed = []
    prober = _prober(None, FakeGateway(clock, {}), clock, pushed, load_targets=load)

    with caplog.at_level(logging.ERROR, logger="freshness_prober"):
        await prober.refresh()

    assert prober._targets is None
    assert "no copy of the networks registry" in caplog.text


# ---------------------------------------------------------------------------
# The loop, on a sped-up real clock
# ---------------------------------------------------------------------------


@pytest.fixture
def fast_minutes(monkeypatch):
    """Minutes of 0.3s with a 0.15s push delay, so the loop runs a few minutes in a second."""
    monkeypatch.setattr(freshness_prober, "MINUTE", 0.3)
    monkeypatch.setattr(freshness_prober, "PUSH_DELAY_SECONDS", 0.15)
    monkeypatch.setattr(freshness_prober, "SEND_SLACK_SECONDS", 0.02)
    monkeypatch.setattr(freshness_prober, "LATE_MINUTE_LIMIT_SECONDS", 0.1)
    monkeypatch.setattr(freshness_prober, "STOP_POLL_SECONDS", 0.01)
    return FreshnessConfig(
        gateway_url="https://g.example.com", api_key=FAKE_KEY, request_timeout=0.05
    )


async def _run_until(prober, stop, deadline_seconds):
    try:
        await asyncio.wait_for(prober.run(stop), timeout=deadline_seconds)
    except asyncio.TimeoutError:
        pytest.fail("the prober didn't stop when its stop event was set")


async def test_the_loop_checks_every_minute_and_stops_on_the_stop_event(fast_minutes):
    pair = (_indexer(1), "mainnet")
    targets = Targets(pairs=(pair,), eligible={pair: (_deployment("A"),)})
    stop = threading.Event()
    pushed = []

    async def ask(deployment, indexer):
        return _answer(0)(0.0)

    def push(payload):
        pushed.append(payload)
        if len(pushed) == 3:
            stop.set()
        return {"applied": True}

    prober = FreshnessProber(
        fast_minutes, load_targets=lambda: targets, ask=ask, push=push, **NO_SECOND_OPINIONS
    )

    await _run_until(prober, stop, deadline_seconds=10)

    assert len(pushed) >= 3
    assert all(len(p["outcomes"]) == 1 for p in pushed)
    assert prober.running is False
    assert prober.last_push is not None


async def test_the_loop_pushes_nothing_before_the_first_successful_refresh(fast_minutes, caplog):
    stop = threading.Event()
    pushed = []

    def failing_load():
        raise ValueError("GET /scores returned no score records")

    prober = FreshnessProber(
        fast_minutes,
        load_targets=failing_load,
        ask=None,
        push=pushed.append,
        **NO_SECOND_OPINIONS,
    )
    asyncio.get_running_loop().call_later(1.0, stop.set)

    with caplog.at_level(logging.ERROR, logger="freshness_prober"):
        await _run_until(prober, stop, deadline_seconds=10)

    assert pushed == []
    assert "Freshness refresh failed" in caplog.text


async def test_the_push_runs_on_its_own_thread():
    """A slow refresh or network subgraph read can't hold up a minute's push."""
    clock = FakeClock(MINUTE)
    threads = []
    prober = _prober(None, FakeGateway(clock, {}), clock, [])
    prober._targets = Targets(pairs=(), eligible={})
    prober._push_payload = lambda payload: (
        threads.append(threading.current_thread().name) or {"applied": True}
    )

    await prober.run_minute(MINUTE)

    assert len(threads) == 1
    assert threads[0].startswith("freshness-push")
