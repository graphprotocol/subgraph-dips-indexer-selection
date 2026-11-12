"""Tests for the last check before a freshness push: every outcome the service would refuse is
left out, so 1 bad outcome can't cost the whole minute."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

from freshness_contract import refusal, refusals  # noqa: E402

INDEXER = "0x" + "a" * 40


def _outcome(**changes):
    outcome = {
        "indexer": INDEXER,
        "chain": "mainnet",
        "outcome": "fail",
        "reason": "stale",
        "detail": "x" * 300,
        "deployment": "Qm" + "A" * 44,
        "age_seconds": 1778.9,
    }
    return {**outcome, **changes}


@pytest.mark.parametrize(
    "outcome",
    [
        _outcome(),
        _outcome(outcome="pass", reason=None, detail=None, age_seconds=0),
        _outcome(outcome="no_evidence", reason="no_eligible_subgraph", deployment=None),
        _outcome(outcome="void", reason="not_sent", age_seconds=None, detail=""),
        _outcome(indexer="0x" + "A" * 40, chain="x" * 64, reason="r" * 64, deployment="Q" * 64),
        _outcome(chain="réseau", detail="un réseau \U0001d510"),
    ],
)
def test_outcomes_the_service_accepts_pass(outcome):
    assert refusal(outcome) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("indexer", "0x" + "a" * 39),
        ("indexer", "0x" + "g" * 40),
        ("indexer", None),
        ("chain", ""),
        ("chain", "x" * 65),
        ("chain", "\ud800"),
        ("chain", None),
        ("outcome", "passed"),
        ("outcome", ["pass"]),
        ("reason", "Stale"),
        ("reason", "stale\n"),
        ("reason", "r" * 65),
        ("reason", "_stale"),
        ("detail", "x" * 301),
        ("detail", "bad \udfff"),
        ("detail", 7),
        ("deployment", "Q" * 65),
        ("deployment", "Qm\ud800"),
        ("age_seconds", float("nan")),
        ("age_seconds", float("inf")),
        ("age_seconds", -0.5),
        ("age_seconds", True),
        ("age_seconds", "1.0"),
    ],
)
def test_an_outcome_the_service_would_refuse_is_named(field, value):
    assert field in refusal(_outcome(**{field: value}))


def test_an_outcome_with_a_missing_or_unknown_field_is_refused():
    missing = {k: v for k, v in _outcome().items() if k != "age_seconds"}

    assert refusal(missing)
    assert refusal(_outcome(attested=True))


def test_refusals_names_each_refused_outcome_in_order():
    outcomes = [_outcome(chain="base"), _outcome(chain="main\nnet\ud800"), _outcome()]

    problems = refusals(outcomes)

    assert problems[0] is None and problems[2] is None
    assert "chain" in problems[1]


def test_a_second_outcome_for_the_same_indexer_and_chain_is_refused():
    """The service compares indexers in lower case, so a change of case is the same pair."""
    second = _outcome(indexer=INDEXER.upper().replace("0X", "0x"), outcome="pass", reason=None)

    problems = refusals([_outcome(), second])

    assert problems[0] is None
    assert "second outcome" in problems[1]


def test_at_most_50000_outcomes_are_accepted():
    outcomes = [_outcome(indexer=f"0x{n:040x}") for n in range(50_001)]

    problems = refusals(outcomes)

    assert problems[:50_000] == [None] * 50_000
    assert "50,000" in problems[50_000]
