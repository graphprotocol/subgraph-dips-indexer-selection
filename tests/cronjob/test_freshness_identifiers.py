"""Tests that the prober's checks on addresses and hashes take exactly what the push's own check
takes, so a pair is never built only to be refused every minute."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

from freshness_contract import refusal  # noqa: E402
from freshness_rules import (  # noqa: E402
    is_deployment_entity_id,
    is_deployment_hash,
    is_indexer_address,
)

ADDRESS = "0x" + "a" * 40
HASH = "Qm" + "A" * 44
ENTITY_ID = "0x" + "b" * 64


@pytest.mark.parametrize(
    ("check", "value"),
    [
        (is_indexer_address, ADDRESS),
        (is_deployment_hash, HASH),
        (is_deployment_entity_id, ENTITY_ID),
    ],
    ids=["indexer-address", "deployment-hash", "deployment-entity-id"],
)
def test_a_value_with_a_trailing_line_break_is_refused(check, value):
    """In a Python pattern, $ also matches just before a final line break."""
    assert check(value)
    assert not check(value + "\n")


def test_the_push_refuses_the_same_indexer_address_the_prober_does():
    outcome = {
        "indexer": ADDRESS + "\n",
        "chain": "mainnet",
        "outcome": "pass",
        "reason": None,
        "detail": None,
        "deployment": HASH,
        "age_seconds": 1.0,
    }

    assert refusal(outcome) is not None
    assert not is_indexer_address(outcome["indexer"])
