"""Tests for loading The Graph's networks registry, which says which chain names are real enough
to check, with every fetch faked."""

import logging
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

from freshness_config import (  # noqa: E402
    DEFAULT_NETWORKS_REGISTRY_URL,
    FALLBACK_NETWORKS_REGISTRY_URL,
)
from freshness_registry import (  # noqa: E402
    RELOAD_SECONDS,
    NetworksRegistry,
    RegistryUnavailable,
)

NOW = 1_794_345_636.0
OTHER_URL = "https://registry.example.com/networks.json"


def _document(*ids):
    return {
        "version": "0.7.136",
        "networks": [{"id": network, "aliases": [], "caip2Id": "eip155:1"} for network in ids],
    }


class FakeFetch:
    """Answers each address as told, an answer at a time when given a list, and records what it
    was asked."""

    def __init__(self, answers):
        self.answers = answers
        self.asked = []

    def __call__(self, url):
        self.asked.append(url)
        answer = self.answers[url]
        if isinstance(answer, list):
            answer = answer.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


class Clock:
    def __init__(self, now=NOW):
        self.now = now

    def __call__(self):
        return self.now


def _registry(answers, url=OTHER_URL, additional=(), clock=None):
    fetch = FakeFetch(answers)
    registry = NetworksRegistry(url, additional, fetch=fetch, clock=clock or Clock())
    return registry, fetch


def test_the_networks_are_the_registry_ids_plus_the_additional_names():
    registry, _ = _registry(
        {OTHER_URL: _document("mainnet", "arbitrum-one")}, additional=["hardhat"]
    )

    assert registry.networks() == {"mainnet", "arbitrum-one", "hardhat"}


def test_entries_without_a_usable_id_are_ignored():
    document = _document("mainnet", "x" * 65, "has space", "bad\nid", "\ud800")
    document["networks"] += [{"aliases": ["eth"]}, {"id": 7}, "matic"]
    registry, _ = _registry({OTHER_URL: document})

    assert registry.networks() == {"mainnet"}


@pytest.mark.parametrize(
    "document",
    [{}, {"networks": []}, {"networks": [{"id": 7}, {"id": ""}]}, [], "networks"],
    ids=["no-networks", "empty", "no-usable-id", "a-list", "text"],
)
def test_a_file_with_no_usable_ids_is_a_failed_load(document):
    registry, _ = _registry({OTHER_URL: [document]})

    with pytest.raises(RegistryUnavailable, match="no copy of the networks registry"):
        registry.networks()


def test_nothing_loaded_yet_raises_and_the_next_call_tries_again():
    registry, fetch = _registry(
        {OTHER_URL: [requests.ConnectionError("refused"), _document("mainnet")]}
    )

    with pytest.raises(RegistryUnavailable, match="refused"):
        registry.networks()
    assert registry.networks() == {"mainnet"}
    assert fetch.asked == [OTHER_URL, OTHER_URL]


def test_the_registry_is_read_again_only_after_6_hours():
    clock = Clock()
    registry, fetch = _registry(
        {OTHER_URL: [_document("mainnet"), _document("mainnet", "base")]}, clock=clock
    )

    assert registry.networks() == {"mainnet"}
    clock.now += RELOAD_SECONDS - 1
    assert registry.networks() == {"mainnet"}
    clock.now += 1
    assert registry.networks() == {"mainnet", "base"}
    assert len(fetch.asked) == 2


def test_a_failed_load_keeps_the_previous_copy_and_warns_how_old_it_is(caplog):
    clock = Clock()
    registry, fetch = _registry(
        {OTHER_URL: [_document("mainnet"), requests.Timeout("slow"), _document("base")]},
        clock=clock,
    )
    registry.networks()

    clock.now += RELOAD_SECONDS
    with caplog.at_level(logging.WARNING, logger="freshness_registry"):
        assert registry.networks() == {"mainnet"}
    (warning,) = [r.getMessage() for r in caplog.records]
    assert f"{RELOAD_SECONDS}s ago" in warning and "slow" in warning

    # A failed load is tried again at the next call rather than 6 hours later.
    clock.now += 900
    assert registry.networks() == {"base"}
    assert len(fetch.asked) == 3


def test_the_fallback_address_is_tried_when_the_default_address_fails():
    registry, fetch = _registry(
        {
            DEFAULT_NETWORKS_REGISTRY_URL: requests.HTTPError("503 Server Error"),
            FALLBACK_NETWORKS_REGISTRY_URL: _document("mainnet"),
        },
        url=DEFAULT_NETWORKS_REGISTRY_URL,
    )

    assert registry.networks() == {"mainnet"}
    assert fetch.asked == [DEFAULT_NETWORKS_REGISTRY_URL, FALLBACK_NETWORKS_REGISTRY_URL]


def test_the_fallback_address_is_not_tried_for_an_address_someone_set():
    registry, fetch = _registry({OTHER_URL: requests.HTTPError("404 Client Error")})

    with pytest.raises(RegistryUnavailable, match="404"):
        registry.networks()
    assert fetch.asked == [OTHER_URL]


def test_health_says_when_it_loaded_and_how_many_registry_networks_it_holds():
    registry, _ = _registry({OTHER_URL: _document("mainnet", "base")}, additional=["hardhat"])
    assert registry.health() == {
        "networks_registry_loaded_at": None,
        "networks_registry_networks": None,
    }

    registry.networks()

    assert registry.health() == {
        "networks_registry_loaded_at": "2026-11-10T21:20:36+00:00",
        "networks_registry_networks": 2,
    }
