"""Tests for reading the freshness prober's settings, and for the fetcher refusing to start
when the prober is switched on without what it needs."""

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import sync_status_fetcher  # noqa: E402
from freshness_config import FreshnessConfig, load_freshness_config  # noqa: E402

FAKE_KEY = "fake-freshness-key-0000"
PAYER = "0x" + "9" * 40
ENABLED = {
    "FRESHNESS_PROBE_ENABLED": "true",
    "FRESHNESS_GATEWAY_URL": "https://gateway.example.com/api/",
    "FRESHNESS_GATEWAY_API_KEY": FAKE_KEY,
    "FRESHNESS_GATEWAY_PAYERS": PAYER,
}


@pytest.mark.parametrize("value", [None, "", "false", "False", "0", "no"])
def test_the_prober_is_off_unless_switched_on(value):
    environ = {} if value is None else {"FRESHNESS_PROBE_ENABLED": value}

    assert load_freshness_config(environ) == (None, None)


def test_defaults_when_switched_on():
    config, problem = load_freshness_config(ENABLED)

    assert problem is None
    assert config == FreshnessConfig(
        gateway_url="https://gateway.example.com/api",
        api_key=FAKE_KEY,
        bar_seconds=36.0,
        min_allocation_age_days=7.0,
        refresh_interval=900.0,
        request_timeout=25.0,
        max_concurrency=50,
        gateway_payers=(PAYER,),
    )
    assert config.min_allocation_age_seconds == 7 * 86_400
    assert FAKE_KEY not in repr(config)


def test_every_setting_can_be_changed():
    config, problem = load_freshness_config(
        {
            **ENABLED,
            "FRESHNESS_BAR_SECONDS": "20",
            "FRESHNESS_MIN_ALLOCATION_AGE_DAYS": "3.5",
            "FRESHNESS_REFRESH_INTERVAL": "600",
            "FRESHNESS_REQUEST_TIMEOUT": "10",
            "FRESHNESS_MAX_CONCURRENCY": "8",
        }
    )

    assert problem is None
    assert (config.bar_seconds, config.min_allocation_age_days) == (20.0, 3.5)
    assert (config.refresh_interval, config.request_timeout, config.max_concurrency) == (
        600.0,
        10.0,
        8,
    )


def test_the_networks_registry_defaults_to_the_graphs_own_copy_and_no_extra_names():
    config, _ = load_freshness_config(ENABLED)

    assert config.networks_registry_url == (
        "https://networks-registry.thegraph.com/TheGraphNetworksRegistry_v0_7_x.json"
    )
    assert config.additional_networks == ()


def test_the_networks_registry_and_extra_names_can_be_set():
    config, problem = load_freshness_config(
        {
            **ENABLED,
            "FRESHNESS_NETWORKS_REGISTRY_URL": " https://registry.example.com/networks.json ",
            "FRESHNESS_ADDITIONAL_NETWORKS": " hardhat, ,local-test,hardhat ",
        }
    )

    assert problem is None
    assert config.networks_registry_url == "https://registry.example.com/networks.json"
    assert config.additional_networks == ("hardhat", "local-test")


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"FRESHNESS_NETWORKS_REGISTRY_URL": "registry"}, "FRESHNESS_NETWORKS_REGISTRY_URL must"),
        ({"FRESHNESS_ADDITIONAL_NETWORKS": "hardhat,réseau"}, "FRESHNESS_ADDITIONAL_NETWORKS must"),
        ({"FRESHNESS_ADDITIONAL_NETWORKS": "x" * 65}, "FRESHNESS_ADDITIONAL_NETWORKS must"),
    ],
)
def test_an_unusable_registry_address_or_extra_name_is_a_problem(change, expected):
    config, problem = load_freshness_config({**ENABLED, **change})

    assert config is None
    assert problem.startswith(expected)


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"FRESHNESS_GATEWAY_URL": ""}, "FRESHNESS_GATEWAY_URL is required"),
        ({"FRESHNESS_GATEWAY_URL": "gateway.example.com"}, "FRESHNESS_GATEWAY_URL must be"),
        ({"FRESHNESS_GATEWAY_URL": "https://gw.example.com/a b"}, "FRESHNESS_GATEWAY_URL contains"),
        ({"FRESHNESS_GATEWAY_API_KEY": ""}, "FRESHNESS_GATEWAY_API_KEY is required"),
        ({"FRESHNESS_GATEWAY_API_KEY": "   "}, "FRESHNESS_GATEWAY_API_KEY is required"),
        ({"FRESHNESS_PROBE_ENABLED": "ture"}, "FRESHNESS_PROBE_ENABLED must be true or false"),
        ({"FRESHNESS_BAR_SECONDS": "soon"}, "FRESHNESS_BAR_SECONDS must be a number"),
        ({"FRESHNESS_BAR_SECONDS": "0"}, "FRESHNESS_BAR_SECONDS must be a finite number"),
        ({"FRESHNESS_BAR_SECONDS": "nan"}, "FRESHNESS_BAR_SECONDS must be a finite number"),
        ({"FRESHNESS_MAX_CONCURRENCY": "inf"}, "FRESHNESS_MAX_CONCURRENCY must be a finite"),
        ({"FRESHNESS_REQUEST_TIMEOUT": "30"}, "FRESHNESS_REQUEST_TIMEOUT must be under 30"),
        ({"FRESHNESS_REFRESH_INTERVAL": "5"}, "FRESHNESS_REFRESH_INTERVAL must be a finite"),
    ],
)
def test_unusable_settings_are_a_problem_that_never_repeats_the_key(change, expected):
    config, problem = load_freshness_config({**ENABLED, **change})

    assert config is None
    assert problem.startswith(expected)
    assert FAKE_KEY not in problem


@pytest.fixture
def fetcher_env(monkeypatch):
    """A fetcher whose network subgraph URL is fine, so only the prober's settings decide."""
    monkeypatch.setattr(
        sync_status_fetcher, "GRAPH_NETWORK_SUBGRAPH_URL", "https://network.example.com/sg"
    )
    for name in ENABLED:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        sync_status_fetcher,
        "run_fetch_cycle",
        lambda: pytest.fail("the fetcher should have refused to start"),
    )
    return monkeypatch


@pytest.mark.parametrize("missing", ["FRESHNESS_GATEWAY_URL", "FRESHNESS_GATEWAY_API_KEY"])
def test_fetcher_refuses_to_start_when_enabled_without_url_or_key(fetcher_env, caplog, missing):
    for name, value in ENABLED.items():
        if name != missing:
            fetcher_env.setenv(name, value)

    with caplog.at_level(logging.CRITICAL, logger="sync_status_fetcher"):
        assert sync_status_fetcher.main() == 1

    assert f"{missing} is required; refusing to start" in caplog.text
    assert FAKE_KEY not in caplog.text


@pytest.mark.parametrize(
    ("payers", "expected"),
    [
        ("", "FRESHNESS_GATEWAY_PAYERS is required"),
        (" , ", "FRESHNESS_GATEWAY_PAYERS is required"),
        ("0x1234", "FRESHNESS_GATEWAY_PAYERS must be 0x addresses"),
        (PAYER + ";" + PAYER, "FRESHNESS_GATEWAY_PAYERS must be 0x addresses"),
    ],
)
def test_the_gateways_payers_are_required(payers, expected):
    config, problem = load_freshness_config({**ENABLED, "FRESHNESS_GATEWAY_PAYERS": payers})

    assert config is None
    assert problem.startswith(expected)


def test_the_gateways_collector_is_optional_and_in_lower_case():
    collector = "0x" + "CD" * 20
    unset, _ = load_freshness_config(ENABLED)
    config, problem = load_freshness_config(
        {**ENABLED, "FRESHNESS_GATEWAY_COLLECTOR": f" {collector} "}
    )

    assert unset.gateway_collector is None
    assert problem is None
    assert config.gateway_collector == collector.lower()


@pytest.mark.parametrize("collector", ["0x1234", f"{PAYER},{PAYER}"])
def test_a_collector_that_isnt_1_address_is_a_problem(collector):
    config, problem = load_freshness_config({**ENABLED, "FRESHNESS_GATEWAY_COLLECTOR": collector})

    assert config is None
    assert problem == "FRESHNESS_GATEWAY_COLLECTOR must be a 0x address"


def test_payers_are_a_comma_separated_list_in_lower_case():
    second = "0x" + "AB" * 20
    config, problem = load_freshness_config(
        {**ENABLED, "FRESHNESS_GATEWAY_PAYERS": f" {PAYER}, {second},{PAYER} "}
    )

    assert problem is None
    assert config.gateway_payers == (PAYER, second.lower())
