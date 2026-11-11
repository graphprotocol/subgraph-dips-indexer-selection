"""Tests for how the freshness prober reads what to check, with every HTTP call faked."""

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

import freshness_sources  # noqa: E402
from freshness_rules import Allocation, Targets  # noqa: E402
from freshness_sources import (  # noqa: E402
    TargetLoader,
    post_graphql,
    read_first_allocations,
    read_open_allocations,
)
from subgraph import SubgraphResponseError  # noqa: E402

URL = "https://gateway.example.com/api/fake-network-key/subgraphs/id/QmNetwork"
NOW = 1_794_345_636.0
DAY = 86_400
INDEXER_A = "0x" + "a" * 40
INDEXER_B = "0x" + "b" * 40


def _deployment(letter, network="arbitrum-one"):
    return {
        "id": "0x" + letter.lower() * 64,
        "ipfsHash": "Qm" + letter * 44,
        "manifest": None if network is None else {"network": network},
    }


def _raw_allocation(n, letter, created_at, network="arbitrum-one"):
    return {
        "id": f"0x{n:040x}",
        "createdAt": str(int(created_at)),
        "subgraphDeployment": (_deployment(letter, network)),
    }


def _reply(data):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"data": data}
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def no_retry_wait():
    """Skip the retry backoff, which otherwise sleeps for about 30s."""
    with (
        patch.object(read_open_allocations.retry, "sleep", lambda _: None),
        patch.object(freshness_sources._read_first_allocation_chunk.retry, "sleep", lambda _: None),
    ):
        yield


@patch("requests.post")
def test_reads_open_allocations_with_their_chain(mock_post):
    mock_post.return_value = _reply(
        {
            "indexers": [
                {
                    "id": INDEXER_A,
                    "allocations": [
                        _raw_allocation(1, "A", NOW - 9 * DAY),
                        _raw_allocation(2, "B", NOW - DAY, network="mainnet"),
                        _raw_allocation(3, "C", NOW - DAY, network=None),
                    ],
                }
            ]
        }
    )

    allocations = read_open_allocations(URL, [INDEXER_A, INDEXER_B])

    assert allocations == [
        Allocation(INDEXER_A, "arbitrum-one", "0x" + "a" * 64, "Qm" + "A" * 44, int(NOW - 9 * DAY)),
        Allocation(INDEXER_A, "mainnet", "0x" + "b" * 64, "Qm" + "B" * 44, int(NOW - DAY)),
    ]
    query = mock_post.call_args.kwargs["json"]["query"]
    assert f'id_in: ["{INDEXER_A}", "{INDEXER_B}"]' in query
    assert "status: Active" in query


@patch("requests.post")
def test_pages_through_an_indexer_with_more_open_allocations_than_1_nested_page(mock_post):
    nested = [_raw_allocation(n, "A", NOW - 9 * DAY) for n in range(1000)]
    full = nested + [_raw_allocation(1000 + n, "B", NOW - 9 * DAY) for n in range(5)]
    mock_post.side_effect = [
        _reply({"indexers": [{"id": INDEXER_A, "allocations": nested}]}),
        _reply({"allocations": full[:1000]}),
        _reply({"allocations": full[1000:]}),
    ]

    allocations = read_open_allocations(URL, [INDEXER_A])

    assert len(allocations) == 1005
    follow_up = mock_post.call_args_list[1].kwargs["json"]["query"]
    assert f'indexer: "{INDEXER_A}"' in follow_up


@patch("requests.post")
def test_no_open_allocations_for_anyone_is_a_failed_read(mock_post, no_retry_wait):
    mock_post.return_value = _reply({"indexers": [{"id": INDEXER_A, "allocations": []}]})

    with pytest.raises(SubgraphResponseError, match="no open allocations"):
        read_open_allocations(URL, [INDEXER_A])


@patch("requests.post")
def test_reads_first_allocations_in_batches(mock_post, monkeypatch):
    monkeypatch.setattr(freshness_sources, "FIRST_ALLOCATIONS_PER_QUERY", 2)
    keys = [(INDEXER_A, "0x" + letter * 64) for letter in "abc"]
    mock_post.side_effect = [
        _reply({"a0": [{"createdAt": "100"}], "a1": []}),
        _reply({"a0": [{"createdAt": "300"}]}),
    ]

    found = read_first_allocations(URL, keys)

    assert found == {keys[0]: 100, keys[2]: 300}
    first_query = mock_post.call_args_list[0].kwargs["json"]["query"]
    assert "orderBy: createdAt, orderDirection: asc" in first_query
    assert f'subgraphDeployment: "{keys[1][1]}"' in first_query


@patch("requests.post")
def test_a_failed_read_never_shows_the_network_subgraph_key(mock_post):
    mock_post.side_effect = requests.ConnectionError(f"cannot reach {URL}")

    with pytest.raises(requests.ConnectionError) as raised:
        post_graphql(URL, "{ x }")

    assert "fake-network-key" not in str(raised.value)
    assert raised.value.__cause__ is None


class _Sources:
    """Fake GET /scores and network subgraph reads that record what was asked."""

    def __init__(self, scores, allocations, first_allocations=None):
        self.scores = scores
        self.allocations = allocations
        self.first_allocations = first_allocations or {}
        self.allocation_reads = []
        self.first_allocation_reads = []

    def loader(self, clock=lambda: NOW):
        return TargetLoader(
            fetch_scores=lambda: self.scores,
            read_allocations=self._read_allocations,
            read_first_allocations=self._read_first_allocations,
            min_allocation_age_seconds=7 * DAY,
            clock=clock,
        )

    def _read_allocations(self, indexers):
        self.allocation_reads.append(list(indexers))
        return [a for a in self.allocations if a.indexer in indexers]

    def _read_first_allocations(self, keys):
        self.first_allocation_reads.append(list(keys))
        return {key: self.first_allocations[key] for key in keys if key in self.first_allocations}


def _allocation(indexer, letter, created_at, chain="arbitrum-one"):
    return Allocation(indexer, chain, "0x" + letter.lower() * 64, "Qm" + letter * 44, created_at)


def _scores(*records):
    return {"computed_at": "2026-11-10T00:00:00Z", "count": len(records), "scores": list(records)}


def _offer(indexer, chains, prices=None):
    """A GET /scores record offering DIPs on ``chains``, each priced unless ``prices`` is given."""
    return {
        "indexer": indexer,
        "dips_supported_networks": list(chains),
        "dips_min_grt_per_30_days": dict.fromkeys(chains, 100.0) if prices is None else prices,
    }


def test_targets_pair_each_dips_chain_with_its_eligible_deployments():
    young = _allocation(INDEXER_A, "B", int(NOW - DAY))
    sources = _Sources(
        _scores(
            _offer(INDEXER_A.upper().replace("0X", "0x"), ["arbitrum-one", "matic"]),
            _offer(INDEXER_B, []),
        ),
        [
            _allocation(INDEXER_A, "A", int(NOW - 9 * DAY)),
            young,
            _allocation(INDEXER_A, "C", int(NOW - 9 * DAY), chain="mainnet"),
        ],
        {(INDEXER_A, young.deployment_id): int(NOW - 40 * DAY)},
    )

    targets = sources.loader()()

    assert targets == Targets(
        pairs=((INDEXER_A, "arbitrum-one"), (INDEXER_A, "matic")),
        eligible={(INDEXER_A, "arbitrum-one"): ("Qm" + "A" * 44, "Qm" + "B" * 44)},
    )
    assert sources.allocation_reads == [[INDEXER_A]]
    assert sources.first_allocation_reads == [[(INDEXER_A, young.deployment_id)]]


def test_a_chain_the_indexer_lists_without_a_price_is_not_checked():
    sources = _Sources(
        _scores(_offer(INDEXER_A, ["arbitrum-one", "matic"], {"arbitrum-one": 100.0})),
        [_allocation(INDEXER_A, "A", int(NOW - 9 * DAY), chain="matic")],
    )

    assert sources.loader()().pairs == ((INDEXER_A, "arbitrum-one"),)


def test_an_indexer_listing_150_chains_is_checked_on_100_with_a_warning(caplog):
    """1 indexer with a low address used to fill the 50,000 pairs a push holds by itself."""
    many = [f"chain-{n:03d}" for n in range(150)]
    sources = _Sources(
        _scores(_offer(INDEXER_A, many), _offer(INDEXER_B, ["mainnet"])),
        [_allocation(INDEXER_B, "A", int(NOW - 9 * DAY), chain="mainnet")],
    )

    with caplog.at_level(logging.WARNING):
        targets = sources.loader()()

    assert len(targets.pairs) == 101
    assert (INDEXER_B, "mainnet") in targets.pairs
    (warning,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert INDEXER_A in warning.getMessage()
    assert "150" in warning.getMessage()


def _warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_refused_chain_names_get_1_warning_per_refresh(caplog):
    """Nothing is dropped silently, and 1 line covers them however many there are."""
    sources = _Sources(
        _scores(
            _offer(INDEXER_A, ["mainnet", "bad\nname", 7]),
            _offer(INDEXER_B, ["\ud800", "base"]),
        ),
        [],
    )

    with caplog.at_level(logging.DEBUG):
        targets = sources.loader()()

    assert targets.pairs == ((INDEXER_A, "mainnet"), (INDEXER_B, "base"))
    (warning,) = _warnings(caplog)
    assert "3 chain names" in warning and "listed by 2 indexers" in warning
    assert INDEXER_A in warning and "bad\\nname" in warning and "\n" not in warning


def test_unreadable_price_fields_get_1_warning_per_refresh(caplog):
    sources = _Sources(
        _scores(
            _offer(INDEXER_A, ["mainnet"], "x{"),
            _offer(INDEXER_B, ["mainnet"], ["mainnet"]),
            _offer("0x" + "c" * 40, ["mainnet"]),
        ),
        [],
    )

    with caplog.at_level(logging.DEBUG):
        targets = sources.loader()()

    assert targets.pairs == (("0x" + "c" * 40, "mainnet"),)
    (warning,) = _warnings(caplog)
    assert "2 indexers" in warning and INDEXER_A in warning


def test_a_listed_chain_without_a_price_is_logged_at_debug_only(caplog):
    """An indexer listing a chain it hasn't priced yet is ordinary."""
    sources = _Sources(
        _scores(_offer(INDEXER_A, ["mainnet", "matic", "base"], {"mainnet": 100.0})), []
    )

    with caplog.at_level(logging.DEBUG):
        sources.loader()()

    assert _warnings(caplog) == []
    assert any("2 listed chains" in r.getMessage() for r in caplog.records)


def test_scores_without_a_price_field_check_every_listed_chain_with_a_warning(caplog):
    """Selection applies no price filter when the scores carry no price field at all, so every
    listed chain can be picked and is worth checking."""
    records = [{"indexer": INDEXER_A, "dips_supported_networks": ["mainnet", "base"]}]
    sources = _Sources(_scores(*records), [])

    with caplog.at_level(logging.WARNING):
        targets = sources.loader()()

    assert targets.pairs == ((INDEXER_A, "base"), (INDEXER_A, "mainnet"))
    (warning,) = _warnings(caplog)
    assert "dips_min_grt_per_30_days" in warning


def test_first_allocations_are_cached_across_refreshes():
    young = _allocation(INDEXER_A, "B", int(NOW - DAY))
    sources = _Sources(
        _scores(_offer(INDEXER_A, ["arbitrum-one"])),
        [young],
        {(INDEXER_A, young.deployment_id): int(NOW - 2 * DAY)},
    )
    loader = sources.loader()

    assert loader().eligible == {}
    assert loader().eligible == {}
    assert len(sources.first_allocation_reads) == 1


@pytest.mark.parametrize("response", [{}, {"scores": []}, {"scores": "nope"}, None])
def test_scores_without_records_fail_the_refresh(response):
    sources = _Sources(response, [])

    with pytest.raises(ValueError, match="no score records"):
        sources.loader()()


def test_no_dips_indexers_means_no_pairs_and_no_subgraph_read():
    sources = _Sources(_scores(_offer(INDEXER_A, [])), [])

    assert sources.loader()() == Targets(pairs=(), eligible={})
    assert sources.allocation_reads == []
