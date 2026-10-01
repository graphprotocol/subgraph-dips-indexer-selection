"""Golden tests pinning the exact rows the sample pass keeps for a fixed seed.

Both the per-partition worker and the parent merge run Algorithm R off seeded RNGs, so a fixed
input and seed always keep the same rows in the same order. These tests fail if a change alters
which rows are kept, their order, or their field values.
"""

import logging
import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import redpanda  # noqa: E402
from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from redpanda import RedpandaProvider, _sample_partition_worker  # noqa: E402

PAIR_A = (b"\xaa" * 32, b"\x0a" * 20)
PAIR_B = (b"\xbb" * 32, b"\x0b" * 20)
PAIR_C = (b"\xcc" * 32, b"\x0c" * 20)


def _add_attempt(query, pair, url, result, chain, i):
    attempt = query.indexer_queries.add()
    attempt.deployment, attempt.indexer = pair
    attempt.url = url
    attempt.result = result
    attempt.indexed_chain = chain
    attempt.fee_grt = i * 0.25
    attempt.response_time_ms = 100 + i
    attempt.blocks_behind = i


def _message(i, gateway_id="gw-1"):
    """Message i has 1 attempt per pair, alternating result and chain, plus 2 that are skipped."""
    query = ClientQueryProtobuf()
    query.gateway_id = gateway_id
    query.query_id = f"q-{i:02d}"
    result = "success" if i % 2 == 0 else "timeout"
    chain = "mainnet" if i % 3 else "arbitrum-one"
    _add_attempt(query, PAIR_A, "https://a.example.com", result, chain, i)
    _add_attempt(query, PAIR_B, "https://b.example.com/", result, chain, i)
    _add_attempt(query, (b"\xaa" * 31, b"\x0a" * 20), "https://a.example.com", result, chain, i)
    _add_attempt(query, PAIR_A, "", result, chain, i)
    msg = MagicMock()
    msg.error.return_value = None
    msg.timestamp.return_value = (1, 1_000 + i)
    msg.value.return_value = query.SerializeToString()
    msg.offset.return_value = i
    return msg


def test_sample_worker_keeps_the_same_rows_for_a_fixed_seed():
    """12 in-window messages per pair against a cap of 3, so every pair goes through replacement."""
    messages = [_message(i) for i in range(12)]
    messages.insert(5, _message(99, gateway_id="gw-2"))
    consumer = MagicMock()
    consumer.consume.side_effect = [messages, [], [], []]

    with patch("confluent_kafka.Consumer", return_value=consumer):
        reservoirs, counts, filtered = _sample_partition_worker(
            ("gateway_queries", 3, 0, 10**12, {}, {"gw-1"}, 3, 20260420, 100)
        )

    url_a, url_b = "https://a.example.com/", "https://b.example.com/"
    assert dict(reservoirs) == {
        PAIR_A: [
            ("q-05", 1.25, 1_005, 5, 105, "timeout", "mainnet", url_a),
            ("q-06", 1.5, 1_006, 6, 106, "200 OK", "arbitrum-one", url_a),
            ("q-10", 2.5, 1_010, 10, 110, "200 OK", "mainnet", url_a),
        ],
        PAIR_B: [
            ("q-10", 2.5, 1_010, 10, 110, "200 OK", "mainnet", url_b),
            ("q-08", 2.0, 1_008, 8, 108, "200 OK", "mainnet", url_b),
            ("q-07", 1.75, 1_007, 7, 107, "timeout", "mainnet", url_b),
        ],
    }
    assert dict(counts) == {PAIR_A: 12, PAIR_B: 12}
    assert filtered == 1


def _row(qid):
    return (qid, 0.5, 1_000, 2, 100, "200 OK", "mainnet", "https://x.example.com/")


def _worker_result(rows_per_pair, filtered):
    reservoirs = {pair: [_row(f"{tag}-{i}") for i in range(n)] for pair, tag, n in rows_per_pair}
    return reservoirs, {pair: n for pair, _, n in rows_per_pair}, filtered


def test_sample_pass_merge_keeps_the_same_rows_for_a_fixed_seed(caplog):
    """3 worker reservoirs overflow a cap of 4 for 2 pairs; a third pair stays under it."""
    results = [
        _worker_result([(PAIR_A, "a0", 4), (PAIR_B, "b0", 3)], filtered=1),
        _worker_result([(PAIR_A, "a1", 5), (PAIR_C, "c1", 2)], filtered=2),
        _worker_result([(PAIR_A, "a2", 3), (PAIR_B, "b2", 3)], filtered=3),
    ]
    provider = RedpandaProvider()
    provider._cached_partitions = [MagicMock(), MagicMock(), MagicMock()]

    with (
        patch.dict(os.environ, {"SCORING_SEED": "20260420"}),
        patch.object(provider, "_run_partition_workers", return_value=results) as run,
        caplog.at_level(logging.INFO, logger="redpanda"),
    ):
        provider._sample_pass(date(2024, 1, 1), num_days=1, rows_to_use=4)

    # Looked up at call time: another test module reloads redpanda, replacing its functions.
    run.assert_called_once_with(redpanda._sample_partition_worker, ANY, ANY, 4, 20260420)
    df = provider._row_cache_df
    assert df is not None
    assert df["query_id"].tolist() == [
        *("a2-0", "a0-1", "a0-0", "a0-2"),
        *("b0-2", "b0-1", "b2-2", "b0-0"),
        *("c1-0", "c1-1"),
    ]
    assert "(6 messages filtered by gateway ID)" in caplog.text
    assert provider._row_cache_start_date == date(2024, 1, 1)
    assert provider._row_cache_num_days == 1
    assert provider._row_cache_rows_to_use == 4


def test_sample_pass_with_no_partitions_caches_an_empty_frame():
    provider = RedpandaProvider()
    provider._cached_partitions = []

    with patch.object(provider, "_run_partition_workers") as run:
        provider._sample_pass(date(2024, 1, 1), num_days=1, rows_to_use=4)

    run.assert_not_called()
    df = provider._row_cache_df
    assert df is not None and df.empty
    assert "query_id" in df.columns
    assert provider._row_cache_start_date == date(2024, 1, 1)
    assert provider._row_cache_num_days == 1
    assert provider._row_cache_rows_to_use == 4
