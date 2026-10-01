"""Tests for the Algorithm-R streaming truncation in the sample-pass merge.

Before this change, the parent extended every worker's rows into a single
list per pair, then shuffled and truncated to rows_to_use. On production data
with 8 workers that held up to 8 * rows_to_use rows per pair transiently —
enough to push the pod past its 50Gi memory limit.

The fix absorbs each worker's rows one at a time, applying Algorithm R at the
parent so the merged reservoir never exceeds rows_to_use for any pair.
These tests assert the bounded footprint and that retained rows all came from
the union of worker inputs.
"""

import os
import sys
from concurrent.futures import Executor, Future
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from redpanda import RedpandaProvider, _merge_worker_reservoirs  # noqa: E402


class _InlineExecutor(Executor):
    """Executor that runs callables inline — bypasses pickling for mocked workers."""

    def __init__(self, *_args, **_kwargs):
        pass

    def map(self, fn, *iterables, **kwargs):
        if len(iterables) > 1:
            return [fn(*args) for args in zip(*iterables)]
        return [fn(args) for args in iterables[0]]

    def submit(self, fn, *args, **kwargs):
        fut = Future()
        fut.set_result(fn(*args, **kwargs))
        return fut


def _fake_topic_partition(partition: int):
    tp = MagicMock()
    tp.topic = "gateway_queries"
    tp.partition = partition
    tp.offset = 0
    return tp


def _build_worker_result(pair, rows_per_worker, worker_idx):
    """Build a (reservoirs, counts, filtered) tuple for one simulated worker.

    rows_per_worker rows are emitted for `pair`, each with a unique query_id
    that encodes worker and row index so we can assert provenance downstream.
    """
    rows = []
    for i in range(rows_per_worker):
        qid = f"w{worker_idx:02d}-r{i:04d}"
        # (query_id, fee, ts_ms, blocks_behind, response_time_ms,
        #  status, subgraph_network, url) — matches _ROW_COLUMNS order.
        rows.append((qid, 0.001, 1000 + i, 0, 100, "200 OK", "mainnet", "https://x.example.com/"))
    return ({pair: rows}, {pair: rows_per_worker}, 0)


def _build_provider() -> RedpandaProvider:
    with patch.dict(
        os.environ,
        {
            "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
            "REDPANDA_TOPIC": "gateway_queries",
            "SCORING_SEED": "20260420",
        },
    ):
        return RedpandaProvider()


def test_merged_reservoir_capped_at_rows_to_use_per_pair():
    """8 workers each return 2 * rows_to_use for the same pair; result is exactly rows_to_use."""
    rows_to_use = 10
    num_workers = 8
    pair = (bytes(32), b"\x01" * 20)

    results_queue = [_build_worker_result(pair, 2 * rows_to_use, w) for w in range(num_workers)]

    call_count = [0]

    def fake_worker(_args):
        result = results_queue[call_count[0]]
        call_count[0] += 1
        return result

    provider = _build_provider()
    # Seed the count cache so _ensure_count_cache short-circuits
    provider._count_cache_start_date = date(2024, 1, 1)
    provider._count_cache_num_days = 1
    provider._count_cache = {}
    provider._fees_per_indexer = {}
    # Pre-populate partitions so _resolve_partitions isn't called
    provider._cached_partitions = [_fake_topic_partition(i) for i in range(num_workers)]

    with (
        patch("redpanda._sample_partition_worker", side_effect=fake_worker),
        patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
    ):
        provider._sample_pass(date(2024, 1, 1), num_days=1, rows_to_use=rows_to_use)

    df = provider._row_cache_df
    assert df is not None
    assert len(df) == rows_to_use, (
        f"Merged reservoir has {len(df)} rows, expected exactly {rows_to_use}. "
        f"The streaming truncation should bound per-pair size at rows_to_use."
    )


def test_retained_rows_come_from_worker_inputs():
    """Every retained query_id was present in some worker's input.

    Guards against merge bugs that could fabricate rows or leave duplicates.
    """
    rows_to_use = 25
    num_workers = 4
    pair = (bytes(32), b"\x01" * 20)

    results_queue = [_build_worker_result(pair, 2 * rows_to_use, w) for w in range(num_workers)]
    all_input_qids = {
        row[0] for reservoirs, _, _ in results_queue for rows in reservoirs.values() for row in rows
    }

    call_count = [0]

    def fake_worker(_args):
        result = results_queue[call_count[0]]
        call_count[0] += 1
        return result

    provider = _build_provider()
    provider._count_cache_start_date = date(2024, 1, 1)
    provider._count_cache_num_days = 1
    provider._count_cache = {}
    provider._fees_per_indexer = {}
    provider._cached_partitions = [_fake_topic_partition(i) for i in range(num_workers)]

    with (
        patch("redpanda._sample_partition_worker", side_effect=fake_worker),
        patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
    ):
        provider._sample_pass(date(2024, 1, 1), num_days=1, rows_to_use=rows_to_use)

    df = provider._row_cache_df
    assert df is not None
    retained_qids = set(df["query_id"].tolist())

    assert retained_qids.issubset(all_input_qids), (
        f"Retained query_ids not in union of worker inputs: {retained_qids - all_input_qids}"
    )
    assert len(retained_qids) == len(df), "Retained rows contain duplicates"
    assert len(df) == rows_to_use


def test_small_inputs_dont_get_truncated():
    """If total input < rows_to_use, merge returns everything (no lossy truncation)."""
    rows_to_use = 100
    num_workers = 3
    pair = (bytes(32), b"\x01" * 20)

    # Each worker returns 10 rows — 30 total, well under the 100 cap.
    results_queue = [_build_worker_result(pair, 10, w) for w in range(num_workers)]
    total_input = sum(
        len(rows) for reservoirs, _, _ in results_queue for rows in reservoirs.values()
    )

    call_count = [0]

    def fake_worker(_args):
        result = results_queue[call_count[0]]
        call_count[0] += 1
        return result

    provider = _build_provider()
    provider._count_cache_start_date = date(2024, 1, 1)
    provider._count_cache_num_days = 1
    provider._count_cache = {}
    provider._fees_per_indexer = {}
    provider._cached_partitions = [_fake_topic_partition(i) for i in range(num_workers)]

    with (
        patch("redpanda._sample_partition_worker", side_effect=fake_worker),
        patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
    ):
        provider._sample_pass(date(2024, 1, 1), num_days=1, rows_to_use=rows_to_use)

    df = provider._row_cache_df
    assert df is not None
    assert len(df) == total_input, (
        f"Got {len(df)} rows, expected all {total_input} inputs preserved when under cap"
    )


def test_multiple_pairs_independently_capped():
    """Each (dep, idx) pair is capped independently — no cross-pair interference."""
    rows_to_use = 8
    num_workers = 3
    pair_a = (bytes(32), b"\x01" * 20)
    pair_b = (bytes(range(32)), b"\x02" * 20)

    # Each worker returns rows for both pairs, each 2x the cap.
    results_queue = []
    for w in range(num_workers):
        reservoirs = {}
        counts = {}
        for pair_idx, pair in enumerate([pair_a, pair_b]):
            rows = []
            for i in range(2 * rows_to_use):
                qid = f"p{pair_idx}-w{w:02d}-r{i:04d}"
                rows.append(
                    (qid, 0.001, 1000 + i, 0, 100, "200 OK", "mainnet", "https://x.example.com/")
                )
            reservoirs[pair] = rows
            counts[pair] = 2 * rows_to_use
        results_queue.append((reservoirs, counts, 0))

    call_count = [0]

    def fake_worker(_args):
        result = results_queue[call_count[0]]
        call_count[0] += 1
        return result

    provider = _build_provider()
    provider._count_cache_start_date = date(2024, 1, 1)
    provider._count_cache_num_days = 1
    provider._count_cache = {}
    provider._fees_per_indexer = {}
    provider._cached_partitions = [_fake_topic_partition(i) for i in range(num_workers)]

    with (
        patch("redpanda._sample_partition_worker", side_effect=fake_worker),
        patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
    ):
        provider._sample_pass(date(2024, 1, 1), num_days=1, rows_to_use=rows_to_use)

    df = provider._row_cache_df
    assert df is not None
    counts_per_pair = df.groupby(["deployment_hash", "indexer"]).size().to_dict()
    assert len(counts_per_pair) == 2
    for pair_count in counts_per_pair.values():
        assert pair_count == rows_to_use, (
            f"Pair got {pair_count} rows, expected exactly {rows_to_use}"
        )


def test_merge_empties_worker_results_and_totals_filtered_counts():
    """Under the cap every row is kept, absorbed from the last worker's result first."""
    pair = (bytes(32), b"\x01" * 20)
    results = [(*_build_worker_result(pair, 2, w)[:2], w + 1) for w in range(3)]

    merged, total_filtered = _merge_worker_reservoirs(results, rows_to_use=10, seed=1)

    assert results == []
    assert total_filtered == 6
    assert [row[0] for row in merged[pair]] == [
        *("w02-r0000", "w02-r0001"),
        *("w01-r0000", "w01-r0001"),
        *("w00-r0000", "w00-r0001"),
    ]
