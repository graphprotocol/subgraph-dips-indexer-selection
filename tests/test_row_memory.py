"""Memory regression test for the sample worker's row representation.

The worker held multi-GB reservoirs of row dicts in production, which sent the
cronjob OOM. The fix changed rows from dicts to tuples, dropped indexer/
deployment bytes from the row (they come from the reservoir key instead), and
interns url/status/subgraph_network. This test locks in those decisions: if a
future change reverts to dicts or re-adds per-row key fields, the per-row byte
count will jump and this test will fail.

The threshold is intentionally loose — the point is to catch shape regressions,
not to gate on sub-byte micro-optimisations.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from redpanda import _InternCache, _sample_partition_worker  # noqa: E402


def _build_message(query_id: str, indexer: bytes, deployment: bytes) -> bytes:
    query = ClientQueryProtobuf()
    query.gateway_id = "gw-1"
    query.query_id = query_id
    attempt = query.indexer_queries.add()
    attempt.indexer = indexer
    attempt.deployment = deployment
    attempt.url = "https://indexer.example.com/"
    attempt.indexed_chain = "mainnet"
    attempt.fee_grt = 0.001
    attempt.response_time_ms = 100
    attempt.blocks_behind = 0
    attempt.result = "success"
    return query.SerializeToString()


def _fake_kafka_message(ts_ms: int, value: bytes):
    msg = MagicMock()
    msg.error.return_value = None
    msg.timestamp.return_value = (1, ts_ms)
    msg.value.return_value = value
    msg.offset.return_value = 0
    return msg


def _deep_size(obj, seen=None) -> int:
    """Recursive memory footprint in bytes, deduplicating shared references.

    This intentionally shares `seen` across calls so interned strings are
    counted once regardless of how many rows reference them — which is the
    whole point of interning. That makes the output a realistic measure of
    retained memory, not a naive sum of per-object sizes.
    """
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        for k, v in obj.items():
            size += _deep_size(k, seen) + _deep_size(v, seen)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        for item in obj:
            size += _deep_size(item, seen)
    return size


def test_sample_worker_per_row_memory_under_threshold():
    """Retained reservoir bytes-per-row must stay below the regression ceiling.

    Synthetic workload: 50 distinct (dep, idx) pairs, 200 messages per pair.
    rows_to_use=100 so reservoirs fill and each pair hits the replacement
    phase of Algorithm R. Expected retained rows: 50 * 100 = 5000.

    A tuple-based row with interning measures ~200-350 B/row in practice.
    Reverting to the pre-fix dict would land in the ~700-1000 B/row range.
    The 500 B/row ceiling below sits between the two, so any shape regression
    trips the assertion.
    """
    num_pairs = 50
    msgs_per_pair = 200
    rows_to_use = 100

    messages = []
    for pair_idx in range(num_pairs):
        indexer = bytes([pair_idx]) + b"\x01" * 19
        deployment = bytes([pair_idx]) + b"\x02" * 31
        for msg_idx in range(msgs_per_pair):
            qid = f"q-{pair_idx:03d}-{msg_idx:04d}-JFK"
            value = _build_message(qid, indexer, deployment)
            messages.append(_fake_kafka_message(1000 + msg_idx, value))

    mock_consumer = MagicMock()
    mock_consumer.consume.side_effect = [messages, [], [], []]

    with patch("confluent_kafka.Consumer", return_value=mock_consumer):
        reservoirs, counts, filtered = _sample_partition_worker(
            (
                "gateway_queries",
                0,
                0,
                999999999999,
                {"bootstrap.servers": "x"},
                None,
                rows_to_use,
                20260420,
                1_000_000,
            )
        )

    retained = sum(len(r) for r in reservoirs.values())
    assert retained == num_pairs * rows_to_use, (
        f"Expected reservoir fill of {num_pairs * rows_to_use} rows, got {retained}"
    )

    reservoir_bytes = _deep_size(reservoirs)
    bytes_per_row = reservoir_bytes / retained

    max_bytes_per_row = 500
    assert bytes_per_row < max_bytes_per_row, (
        f"Reservoir footprint {reservoir_bytes:,} B for {retained} rows "
        f"= {bytes_per_row:.0f} B/row, above the {max_bytes_per_row} B/row "
        f"regression ceiling. Has the row shape regressed to a dict?"
    )


def test_intern_cache_shares_string_identity_across_rows():
    """Repeated url/status/subgraph_network values across reservoir rows must
    share string identity (not just equality), proving the worker-local intern
    cache is canonicalising rather than each row owning its own copy.

    A regression here — cache keyed wrong, value not stored, or sys.intern
    dropped — would still pass string equality checks but leak memory at
    production scale. The `is` check is the only way to catch it.
    """
    indexer = b"\x01" * 20
    deployment = b"\x02" * 32

    # 100 messages with identical url/status/chain so every row should hit
    # the warm-cache fast path from the second message onwards.
    messages = [
        _fake_kafka_message(
            1000 + i,
            _build_message(f"q-{i:04d}", indexer, deployment),
        )
        for i in range(100)
    ]
    mock_consumer = MagicMock()
    mock_consumer.consume.side_effect = [messages, [], [], []]

    with patch("confluent_kafka.Consumer", return_value=mock_consumer):
        reservoirs, _, _ = _sample_partition_worker(
            (
                "gateway_queries",
                0,
                0,
                999999999999,
                {"bootstrap.servers": "x"},
                None,
                1000,
                20260420,
                1_000_000,
            )
        )

    all_rows = [row for rows in reservoirs.values() for row in rows]
    assert len(all_rows) == 100, f"Expected 100 rows, got {len(all_rows)}"

    first = all_rows[0]
    _, _, _, _, _, first_status, first_chain, first_url = first
    for row in all_rows[1:]:
        _, _, _, _, _, status, chain, url = row
        assert status is first_status, (
            "status is equal but not identical — intern cache not canonicalising"
        )
        assert chain is first_chain, (
            "subgraph_network is equal but not identical — intern cache not canonicalising"
        )
        assert url is first_url, "url is equal but not identical — intern cache not canonicalising"


def test_intern_cache_computes_each_value_once():
    calls = []

    def canonical(raw: str) -> str:
        calls.append(raw)
        return raw.upper()

    cache = _InternCache(canonical)
    # Built at runtime so neither key is already an interned constant.
    first = cache["".join(["main", "net"])]
    again = cache["".join(["main", "net"])]

    assert first == "MAINNET"
    assert first is sys.intern("MAINNET")
    assert again is first
    assert calls == ["mainnet"]


def test_intern_cache_preserves_distinct_values_under_rotation():
    """When a pair rotates between two URLs mid-window, both URLs appear in
    the reservoir — the cache shares identity *within* each URL group but
    does not collapse distinct URLs into one.

    Covers the production scenario where an indexer changes their registered
    URL during the 28-day window. The prior dict-row representation carried
    per-row URLs by construction; the tuple + cache representation must
    preserve the same per-row semantics.
    """
    indexer = b"\x01" * 20
    deployment = b"\x02" * 32

    def _msg_with_url(i: int, url: str) -> bytes:
        query = ClientQueryProtobuf()
        query.gateway_id = "gw-1"
        query.query_id = f"q-{i:04d}"
        attempt = query.indexer_queries.add()
        attempt.indexer = indexer
        attempt.deployment = deployment
        attempt.url = url
        attempt.indexed_chain = "mainnet"
        attempt.fee_grt = 0.001
        attempt.response_time_ms = 100
        attempt.blocks_behind = 0
        attempt.result = "success"
        return query.SerializeToString()

    url_a = "https://indexer-a.example.com/"
    url_b = "https://indexer-b.example.com/"

    # 40 rows with url_a, then 40 rows with url_b — simulates mid-window rotation.
    messages = []
    for i in range(40):
        messages.append(_fake_kafka_message(1000 + i, _msg_with_url(i, url_a)))
    for i in range(40, 80):
        messages.append(_fake_kafka_message(1000 + i, _msg_with_url(i, url_b)))

    mock_consumer = MagicMock()
    mock_consumer.consume.side_effect = [messages, [], [], []]

    with patch("confluent_kafka.Consumer", return_value=mock_consumer):
        reservoirs, _, _ = _sample_partition_worker(
            (
                "gateway_queries",
                0,
                0,
                999999999999,
                {"bootstrap.servers": "x"},
                None,
                1000,
                20260420,
                1_000_000,
            )
        )

    all_rows = [row for rows in reservoirs.values() for row in rows]
    assert len(all_rows) == 80

    urls_seen = {row[7] for row in all_rows}
    assert len(urls_seen) == 2, (
        f"Expected 2 distinct URLs in reservoir, got {len(urls_seen)}: {urls_seen}"
    )

    # Within each URL group, all rows must share identity.
    rows_url_a = [row for row in all_rows if row[7] == url_a]
    rows_url_b = [row for row in all_rows if row[7] == url_b]
    for row in rows_url_a[1:]:
        assert row[7] is rows_url_a[0][7], "url_a copies not canonicalised in cache"
    for row in rows_url_b[1:]:
        assert row[7] is rows_url_b[0][7], "url_b copies not canonicalised in cache"


def test_reservoir_rows_are_tuples_in_expected_order():
    """Shape-level lock-in: each reservoir row is an 8-tuple in _ROW_COLUMNS order.

    Guards against silent reintroduction of a dict row or a column reorder
    that would desync from _build_dataframe's explicit column spec.
    """
    from redpanda import _ROW_COLUMNS

    indexer = b"\x01" * 20
    deployment = b"\x02" * 32
    messages = [
        _fake_kafka_message(
            1000 + i,
            _build_message(f"q-{i:03d}", indexer, deployment),
        )
        for i in range(5)
    ]
    mock_consumer = MagicMock()
    mock_consumer.consume.side_effect = [messages, [], [], []]

    with patch("confluent_kafka.Consumer", return_value=mock_consumer):
        reservoirs, _, _ = _sample_partition_worker(
            (
                "gateway_queries",
                0,
                0,
                999999999999,
                {"bootstrap.servers": "x"},
                None,
                100,
                20260420,
                1_000_000,
            )
        )

    assert len(_ROW_COLUMNS) == 8
    for rows in reservoirs.values():
        for row in rows:
            assert isinstance(row, tuple), f"row is {type(row).__name__}, expected tuple"
            assert len(row) == len(_ROW_COLUMNS), (
                f"row has {len(row)} fields, _ROW_COLUMNS declares {len(_ROW_COLUMNS)}"
            )
            # Column-order spot checks — catch silent reorderings.
            query_id, fee, ts_ms, blocks_behind, response_time_ms, status, chain, url = row
            assert query_id.startswith("q-")
            assert fee == 0.001
            assert ts_ms >= 1000
            assert blocks_behind == 0
            assert response_time_ms == 100
            assert status == "200 OK"
            assert chain == "mainnet"
            assert url.endswith("/")
