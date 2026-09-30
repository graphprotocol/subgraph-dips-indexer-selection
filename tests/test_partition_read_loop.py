"""Tests for _iter_partition_queries, the partition read loop shared by both Redpanda passes."""

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

# Make the cronjob package importable.
jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from redpanda import _iter_partition_queries, _PartitionReadStats  # noqa: E402


def _fake_kafka_message(ts_ms, offset, gateway_id="mainnet-gw", value=None, error=None):
    if value is None:
        query = ClientQueryProtobuf()
        query.gateway_id = gateway_id
        query.query_id = f"q-{offset}"
        value = query.SerializeToString()
    msg = MagicMock()
    msg.error.return_value = error
    msg.timestamp.return_value = (1, ts_ms)
    msg.value.return_value = value
    msg.offset.return_value = offset
    return msg


def _read(batches, end_ts_ms=5_000, gw_filter=None):
    consumer = MagicMock()
    consumer.consume.side_effect = batches
    stats = _PartitionReadStats()
    with patch("confluent_kafka.Consumer", return_value=consumer):
        reader = _iter_partition_queries(
            "count", "gateway_queries", 0, 0, end_ts_ms, {}, gw_filter, 100, pairs={}, stats=stats
        )
        yielded = [(query.query_id, ts_ms) for query, ts_ms in reader]
    return yielded, stats, consumer


class TestIterPartitionQueries:
    def test_skips_errors_unparseable_and_filtered_messages(self):
        """Errors are not counted; unparseable payloads count as messages but not as filtered."""
        batch = [
            _fake_kafka_message(1_000, 1, error="broker error"),
            _fake_kafka_message(1_000, 2, value=b"\x0a\xff"),
            _fake_kafka_message(1_000, 3, gateway_id="testnet-gw"),
            _fake_kafka_message(1_000, 4),
        ]

        yielded, stats, consumer = _read([batch, [], [], []], gw_filter={"mainnet-gw"})

        assert yielded == [("q-4", 1_000)]
        assert stats.messages == 3
        assert stats.filtered == 1
        consumer.close.assert_called_once()

    def test_stops_at_first_message_past_end_ts(self):
        """Nothing after the first message past the window is read or polled for."""
        batch = [
            _fake_kafka_message(1_000, 1),
            _fake_kafka_message(5_001, 2),
            _fake_kafka_message(2_000, 3),
        ]

        yielded, stats, consumer = _read([batch])

        assert yielded == [("q-1", 1_000)]
        assert stats.messages == 1
        assert consumer.consume.call_count == 1
        consumer.close.assert_called_once()

    def test_negative_timestamp_is_replaced_with_now(self):
        before_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        yielded, _, _ = _read([[_fake_kafka_message(-1, 1)], [], [], []], end_ts_ms=10**15)
        after_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        [(query_id, ts_ms)] = yielded
        assert query_id == "q-1"
        assert before_ms <= ts_ms <= after_ms
