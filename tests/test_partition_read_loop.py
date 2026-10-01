"""Tests for _iter_partition_queries, the partition read loop shared by both Redpanda passes."""

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

# Make the cronjob package importable.
jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from redpanda import (  # noqa: E402
    _iter_batch_queries,
    _iter_partition_queries,
    _PartitionReadStats,
)


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

    def test_an_empty_batch_between_data_does_not_end_the_read(self):
        """Only 3 empty consume() calls in a row end the read; data in between resets the run."""
        batches = [[_fake_kafka_message(1_000, 1)], [], [], [_fake_kafka_message(1_000, 2)]]

        yielded, stats, consumer = _read(batches + [[], [], []])

        assert yielded == [("q-1", 1_000), ("q-2", 1_000)]
        assert stats.messages == 2
        assert consumer.consume.call_count == 7

    def test_stats_are_set_when_the_reader_is_closed_mid_batch(self):
        batch = [_fake_kafka_message(1_000, offset, gateway_id="testnet-gw") for offset in (1, 2)]
        batch += [_fake_kafka_message(1_000, offset) for offset in (3, 4, 5)]
        consumer = MagicMock()
        consumer.consume.side_effect = [batch, [], [], []]
        stats = _PartitionReadStats()

        with patch("confluent_kafka.Consumer", return_value=consumer):
            reader = _iter_partition_queries(
                "count", "gateway_queries", 0, 0, 5_000, {}, {"mainnet-gw"}, 100, {}, stats
            )
            query, _ = next(reader)
            reader.close()

        assert query.query_id == "q-3"
        assert stats.messages == 3
        assert stats.filtered == 2
        consumer.close.assert_called_once()


def _drain(reader):
    """Return everything the generator yields plus its return value."""
    yielded = []
    while True:
        try:
            query, ts_ms = next(reader)
        except StopIteration as stop:
            return yielded, stop.value
        yielded.append((query.query_id, ts_ms))


class TestIterBatchQueries:
    def test_adds_to_running_stats_and_returns_false_within_the_window(self):
        stats = _PartitionReadStats(messages=5, filtered=1, last_offset=9)
        batch = [_fake_kafka_message(1_000, 10), _fake_kafka_message(1_000, 11, "testnet-gw")]

        yielded, past_end = _drain(_iter_batch_queries(batch, 5_000, {"mainnet-gw"}, stats))

        assert yielded == [("q-10", 1_000)]
        assert past_end is False
        assert (stats.messages, stats.filtered, stats.last_offset) == (7, 2, 11)

    def test_returns_true_at_the_first_message_past_the_window(self):
        stats = _PartitionReadStats()
        batch = [_fake_kafka_message(1_000, 1), _fake_kafka_message(5_001, 2)]

        yielded, past_end = _drain(_iter_batch_queries(batch, 5_000, None, stats))

        assert yielded == [("q-1", 1_000)]
        assert past_end is True
        assert (stats.messages, stats.last_offset) == (1, 1)
