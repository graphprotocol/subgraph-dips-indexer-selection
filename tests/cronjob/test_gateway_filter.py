"""Tests for the REDPANDA_GATEWAY_IDS filter on RedpandaProvider."""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Make the cronjob package importable.
jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from redpanda import RedpandaProvider, _count_partition_worker  # noqa: E402


def _build_message(gateway_id: str, indexer: bytes, deployment: bytes, fee: float) -> bytes:
    """Build a serialised ClientQueryProtobuf with one indexer attempt."""
    query = ClientQueryProtobuf()
    query.gateway_id = gateway_id
    query.query_id = "test-query-JFK"
    attempt = query.indexer_queries.add()
    attempt.indexer = indexer
    attempt.deployment = deployment
    attempt.fee_grt = fee
    attempt.url = "https://indexer.example.com/"
    attempt.result = "success"
    return query.SerializeToString()


def _fake_kafka_message(ts_ms: int, value: bytes):
    """Create a mock Kafka message."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.timestamp.return_value = (1, ts_ms)
    msg.value.return_value = value
    msg.offset.return_value = 0
    return msg


class TestGatewayIdFilter:
    """Test the gateway_id filter initialization and message filtering."""

    def test_filter_active_when_env_set(self):
        """When REDPANDA_GATEWAY_IDS is set, only those gateways are allowed."""
        with patch.dict(os.environ, {"REDPANDA_GATEWAY_IDS": "gw-1,gw-2"}):
            provider = RedpandaProvider()
        assert provider._gateway_id_filter == {"gw-1", "gw-2"}

    def test_filter_none_when_env_unset(self):
        """When REDPANDA_GATEWAY_IDS is not set, no filtering occurs."""
        with patch.dict(os.environ, {}, clear=False):
            env = os.environ.copy()
            env.pop("REDPANDA_GATEWAY_IDS", None)
            with patch.dict(os.environ, env, clear=True):
                provider = RedpandaProvider()
        assert provider._gateway_id_filter is None

    def test_filter_none_when_env_empty(self):
        """Empty string is treated the same as unset."""
        with patch.dict(os.environ, {"REDPANDA_GATEWAY_IDS": ""}):
            provider = RedpandaProvider()
        assert provider._gateway_id_filter is None

    def test_filter_strips_whitespace(self):
        """Whitespace around gateway IDs is stripped."""
        with patch.dict(os.environ, {"REDPANDA_GATEWAY_IDS": " gw-1 , gw-2 "}):
            provider = RedpandaProvider()
        assert provider._gateway_id_filter == {"gw-1", "gw-2"}

    def test_filter_ignores_empty_segments(self):
        """Trailing commas or double commas don't create empty entries."""
        with patch.dict(os.environ, {"REDPANDA_GATEWAY_IDS": "gw-1,,gw-2,"}):
            provider = RedpandaProvider()
        assert provider._gateway_id_filter == {"gw-1", "gw-2"}

    def test_worker_skips_non_matching_gateway(self):
        """Messages from non-matching gateways are skipped by the worker."""
        indexer = b"\x01" * 20
        deployment = b"\x02" * 32
        value = _build_message("testnet-gw", indexer, deployment, 0.1)
        msg = _fake_kafka_message(1000, value)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[msg], [], [], []]

        with patch("confluent_kafka.Consumer", return_value=mock_consumer):
            config = {"bootstrap.servers": "localhost:9092"}
            gw_filter = {"mainnet-gw"}
            counts, fees, total, filtered = _count_partition_worker(
                ("gateway_queries", 0, 0, 999999999999, config, gw_filter, 1_000_000)
            )

        assert total == 1
        assert filtered == 1
        assert len(counts) == 0
        assert len(fees) == 0

    def test_worker_accepts_matching_gateway(self):
        """Messages from matching gateways are processed by the worker."""
        indexer = b"\x01" * 20
        deployment = b"\x02" * 32
        value = _build_message("mainnet-gw", indexer, deployment, 0.1)
        msg = _fake_kafka_message(1000, value)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[msg], [], [], []]

        with patch("confluent_kafka.Consumer", return_value=mock_consumer):
            config = {"bootstrap.servers": "localhost:9092"}
            gw_filter = {"mainnet-gw"}
            counts, fees, total, filtered = _count_partition_worker(
                ("gateway_queries", 0, 0, 999999999999, config, gw_filter, 1_000_000)
            )

        assert total == 1
        assert filtered == 0
        assert len(counts) == 1
        assert len(fees) == 1

    def test_worker_no_filter_passes_all(self):
        """When no filter is set, all messages are processed."""
        indexer = b"\x01" * 20
        deployment = b"\x02" * 32
        value = _build_message("any-gateway", indexer, deployment, 0.1)
        msg = _fake_kafka_message(1000, value)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[msg], [], [], []]

        with patch("confluent_kafka.Consumer", return_value=mock_consumer):
            config = {"bootstrap.servers": "localhost:9092"}
            counts, fees, total, filtered = _count_partition_worker(
                ("gateway_queries", 0, 0, 999999999999, config, None, 1_000_000)
            )

        assert total == 1
        assert filtered == 0
        assert len(counts) == 1
