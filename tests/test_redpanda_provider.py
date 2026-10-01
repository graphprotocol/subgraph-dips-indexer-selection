"""
Unit tests for RedpandaProvider and its supporting utilities.

Tests use the path-insertion pattern established in test_compute_scores_job.py
so that imports resolve correctly without installing the cronjob package.
"""

import os
import sys
from concurrent.futures import Executor, Future
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class _InlineExecutor(Executor):
    """Executor that runs callables inline (no child processes).

    Avoids pickling issues when mocking functions used by ProcessPoolExecutor.
    """

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


# Make the cronjob package importable.
jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import (  # noqa: E402
    ClientQueryProtobuf,
    IndexerQueryProtobuf,
)
from redpanda import (  # noqa: E402
    RedpandaProvider,
    _bytes_to_cid,
    _bytes_to_hex,
    _map_result_to_status,
)

# ---------------------------------------------------------------------------
# Proto helpers — build test messages using generated protobuf classes
# ---------------------------------------------------------------------------


def _build_indexer_attempt(
    indexer: bytes,
    deployment: bytes,
    indexed_chain: str,
    url: str,
    fee_grt: float,
    response_time_ms: int,
    result: str,
    blocks_behind: int,
) -> IndexerQueryProtobuf:
    """Build an IndexerQueryProtobuf message."""
    msg = IndexerQueryProtobuf()
    msg.indexer = indexer
    msg.deployment = deployment
    msg.allocation = b"\x00" * 20
    msg.indexed_chain = indexed_chain
    msg.url = url
    msg.fee_grt = fee_grt
    msg.response_time_ms = response_time_ms
    msg.result = result
    msg.blocks_behind = blocks_behind
    return msg


def _build_client_query(
    query_id: str,
    attempts: List[IndexerQueryProtobuf],
) -> bytes:
    """Encode a ClientQueryProtobuf message and return serialized bytes."""
    msg = ClientQueryProtobuf()
    msg.query_id = query_id
    for attempt in attempts:
        msg.indexer_queries.append(attempt)
    return msg.SerializeToString()


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

INDEXER_BYTES = bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
DEPLOYMENT_BYTES = bytes(range(32))  # 32 distinct bytes
EXPECTED_INDEXER = "0x" + INDEXER_BYTES.hex().lower()
EXPECTED_DEPLOYMENT_CID = _bytes_to_cid(DEPLOYMENT_BYTES)


# ---------------------------------------------------------------------------
# Encoding helper tests
# ---------------------------------------------------------------------------


class TestBytesToCid:
    def test_valid_32_bytes(self):
        # Arrange
        b = bytes(range(32))

        # Act
        result = _bytes_to_cid(b)

        # Assert
        assert result.startswith("Qm"), f"Expected CIDv0 starting with Qm, got {result!r}"
        assert len(result) == 46, f"Expected CIDv0 length 46, got {len(result)}"

    def test_wrong_length_returns_empty(self):
        assert _bytes_to_cid(b"\x01\x02\x03") == ""
        assert _bytes_to_cid(b"") == ""

    def test_deterministic(self):
        b = bytes(range(32))
        assert _bytes_to_cid(b) == _bytes_to_cid(b)


class TestBytesToHex:
    def test_valid_20_bytes(self):
        # Arrange
        b = bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")

        # Act
        result = _bytes_to_hex(b)

        # Assert
        assert result == "0xabcdef1234567890abcdef1234567890abcdef12"

    def test_wrong_length_returns_empty(self):
        assert _bytes_to_hex(b"\x01") == ""
        assert _bytes_to_hex(b"") == ""

    def test_lowercase(self):
        b = bytes.fromhex("ABCDEF1234567890ABCDEF1234567890ABCDEF12")
        assert _bytes_to_hex(b) == "0x" + "abcdef1234567890abcdef1234567890abcdef12"


class TestMapResultToStatus:
    def test_success_maps_to_200_ok(self):
        assert _map_result_to_status("success") == "200 OK"

    def test_other_values_pass_through(self):
        assert _map_result_to_status("Unavailable(MissingBlock)") == "Unavailable(MissingBlock)"
        assert _map_result_to_status("Timeout") == "Timeout"
        assert _map_result_to_status("") == ""


# ---------------------------------------------------------------------------
# Proto round-trip tests (generated classes)
# ---------------------------------------------------------------------------


class TestProtobufRoundTrip:
    def test_indexer_query_round_trip(self):
        # Arrange
        attempt = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=DEPLOYMENT_BYTES,
            indexed_chain="mainnet",
            url="https://indexer.example.com/",
            fee_grt=0.001,
            response_time_ms=42,
            result="success",
            blocks_behind=0,
        )

        # Act — serialize and re-parse
        raw = attempt.SerializeToString()
        parsed = IndexerQueryProtobuf()
        parsed.ParseFromString(raw)

        # Assert
        assert bytes(parsed.indexer) == INDEXER_BYTES
        assert bytes(parsed.deployment) == DEPLOYMENT_BYTES
        assert parsed.indexed_chain == "mainnet"
        assert parsed.url == "https://indexer.example.com/"
        assert abs(parsed.fee_grt - 0.001) < 1e-9
        assert parsed.response_time_ms == 42
        assert parsed.result == "success"
        assert parsed.blocks_behind == 0

    def test_client_query_with_attempts(self):
        # Arrange
        attempt = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=DEPLOYMENT_BYTES,
            indexed_chain="mainnet",
            url="https://indexer.example.com/",
            fee_grt=0.001,
            response_time_ms=100,
            result="success",
            blocks_behind=5,
        )
        raw = _build_client_query("abc123def456-JFK", [attempt])

        # Act
        msg = ClientQueryProtobuf()
        msg.ParseFromString(raw)

        # Assert
        assert msg.query_id == "abc123def456-JFK"
        assert len(msg.indexer_queries) == 1
        assert msg.indexer_queries[0].result == "success"
        assert msg.indexer_queries[0].response_time_ms == 100
        assert msg.indexer_queries[0].blocks_behind == 5

    def test_multiple_attempts(self):
        # Arrange
        a1 = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=DEPLOYMENT_BYTES,
            indexed_chain="mainnet",
            url="https://a.example.com/",
            fee_grt=0.001,
            response_time_ms=100,
            result="success",
            blocks_behind=0,
        )
        a2 = _build_indexer_attempt(
            indexer=bytes(20),
            deployment=DEPLOYMENT_BYTES,
            indexed_chain="mainnet",
            url="https://b.example.com/",
            fee_grt=0.002,
            response_time_ms=200,
            result="Timeout",
            blocks_behind=10,
        )
        raw = _build_client_query("qid-001-LAX", [a1, a2])

        # Act
        msg = ClientQueryProtobuf()
        msg.ParseFromString(raw)

        # Assert
        assert len(msg.indexer_queries) == 2
        assert msg.indexer_queries[0].result == "success"
        assert msg.indexer_queries[1].result == "Timeout"


# ---------------------------------------------------------------------------
# RedpandaProvider: write_scores / scores_exist_for_today
# ---------------------------------------------------------------------------


class TestScoresPush:
    """Tests for the HTTP push path — write_scores POSTs to iisa, idempotency uses GET."""

    def _make_scores_df(self, today: bool = True) -> pd.DataFrame:
        """Build a minimal scores DataFrame."""
        computed_at = (
            datetime.now(timezone.utc) if today else datetime(2000, 1, 1, tzinfo=timezone.utc)
        )
        return pd.DataFrame(
            [
                {
                    "indexer": EXPECTED_INDEXER,
                    "url": "https://indexer.example.com/",
                    "lat_normalized_score": 0.8,
                    "uptime_score": 0.95,
                    "success_rate": 0.99,
                    "stake_to_fees": float("nan"),
                    "computed_at": computed_at,
                }
            ]
        )

    def _build_provider(self, iisa_url: str = "http://iisa:8080") -> RedpandaProvider:
        with patch.dict(
            os.environ,
            {
                "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
                "IISA_API_URL": iisa_url,
                "IISA_PUSH_TOKEN": "test-token",
            },
        ):
            return RedpandaProvider()

    def test_write_scores_posts_to_iisa(self):
        """write_scores should POST a JSON array to IISA_API_URL/scores with bearer auth."""
        # Arrange
        provider = self._build_provider()

        # Act
        with patch("iisa_client.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"status": "success", "rows": 1}
            mock_request.return_value = mock_response

            provider.write_scores(self._make_scores_df())

        # Assert — one POST against /scores with the bearer header
        mock_request.assert_called_once()
        call = mock_request.call_args
        assert call.args[0] == "POST"
        assert call.args[1] == "http://iisa:8080/scores"
        assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
        assert call.kwargs["headers"]["Content-Type"] == "application/json"

        body = call.kwargs["json"]
        assert isinstance(body, list)
        assert len(body) == 1
        assert body[0]["indexer"] == EXPECTED_INDEXER

    def test_write_scores_raises_on_exhausted_retries(self):
        """All-retries-exhausted should raise IISAPushError with the expected call count."""
        import requests
        from iisa_client import RETRY_ATTEMPTS, IISAPushError

        provider = self._build_provider()

        with (
            patch("iisa_client.requests.request") as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            mock_request.side_effect = requests.ConnectionError("refused")

            with pytest.raises(IISAPushError) as exc_info:
                provider.write_scores(self._make_scores_df())

        # Exactly RETRY_ATTEMPTS calls were made, and one sleep fewer than
        # that (no sleep after the final failing attempt).
        assert mock_request.call_count == RETRY_ATTEMPTS
        assert mock_sleep.call_count == RETRY_ATTEMPTS - 1
        assert f"{RETRY_ATTEMPTS} attempts" in str(exc_info.value)

    def test_scores_exist_for_today_true(self):
        """GET /scores/status returning today's computed_at ⇒ skip recompute."""
        # Arrange
        provider = self._build_provider()
        today_iso = datetime.now(timezone.utc).isoformat()

        # Act
        with patch("iisa_client.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"computed_at": today_iso, "rows": 1}
            mock_request.return_value = mock_response

            assert provider.scores_exist_for_today() is True

        # Assert — GET with bearer auth
        call = mock_request.call_args
        assert call.args[0] == "GET"
        assert call.args[1] == "http://iisa:8080/scores/status"
        assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"

    def test_scores_exist_for_today_false_old_timestamp(self):
        """Pre-today computed_at ⇒ run again."""
        provider = self._build_provider()
        old_iso = datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()

        with patch("iisa_client.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"computed_at": old_iso, "rows": 1}
            mock_request.return_value = mock_response

            assert provider.scores_exist_for_today() is False

    def test_scores_exist_for_today_false_on_push_error(self):
        """Network failure on the status query ⇒ proceed with a fresh run (fail-safe)."""
        import requests

        provider = self._build_provider()

        with patch("iisa_client.requests.request") as mock_request, patch("iisa_client.time.sleep"):
            mock_request.side_effect = requests.ConnectionError("dns lookup failed")

            assert provider.scores_exist_for_today() is False

    def test_scores_exist_for_today_false_missing_computed_at(self):
        """Empty/missing computed_at ⇒ proceed with a fresh run."""
        provider = self._build_provider()

        with patch("iisa_client.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"computed_at": None, "rows": 0}
            mock_request.return_value = mock_response

            assert provider.scores_exist_for_today() is False


# ---------------------------------------------------------------------------
# Mock helpers for two-pass architecture
# ---------------------------------------------------------------------------


def _fake_kafka_message(ts_ms: int, value: bytes, partition: int = 0, offset: int = 0):
    """Create a minimal mock confluent_kafka Message."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.timestamp.return_value = (1, ts_ms)  # TIMESTAMP_CREATE_TIME
    msg.value.return_value = value
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


def _make_mock_consumer_class(consumer_mocks: List[MagicMock]) -> MagicMock:
    """
    Build a mock Consumer class that returns a different mock instance
    on each instantiation (resolution consumer, then per-partition consumers).
    """
    MockClass = MagicMock()
    MockClass.side_effect = consumer_mocks
    return MockClass


def _make_resolution_consumer(topic: str, partition_ids: List[int], start_offset: int = 100):
    """Build a mock consumer for partition resolution (list_topics + offsets_for_times)."""
    mock = MagicMock()

    mock_topic_meta = MagicMock()
    mock_topic_meta.partitions = {pid: MagicMock() for pid in partition_ids}
    mock.list_topics.return_value.topics = {topic: mock_topic_meta}

    resolved_tps = []
    for pid in partition_ids:
        tp = MagicMock()
        tp.topic = topic
        tp.partition = pid
        tp.offset = start_offset
        resolved_tps.append(tp)
    mock.offsets_for_times.return_value = resolved_tps

    return mock


def _make_partition_consumer(message_batches: List[List]):
    """
    Build a mock consumer for a single partition.

    message_batches is a list of lists: each inner list is returned by
    one call to consume(). Empty lists simulate timeouts.
    """
    mock = MagicMock()
    mock.consume.side_effect = message_batches
    return mock


# ---------------------------------------------------------------------------
# RedpandaProvider: two-pass caching tests
# ---------------------------------------------------------------------------


class TestRedpandaProviderCaching:
    """Tests for the two-pass caching architecture."""

    def _build_provider(self) -> RedpandaProvider:
        """Build a RedpandaProvider with mocked environment."""
        with patch.dict(
            os.environ,
            {
                "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
                "REDPANDA_TOPIC": "gateway_queries",
            },
        ):
            return RedpandaProvider()

    def _two_attempt_message(
        self,
        query_id: str,
        deployment: bytes,
        indexer1: bytes,
        indexer2: bytes,
        ts_ms: int,
    ) -> bytes:
        """Encode a ClientQueryProtobuf with two indexer attempts."""
        a1 = _build_indexer_attempt(
            indexer=indexer1,
            deployment=deployment,
            indexed_chain="mainnet",
            url="https://indexer1.example.com/",
            fee_grt=0.001,
            response_time_ms=50,
            result="success",
            blocks_behind=0,
        )
        a2 = _build_indexer_attempt(
            indexer=indexer2,
            deployment=deployment,
            indexed_chain="mainnet",
            url="https://indexer2.example.com/",
            fee_grt=0.002,
            response_time_ms=80,
            result="Unavailable(MissingBlock)",
            blocks_behind=3,
        )
        return _build_client_query(query_id, [a1, a2])

    def test_fetch_initial_query_results_counts(self):
        """fetch_initial_query_results should return true attempt counts (count pass only)."""
        # Arrange
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1
        ts_ms = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)

        deployment = bytes(range(32))
        indexer1 = INDEXER_BYTES
        indexer2 = bytes(20)

        msg_value = self._two_attempt_message("qid-001-JFK", deployment, indexer1, indexer2, ts_ms)
        fake_msg = _fake_kafka_message(ts_ms, msg_value)

        resolution_consumer = _make_resolution_consumer("gateway_queries", [0])
        count_consumer = _make_partition_consumer([[fake_msg], [], [], []])

        with (
            patch(
                "confluent_kafka.Consumer",
                side_effect=[resolution_consumer, count_consumer],
            ),
            patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
        ):
            result = provider.fetch_initial_query_results(start_date, num_days)

        # Assert — two (deployment, indexer) pairs, each seen once
        assert len(result) == 2, f"Expected 2 pairs, got {len(result)}: {result}"
        assert set(result["num_rows"].tolist()) == {1}

    def test_fetch_combined_query_results_rows_to_use_cap(self):
        """fetch_combined_query_results should cap each pair at rows_to_use."""
        # Arrange
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1
        base_ts_ms = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)

        deployment = bytes(range(32))
        indexer1 = INDEXER_BYTES

        # Build 5 messages for the same (deployment, indexer1) pair
        messages = []
        for i in range(5):
            a = _build_indexer_attempt(
                indexer=indexer1,
                deployment=deployment,
                indexed_chain="mainnet",
                url="https://indexer1.example.com/",
                fee_grt=0.001,
                response_time_ms=50 + i,
                result="success",
                blocks_behind=0,
            )
            raw = _build_client_query(f"qid-{i:03d}-JFK", [a])
            messages.append(_fake_kafka_message(base_ts_ms + i * 1000, raw))

        resolution_consumer = _make_resolution_consumer("gateway_queries", [0])
        count_consumer = _make_partition_consumer([messages, [], [], []])
        sample_consumer = _make_partition_consumer([messages, [], [], []])

        with (
            patch(
                "confluent_kafka.Consumer",
                side_effect=[resolution_consumer, count_consumer, sample_consumer],
            ),
            patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
        ):
            result = provider.fetch_combined_query_results(start_date, num_days, rows_to_use=3)

        # Assert — capped at 3
        assert len(result) == 3, f"Expected 3 rows (cap), got {len(result)}"

    def test_second_call_uses_count_cache(self):
        """The second fetch_initial call must not trigger a second count pass."""
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1

        # Seed the count cache directly
        provider._count_cache_start_date = start_date
        provider._count_cache_num_days = num_days
        provider._count_cache = {(_bytes_to_cid(bytes(32)), _bytes_to_hex(INDEXER_BYTES)): 10}
        provider._fees_per_indexer = {_bytes_to_hex(INDEXER_BYTES): 0.01}

        with patch.object(provider, "_count_pass") as mock_count:
            result = provider.fetch_initial_query_results(start_date, num_days)

        mock_count.assert_not_called()
        assert len(result) == 1

    def test_second_call_uses_row_cache(self):
        """The second fetch_combined call must not trigger a second sample pass."""
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1
        rows_to_use = 100

        # Seed both caches
        provider._count_cache_start_date = start_date
        provider._count_cache_num_days = num_days
        provider._count_cache = {(_bytes_to_cid(bytes(32)), _bytes_to_hex(INDEXER_BYTES)): 10}
        provider._fees_per_indexer = {}

        provider._row_cache_start_date = start_date
        provider._row_cache_num_days = num_days
        provider._row_cache_rows_to_use = rows_to_use
        provider._row_cache_df = pd.DataFrame(
            [
                {
                    "query_id": "x",
                    "deployment_hash": _bytes_to_cid(bytes(32)),
                    "fee": 0.001,
                    "timestamp": pd.Timestamp("2024-01-01", tz="UTC"),
                    "blocks_behind": 0,
                    "response_time_ms": 100,
                    "indexer": _bytes_to_hex(INDEXER_BYTES),
                    "status": "200 OK",
                    "day_partition": date(2024, 1, 1),
                    "subgraph_network": "mainnet",
                    "url": "https://indexer.example.com/",
                }
            ]
        )

        with (
            patch.object(provider, "_count_pass") as mock_count,
            patch.object(provider, "_sample_pass") as mock_sample,
        ):
            result = provider.fetch_combined_query_results(start_date, num_days, rows_to_use)

        mock_count.assert_not_called()
        mock_sample.assert_not_called()
        assert len(result) == 1

    def test_empty_partitions_produce_empty_caches(self):
        """When no valid partitions exist, caches should be empty."""
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1

        # Resolution consumer returns no valid offsets (offset = -1001)
        resolution_consumer = MagicMock()
        mock_topic_meta = MagicMock()
        mock_topic_meta.partitions = {0: MagicMock()}
        resolution_consumer.list_topics.return_value.topics = {"gateway_queries": mock_topic_meta}

        invalid_tp = MagicMock()
        invalid_tp.topic = "gateway_queries"
        invalid_tp.partition = 0
        invalid_tp.offset = -1001  # OFFSET_INVALID
        resolution_consumer.offsets_for_times.return_value = [invalid_tp]

        with patch("confluent_kafka.Consumer", side_effect=[resolution_consumer]):
            result = provider.fetch_initial_query_results(start_date, num_days)

        assert result.empty
        assert provider._count_cache == {}
        assert provider._fees_per_indexer == {}


# ---------------------------------------------------------------------------
# Parallel merge test
# ---------------------------------------------------------------------------


class TestParallelMerge:
    """Verify cross-partition merge correctness."""

    def _build_provider(self) -> RedpandaProvider:
        with patch.dict(
            os.environ,
            {
                "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
                "REDPANDA_TOPIC": "gateway_queries",
            },
        ):
            return RedpandaProvider()

    def test_two_partitions_merge_counts_and_cap(self):
        """
        Two partitions with overlapping pairs: merged counts sum correctly
        and merged reservoirs don't exceed rows_to_use.
        """
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1
        base_ts_ms = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)

        deployment = bytes(range(32))
        indexer1 = INDEXER_BYTES

        # 4 messages per partition for the same pair, 8 total
        def make_messages(offset_start):
            msgs = []
            for i in range(4):
                a = _build_indexer_attempt(
                    indexer=indexer1,
                    deployment=deployment,
                    indexed_chain="mainnet",
                    url="https://indexer1.example.com/",
                    fee_grt=0.001,
                    response_time_ms=50 + i,
                    result="success",
                    blocks_behind=0,
                )
                raw = _build_client_query(f"qid-{offset_start + i:03d}-JFK", [a])
                msgs.append(_fake_kafka_message(base_ts_ms + i * 1000, raw))
            return msgs

        p0_msgs = make_messages(0)
        p1_msgs = make_messages(100)

        resolution_consumer = _make_resolution_consumer("gateway_queries", [0, 1])
        count_p0 = _make_partition_consumer([p0_msgs, [], [], []])
        count_p1 = _make_partition_consumer([p1_msgs, [], [], []])
        sample_p0 = _make_partition_consumer([p0_msgs, [], [], []])
        sample_p1 = _make_partition_consumer([p1_msgs, [], [], []])

        with (
            patch(
                "confluent_kafka.Consumer",
                side_effect=[
                    resolution_consumer,
                    count_p0,
                    count_p1,
                    sample_p0,
                    sample_p1,
                ],
            ),
            patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
        ):
            # Count pass
            initial = provider.fetch_initial_query_results(start_date, num_days)

            # Verify merged count = 4 + 4 = 8
            assert len(initial) == 1
            assert initial.iloc[0]["num_rows"] == 8

            # Sample pass with cap = 3
            combined = provider.fetch_combined_query_results(start_date, num_days, rows_to_use=3)

        assert len(combined) == 3, f"Expected 3 rows (cap), got {len(combined)}"


# ---------------------------------------------------------------------------
# Field mapping: status column
# ---------------------------------------------------------------------------


class TestFieldMapping:
    def test_success_result_produces_200_ok_status(self):
        attempt = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=DEPLOYMENT_BYTES,
            indexed_chain="mainnet",
            url="https://indexer.example.com/",
            fee_grt=0.001,
            response_time_ms=100,
            result="success",
            blocks_behind=0,
        )
        status = _map_result_to_status(attempt.result)
        assert status == "200 OK"

    def test_non_success_result_passes_through(self):
        attempt = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=DEPLOYMENT_BYTES,
            indexed_chain="mainnet",
            url="https://indexer.example.com/",
            fee_grt=0.001,
            response_time_ms=100,
            result="Unavailable(MissingBlock)",
            blocks_behind=0,
        )
        status = _map_result_to_status(attempt.result)
        assert status == "Unavailable(MissingBlock)"

    def test_indexer_hex_encoding(self):
        assert _bytes_to_hex(INDEXER_BYTES).startswith("0x")
        assert len(_bytes_to_hex(INDEXER_BYTES)) == 42

    def test_deployment_cid_encoding(self):
        cid = _bytes_to_cid(DEPLOYMENT_BYTES)
        assert cid.startswith("Qm")
        assert len(cid) == 46


# ---------------------------------------------------------------------------
# RedpandaProvider: fetch_stake_to_fees
# ---------------------------------------------------------------------------

INDEXER2_BYTES = bytes(20)  # 20 zero bytes
EXPECTED_INDEXER2 = _bytes_to_hex(INDEXER2_BYTES)


class TestFetchStakeToFees:
    """Tests for stake-to-fees ratio computed from subgraph stake + replay fees."""

    def _build_provider_with_fees(self, fees: Dict[str, float]) -> RedpandaProvider:
        """Build a RedpandaProvider with pre-populated fee cache."""
        with patch.dict(
            os.environ,
            {
                "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
                "GRAPH_NETWORK_SUBGRAPH_URL": "http://graph-node:8000/subgraphs/name/graph-network",
            },
        ):
            provider = RedpandaProvider()
        provider._fees_per_indexer = fees
        return provider

    def _mock_subgraph_response(self, indexers: List[dict]):
        """Build a mock requests.post that returns indexer data."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": {"indexers": indexers}}
        response.raise_for_status = MagicMock()
        return response

    def test_computes_ratio_from_stake_and_fees(self):
        """stake_to_fees = (stakedTokens - lockedTokens) / total_fees."""
        # Arrange
        provider = self._build_provider_with_fees(
            {
                EXPECTED_INDEXER: 100.0,  # earned 100 GRT in fees
                EXPECTED_INDEXER2: 50.0,  # earned 50 GRT in fees
            }
        )

        subgraph_indexers = [
            {"id": EXPECTED_INDEXER, "stakedTokens": "1000000", "lockedTokens": "0"},
            {"id": EXPECTED_INDEXER2, "stakedTokens": "500000", "lockedTokens": "100000"},
        ]

        # Act
        with patch(
            "redpanda.requests.post", return_value=self._mock_subgraph_response(subgraph_indexers)
        ):
            result = provider.fetch_stake_to_fees()

        # Assert
        # Indexer 1: (1000000 - 0) / 100 = 10000
        assert abs(result.loc[EXPECTED_INDEXER, "stake_to_fees"] - 10000.0) < 1e-6
        # Indexer 2: (500000 - 100000) / 50 = 8000
        assert abs(result.loc[EXPECTED_INDEXER2, "stake_to_fees"] - 8000.0) < 1e-6

    def test_zero_fees_produces_nan(self):
        """Indexers with zero fees should get NaN, not infinity."""
        # Arrange — indexer has stake but earned no fees during the window
        provider = self._build_provider_with_fees({})

        subgraph_indexers = [
            {"id": EXPECTED_INDEXER, "stakedTokens": "1000000", "lockedTokens": "0"},
        ]

        # Act
        with patch(
            "redpanda.requests.post", return_value=self._mock_subgraph_response(subgraph_indexers)
        ):
            result = provider.fetch_stake_to_fees()

        # Assert
        assert pd.isna(result.loc[EXPECTED_INDEXER, "stake_to_fees"])

    def test_missing_subgraph_url_returns_empty(self):
        """Without GRAPH_NETWORK_SUBGRAPH_URL, return empty DataFrame."""
        # Arrange
        with patch.dict(os.environ, {"REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092"}, clear=False):
            with patch.dict(os.environ, {"GRAPH_NETWORK_SUBGRAPH_URL": ""}, clear=False):
                provider = RedpandaProvider()

        # Act
        result = provider.fetch_stake_to_fees()

        # Assert
        assert result.empty
        assert "stake_to_fees" in result.columns

    def test_subgraph_indexer_not_in_replay_gets_nan(self):
        """Indexers in the subgraph but absent from the replay get NaN."""
        # Arrange — fee cache has no data for this indexer
        provider = self._build_provider_with_fees(
            {
                EXPECTED_INDEXER: 100.0,
            }
        )
        unknown_indexer = "0x" + "ff" * 20

        subgraph_indexers = [
            {"id": EXPECTED_INDEXER, "stakedTokens": "1000000", "lockedTokens": "0"},
            {"id": unknown_indexer, "stakedTokens": "500000", "lockedTokens": "0"},
        ]

        # Act
        with patch(
            "redpanda.requests.post", return_value=self._mock_subgraph_response(subgraph_indexers)
        ):
            result = provider.fetch_stake_to_fees()

        # Assert
        assert result.loc[EXPECTED_INDEXER, "stake_to_fees"] == 10000.0
        assert pd.isna(result.loc[unknown_indexer, "stake_to_fees"])

    def test_output_schema(self):
        """Output must be indexed by 'indexer' with a 'stake_to_fees' column."""
        # Arrange
        provider = self._build_provider_with_fees({EXPECTED_INDEXER: 50.0})

        subgraph_indexers = [
            {"id": EXPECTED_INDEXER, "stakedTokens": "1000", "lockedTokens": "0"},
        ]

        # Act
        with patch(
            "redpanda.requests.post", return_value=self._mock_subgraph_response(subgraph_indexers)
        ):
            result = provider.fetch_stake_to_fees()

        # Assert
        assert result.index.name == "indexer"
        expected_cols = [
            "stake_to_fees",
            "total_query_fees",
            "last_known_slashable_stake",
        ]
        assert list(result.columns) == expected_cols


class TestFeesAccumulatedDuringCountPass:
    """Verify that _fees_per_indexer is populated during the count pass."""

    def _build_provider(self) -> RedpandaProvider:
        with patch.dict(
            os.environ,
            {
                "REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092",
                "REDPANDA_TOPIC": "gateway_queries",
            },
        ):
            return RedpandaProvider()

    def test_fees_accumulated_across_messages(self):
        """Total fees per indexer should sum across all consumed attempts."""
        # Arrange
        provider = self._build_provider()
        start_date = date(2024, 1, 1)
        num_days = 1
        base_ts_ms = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)

        deployment = bytes(range(32))

        # Three attempts: indexer1 earns 0.001 + 0.003 = 0.004, indexer2 earns 0.002
        a1 = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=deployment,
            indexed_chain="mainnet",
            url="https://i1.example.com/",
            fee_grt=0.001,
            response_time_ms=50,
            result="success",
            blocks_behind=0,
        )
        a2 = _build_indexer_attempt(
            indexer=INDEXER2_BYTES,
            deployment=deployment,
            indexed_chain="mainnet",
            url="https://i2.example.com/",
            fee_grt=0.002,
            response_time_ms=80,
            result="success",
            blocks_behind=0,
        )
        a3 = _build_indexer_attempt(
            indexer=INDEXER_BYTES,
            deployment=deployment,
            indexed_chain="mainnet",
            url="https://i1.example.com/",
            fee_grt=0.003,
            response_time_ms=60,
            result="success",
            blocks_behind=0,
        )

        msg1 = _fake_kafka_message(base_ts_ms, _build_client_query("q1-JFK", [a1, a2]))
        msg2 = _fake_kafka_message(base_ts_ms + 1000, _build_client_query("q2-JFK", [a3]))

        resolution_consumer = _make_resolution_consumer("gateway_queries", [0])
        count_consumer = _make_partition_consumer([[msg1, msg2], [], [], []])

        # Act
        with (
            patch(
                "confluent_kafka.Consumer",
                side_effect=[resolution_consumer, count_consumer],
            ),
            patch("redpanda.ProcessPoolExecutor", _InlineExecutor),
        ):
            provider.fetch_initial_query_results(start_date, num_days)

        # Assert
        assert abs(provider._fees_per_indexer[EXPECTED_INDEXER] - 0.004) < 1e-9
        assert abs(provider._fees_per_indexer[EXPECTED_INDEXER2] - 0.002) < 1e-9
