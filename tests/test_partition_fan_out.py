"""Tests for RedpandaProvider._run_partition_workers, the fan-out shared by both Redpanda passes."""

import os
import sys
from concurrent.futures import Executor
from pathlib import Path
from unittest.mock import patch

from confluent_kafka import TopicPartition

# Make the cronjob package importable.
jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import redpanda  # noqa: E402
from redpanda import RedpandaProvider  # noqa: E402


class _RecordingExecutor(Executor):
    """Runs work inline and records the pool size it was created with."""

    max_workers = None

    def __init__(self, max_workers=None):
        _RecordingExecutor.max_workers = max_workers

    def map(self, fn, *iterables, **kwargs):
        return [fn(args) for args in iterables[0]]


def test_builds_one_argument_tuple_per_partition_and_caps_the_pool(monkeypatch):
    monkeypatch.setattr(redpanda, "MAX_PARTITION_WORKERS", 2)
    env = {"REDPANDA_BOOTSTRAP_SERVERS": "localhost:9092", "REDPANDA_GATEWAY_IDS": "gw-1"}
    with patch.dict(os.environ, env):
        provider = RedpandaProvider()
    provider._partition_ends = {0: 500, 1: 600}
    partitions = [TopicPartition("gateway_queries", pid, 100 + pid) for pid in (0, 1, 2)]

    with patch("redpanda.ProcessPoolExecutor", _RecordingExecutor):
        results = provider._run_partition_workers(lambda args: args, partitions, 9_999, 3, 42)

    config = provider._consumer_config()
    assert _RecordingExecutor.max_workers == 2
    assert results == [
        ("gateway_queries", 0, 100, 9_999, config, {"gw-1"}, 3, 42, 500),
        ("gateway_queries", 1, 101, 9_999, config, {"gw-1"}, 3, 42, 600),
        # With no recorded end offset, progress is reported against the start offset.
        ("gateway_queries", 2, 102, 9_999, config, {"gw-1"}, 3, 42, 102),
    ]
