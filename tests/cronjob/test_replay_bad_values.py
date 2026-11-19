"""Tests for attempt values the replay cannot use: negative or non-finite fees, huge blocks_behind.

1 such value in a replayed message used to break scoring for the 28 days the message stays in
the window, so the replay now leaves the value out, counts it and logs the count.
"""

import logging
import math
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from processing import _fit_latency_rankings  # noqa: E402
from redpanda import (  # noqa: E402
    _build_dataframe,
    _count_partition_worker,
    _sample_partition_worker,
)

INDEXER = b"\x01" * 20
DEPLOYMENT = b"\x02" * 32
INT64_MAX = 2**63 - 1
UNUSABLE_FEES = [math.nan, math.inf, -math.inf, -0.002]


def _add_attempt(
    query: ClientQueryProtobuf,
    indexer: bytes,
    deployment: bytes,
    fee: float,
    response_time_ms: int = 100,
    blocks_behind: int = 0,
) -> None:
    attempt = query.indexer_queries.add()
    attempt.indexer = indexer
    attempt.deployment = deployment
    attempt.url = "https://indexer.example.com/"
    attempt.indexed_chain = "mainnet"
    attempt.fee_grt = fee
    attempt.response_time_ms = response_time_ms
    attempt.blocks_behind = blocks_behind
    attempt.result = "success"


def _kafka_message(query: ClientQueryProtobuf) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    msg.timestamp.return_value = (1, 1_000)
    msg.value.return_value = query.SerializeToString()
    msg.offset.return_value = 0
    return msg


def _message(query_id: str, fee: float, blocks_behind: int = 0) -> MagicMock:
    query = ClientQueryProtobuf()
    query.query_id = query_id
    _add_attempt(query, INDEXER, DEPLOYMENT, fee, blocks_behind=blocks_behind)
    return _kafka_message(query)


def _count(messages: list) -> tuple:
    consumer = MagicMock()
    consumer.consume.side_effect = [messages, [], [], []]
    with patch("confluent_kafka.Consumer", return_value=consumer):
        return _count_partition_worker(
            ("gateway_queries", 3, 0, 999_999_999_999, {}, None, 1_000_000)
        )


def _sample(messages: list, rows_to_use: int = 100) -> tuple:
    consumer = MagicMock()
    consumer.consume.side_effect = [messages, [], [], []]
    with patch("confluent_kafka.Consumer", return_value=consumer):
        return _sample_partition_worker(
            ("gateway_queries", 3, 0, 999_999_999_999, {}, None, rows_to_use, 20260420, 1_000_000)
        )


def _warnings(caplog: pytest.LogCaptureFixture) -> list:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize("bad_fee", UNUSABLE_FEES)
def test_count_pass_leaves_an_unusable_fee_out_of_the_total(bad_fee, caplog):
    messages = [_message("q1", 0.001), _message("q2", bad_fee), _message("q3", 0.003)]

    with caplog.at_level(logging.WARNING):
        counts, fees, _total, _filtered = _count(messages)

    assert fees[INDEXER] == pytest.approx(0.004)
    assert counts[(DEPLOYMENT, INDEXER)] == 3
    assert _warnings(caplog) == [
        "count p3: left 1 attempt(s) with a fee that is negative or not a finite number out "
        "of the indexers' fee totals"
    ]


@pytest.mark.parametrize("bad_fee", UNUSABLE_FEES)
def test_sample_pass_keeps_the_attempt_with_its_fee_missing(bad_fee, caplog):
    messages = [_message("q1", 0.001), _message("q2", bad_fee), _message("q3", 0.003)]

    with caplog.at_level(logging.WARNING):
        reservoirs, _counts, _filtered = _sample(messages)
    df = _build_dataframe(reservoirs).set_index("query_id")

    assert sorted(df.index) == ["q1", "q2", "q3"]
    assert math.isnan(df.loc["q2", "fee"])
    assert df.loc["q3", "fee"] == pytest.approx(0.003)
    assert _warnings(caplog) == [
        "sample p3: read 1 attempt(s) with a fee that is negative or not a finite number; any "
        "the sample keeps have the fee missing, which leaves them out of the latency regression"
    ]


@pytest.mark.parametrize("blocks_behind", [INT64_MAX + 1, 2**64 - 1])
def test_sample_pass_clips_blocks_behind_beyond_the_int64_column(blocks_behind, caplog):
    messages = [_message("q1", 0.001, blocks_behind=5), _message("q2", 0.002, blocks_behind)]

    with caplog.at_level(logging.WARNING):
        reservoirs, _counts, _filtered = _sample(messages)
    df = _build_dataframe(reservoirs).set_index("query_id")

    assert df["blocks_behind"].dtype == np.int64
    assert df.loc["q1", "blocks_behind"] == 5
    assert df.loc["q2", "blocks_behind"] == INT64_MAX
    assert df.loc["q2", "fee"] == pytest.approx(0.002)
    assert _warnings(caplog) == [
        f"sample p3: read 1 attempt(s) with blocks_behind above {INT64_MAX:,}; any the sample "
        "keeps have it clipped to that"
    ]


def test_sample_pass_warnings_count_attempts_read_not_attempts_kept(caplog):
    messages = [_message(f"q{i}", math.inf, blocks_behind=2**64 - 1) for i in range(3)]

    with caplog.at_level(logging.WARNING):
        reservoirs, _counts, _filtered = _sample(messages, rows_to_use=1)

    assert len(_build_dataframe(reservoirs)) == 1
    assert [message.split(";")[0] for message in _warnings(caplog)] == [
        "sample p3: read 3 attempt(s) with a fee that is negative or not a finite number",
        f"sample p3: read 3 attempt(s) with blocks_behind above {INT64_MAX:,}",
    ]


def test_latency_regression_runs_on_a_replay_with_an_infinite_fee():
    # 2 deployments each queried 150 times across the same 3 indexers clears the regression's
    # minimums of 2 indexers per deployment and 250 queries per indexer and per deployment.
    rng = np.random.default_rng(7)
    indexers = [bytes([i + 1]) * 20 for i in range(3)]
    messages = []
    for d in range(2):
        for q in range(150):
            query = ClientQueryProtobuf()
            query.query_id = f"q-{d}-{q:03d}-JFK"
            for i, indexer in enumerate(indexers):
                fee = math.inf if (d, q, i) == (0, 0, 0) else float(rng.uniform(1e-6, 1e-2))
                latency = int(50 + 40 * i + rng.integers(0, 30))
                _add_attempt(query, indexer, bytes([d + 10]) * 32, fee, latency)
            messages.append(_kafka_message(query))
    reservoirs, _counts, _filtered = _sample(messages, rows_to_use=1_000)
    df = _build_dataframe(reservoirs)
    df["indexer_network"] = "arbitrum"
    for column, values in [("dst_lat", [40.7, 51.5, 35.7]), ("dst_lon", [-74.0, -0.1, 139.7])]:
        df[column] = df["indexer"].map(dict(zip(sorted(df["indexer"].unique()), values)))
    df["src_lat"] = rng.uniform(-60, 60, len(df))
    df["src_lon"] = rng.uniform(-180, 180, len(df))

    rankings, query_counts = _fit_latency_rankings(df, 450, date(2026, 11, 1), seed=1)

    assert sorted(rankings["indexer"]) == sorted(df["indexer"].unique())
    assert query_counts["query_count"].sum() == 3 * 300 - 1


def test_usable_values_log_no_warning(caplog):
    messages = [_message("q1", 0.0, blocks_behind=INT64_MAX), _message("q2", 0.002)]

    with caplog.at_level(logging.WARNING):
        _count(messages)
        reservoirs, _counts, _filtered = _sample(messages)

    assert _build_dataframe(reservoirs)["blocks_behind"].max() == INT64_MAX
    assert _warnings(caplog) == []
