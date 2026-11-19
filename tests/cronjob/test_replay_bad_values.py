"""Tests for attempt values the replay cannot use: fees that are not finite, huge blocks_behind.

1 such value in a replayed message used to break scoring for the 28 days the message stays in
the window, so the replay now leaves the value out, counts it and logs the count.
"""

import logging
import math
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from sklearn.preprocessing import StandardScaler

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from gateway_queries_pb2 import ClientQueryProtobuf  # noqa: E402
from redpanda import (  # noqa: E402
    _build_dataframe,
    _count_partition_worker,
    _sample_partition_worker,
)

INDEXER = b"\x01" * 20
DEPLOYMENT = b"\x02" * 32
INT64_MAX = 2**63 - 1
NON_FINITE_FEES = [math.nan, math.inf, -math.inf]


def _message(query_id: str, fee: float, blocks_behind: int = 0) -> MagicMock:
    query = ClientQueryProtobuf()
    query.query_id = query_id
    attempt = query.indexer_queries.add()
    attempt.indexer = INDEXER
    attempt.deployment = DEPLOYMENT
    attempt.url = "https://indexer.example.com/"
    attempt.indexed_chain = "mainnet"
    attempt.fee_grt = fee
    attempt.response_time_ms = 100
    attempt.blocks_behind = blocks_behind
    attempt.result = "success"

    msg = MagicMock()
    msg.error.return_value = None
    msg.timestamp.return_value = (1, 1_000)
    msg.value.return_value = query.SerializeToString()
    msg.offset.return_value = 0
    return msg


def _count(messages: list) -> tuple:
    consumer = MagicMock()
    consumer.consume.side_effect = [messages, [], [], []]
    with patch("confluent_kafka.Consumer", return_value=consumer):
        return _count_partition_worker(
            ("gateway_queries", 3, 0, 999_999_999_999, {}, None, 1_000_000)
        )


def _sample(messages: list) -> tuple:
    consumer = MagicMock()
    consumer.consume.side_effect = [messages, [], [], []]
    with patch("confluent_kafka.Consumer", return_value=consumer):
        return _sample_partition_worker(
            ("gateway_queries", 3, 0, 999_999_999_999, {}, None, 100, 20260420, 1_000_000)
        )


def _warnings(caplog: pytest.LogCaptureFixture) -> list:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize("bad_fee", NON_FINITE_FEES)
def test_count_pass_leaves_a_fee_that_is_not_finite_out_of_the_total(bad_fee, caplog):
    messages = [_message("q1", 0.001), _message("q2", bad_fee), _message("q3", 0.003)]

    with caplog.at_level(logging.WARNING):
        counts, fees, _total, _filtered = _count(messages)

    assert fees[INDEXER] == pytest.approx(0.004)
    assert counts[(DEPLOYMENT, INDEXER)] == 3
    assert _warnings(caplog) == [
        "count p3: left 1 attempt(s) with a fee that is not a finite number out of the "
        "indexers' fee totals"
    ]


@pytest.mark.parametrize("bad_fee", NON_FINITE_FEES)
def test_sample_pass_keeps_the_attempt_with_its_fee_missing(bad_fee, caplog):
    messages = [_message("q1", 0.001), _message("q2", bad_fee), _message("q3", 0.003)]

    with caplog.at_level(logging.WARNING):
        reservoirs, _counts, _filtered = _sample(messages)
    df = _build_dataframe(reservoirs).set_index("query_id")

    assert sorted(df.index) == ["q1", "q2", "q3"]
    assert math.isnan(df.loc["q2", "fee"])
    assert df.loc["q3", "fee"] == pytest.approx(0.003)
    # The latency regression drops rows with a missing fee before its scaler, which an
    # infinite fee would make raise.
    StandardScaler().fit(df[["fee"]].dropna())
    assert _warnings(caplog) == [
        "sample p3: kept 1 attempt(s) with a fee that is not a finite number with the fee "
        "missing, which leaves them out of the latency regression"
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
        f"sample p3: clipped blocks_behind to {INT64_MAX:,} in 1 attempt(s) where it was larger"
    ]


def test_usable_values_log_no_warning(caplog):
    messages = [_message("q1", 0.0, blocks_behind=INT64_MAX), _message("q2", 0.002)]

    with caplog.at_level(logging.WARNING):
        _count(messages)
        reservoirs, _counts, _filtered = _sample(messages)

    assert _build_dataframe(reservoirs)["blocks_behind"].max() == INT64_MAX
    assert _warnings(caplog) == []
