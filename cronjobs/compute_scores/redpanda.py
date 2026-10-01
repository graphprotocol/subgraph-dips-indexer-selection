"""
RedpandaProvider: Redpanda-backed data source for the score computation CronJob.

A daily batch replay consumes the gateway_queries topic for the 28-day
regression window using a two-pass architecture:

  Pass 1 (count): lightweight scan counting (deployment, indexer) pairs and
  accumulating fees.

  Pass 2 (sample): reservoir sampling with cap = rows_to_use (from adjust_rows).
  Uses raw byte keys and deferred string conversion.

Both passes consume partitions in parallel via ProcessPoolExecutor to bypass
the GIL during CPU-bound protobuf parsing. Partition offsets resolved in
pass 1 are cached for reuse in pass 2.

Stake data is fetched via GraphQL from the Graph Network subgraph.
Computed scores are POSTed directly to the iisa HTTP service via iisa_client;
there is no shared filesystem between the cronjob and the iisa service.
"""

import json
import logging
import os
import random
import sys
import time
from collections import defaultdict
from collections.abc import Generator, Sized
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple, cast

import base58
import numpy as np
import pandas as pd
import requests
from gateway_queries_pb2 import ClientQueryProtobuf  # type: ignore[attr-defined]
from iisa_client import (
    IISAPushError,
    get_push_token,
    get_scores_status,
    post_scores,
)
from subgraph import paginate_subgraph_query, redact_url
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Interval for partition worker progress heartbeats. Workers run in child
# processes during pass 1 (count) and pass 2 (sample); both passes are
# otherwise silent until complete, which makes minutes-long runs indistinguishable
# from a stuck worker. Override via the PROGRESS_LOG_INTERVAL_SEC env var
# (e.g. set to a small value for debug runs).
#
# Printing directly to stdout with flush=True is intentional. On Linux
# (fork-based ProcessPoolExecutor) children inherit logger handlers, but
# `print(..., flush=True)` avoids depending on the multiprocessing start
# method and guarantees an unbuffered line per partition regardless of how
# the parent's logging is configured.
PROGRESS_LOG_INTERVAL_SEC = int(os.environ.get("PROGRESS_LOG_INTERVAL_SEC", "120"))

# Cap parallel partition consumers to avoid unbounded thread/connection creation.
# The CronJob runs with 8 CPUs; one consumer per core saturates throughput
# without excessive Kafka connections on topics with many partitions.
MAX_PARTITION_WORKERS = int(os.environ.get("REDPANDA_MAX_WORKERS", "8"))

# Column order for sample-pass row tuples emitted by _sample_partition_worker.
# Rows are stored as tuples (not dicts) to cut per-row memory footprint: a
# populated 10-field dict is ~500B of container overhead, while an 8-tuple is
# ~112B. indexer/deployment bytes are NOT stored in the row — they come from
# the reservoir's outer key and are broadcast at DataFrame construction time.
_ROW_COLUMNS: Tuple[str, ...] = (
    "query_id",
    "fee",
    "ts_ms",
    "blocks_behind",
    "response_time_ms",
    "status",
    "subgraph_network",
    "url",
)


# ---------------------------------------------------------------------------
# Encoding helpers
# ---------------------------------------------------------------------------


def _bytes_to_cid(b: bytes) -> str:
    """Encode a 32-byte deployment hash as a CIDv0 base58 string (Qm...)."""
    if len(b) != 32:
        return ""
    multihash = b"\x12\x20" + b  # sha2-256 function code + 32-byte length prefix
    return cast(str, base58.b58encode(multihash).decode("ascii"))


def _bytes_to_hex(b: bytes) -> str:
    """Encode a 20-byte Ethereum address as a lowercase 0x-prefixed hex string."""
    if len(b) != 20:
        return ""
    return "0x" + b.hex().lower()


def _map_result_to_status(result: str) -> str:
    """Map a protobuf result value to the status string processing.py expects."""
    return "200 OK" if result == "success" else result


# ---------------------------------------------------------------------------
# Module-level partition workers (must be picklable for ProcessPoolExecutor)
# ---------------------------------------------------------------------------


def _format_eta(seconds: float) -> str:
    """Render an ETA in seconds as a compact `<1m` / `Nm` / `NhMMm` string."""
    if seconds < 60:
        return "<1m"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, rem = divmod(minutes, 60)
    return f"{hours}h{rem:02d}m"


def _format_progress(start_off: int, cur_off: int, end_off: int, elapsed_sec: float) -> str:
    """Render a percentage-complete and ETA suffix for a partition worker.

    Returns one of:
      - "100% (done)" when the offset span is empty (partition had no data).
      - "N% (ETA unknown)" at startup, before any messages have been consumed.
      - "N% (ETA <compact>)" during steady-state consumption.
    """
    span = end_off - start_off
    consumed = cur_off - start_off
    if span <= 0:
        return "100% (done)"
    pct = max(0, min(100, int(consumed * 100 / span)))
    if elapsed_sec <= 0 or consumed <= 0:
        return f"{pct}% (ETA unknown)"
    rate = consumed / elapsed_sec
    remaining = max(0, end_off - cur_off)
    eta_sec = remaining / rate if rate > 0 else 0.0
    return f"{pct}% (ETA {_format_eta(eta_sec)})"


def _emit_heartbeat(
    label: str,
    partition: int,
    total: int,
    filtered: int,
    pairs: int,
    start_off: int,
    cur_off: int,
    end_off: int,
    elapsed_sec: float,
) -> None:
    """Emit a worker progress heartbeat line with a full ISO-8601 UTC timestamp.

    Prints directly to stdout for the reasons documented on
    PROGRESS_LOG_INTERVAL_SEC. Used for both the startup line (so a stuck
    worker is distinguishable from one that never entered the loop) and the
    periodic in-loop heartbeat.

    The trailing `N% (ETA ...)` suffix is computed from the partition's
    offset range resolved once at pass start: `start_off` is the offset at
    the window boundary, `end_off` is the broker high watermark at that
    moment, and `cur_off` is the offset of the last message the worker has
    processed so far.
    """
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    progress = _format_progress(start_off, cur_off, end_off, elapsed_sec)
    print(
        f"[{ts} {label} p{partition}] {total:,} msgs ({filtered:,} filtered), "
        f"{pairs} pairs, {progress}",
        flush=True,
    )


@dataclass
class _PartitionReadStats:
    """Message and filtered counts for one partition read, set when the read ends."""

    messages: int = 0
    filtered: int = 0


def _iter_partition_queries(
    label: str,
    topic: str,
    partition: int,
    start_offset: int,
    end_ts_ms: int,
    config: dict,
    gw_filter: Optional[set],
    end_offset: int,
    pairs: Sized,
    stats: _PartitionReadStats,
) -> Generator[Tuple[ClientQueryProtobuf, int], None, None]:
    """Read one partition from start_offset, yielding (query, ts_ms) per parsed message.

    Ends after 3 empty consume() calls in a row or at the first message past end_ts_ms.
    Skips error messages, unparseable payloads and gateways outside gw_filter. Heartbeats
    report len(pairs); the message and filtered counts go to `stats` when the read ends.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(config)
    total_messages = 0
    filtered_count = 0
    consecutive_empty = 0
    last_offset = start_offset

    try:
        consumer.assign([TopicPartition(topic, partition, start_offset)])

        # Startup heartbeat: confirms the worker entered the loop before the
        # first 30s consume() call, so a broker connection problem can't
        # masquerade as an unstarted worker.
        loop_start = time.monotonic()
        _emit_heartbeat(label, partition, 0, 0, 0, start_offset, start_offset, end_offset, 0.0)
        last_progress_log = loop_start

        while True:
            now = time.monotonic()
            if now - last_progress_log >= PROGRESS_LOG_INTERVAL_SEC:
                _emit_heartbeat(
                    label,
                    partition,
                    total_messages,
                    filtered_count,
                    len(pairs),
                    start_offset,
                    last_offset,
                    end_offset,
                    now - loop_start,
                )
                last_progress_log = now

            messages = consumer.consume(num_messages=1000, timeout=30.0)

            if not messages:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                continue

            consecutive_empty = 0

            for msg in messages:
                if msg.error():
                    continue

                _, ts_ms = msg.timestamp()
                if ts_ms < 0:
                    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                if ts_ms > end_ts_ms:
                    return

                # offset() is None only on error events, which are skipped above.
                last_offset = msg.offset()  # type: ignore[assignment]
                total_messages += 1

                try:
                    query = ClientQueryProtobuf()
                    query.ParseFromString(msg.value())
                except Exception:
                    continue

                if gw_filter and query.gateway_id not in gw_filter:
                    filtered_count += 1
                    continue

                yield query, ts_ms
    finally:
        stats.messages = total_messages
        stats.filtered = filtered_count
        consumer.close()


def _count_partition_worker(args: tuple) -> tuple:
    """
    Count pass for a single partition. Runs in a child process.

    Args is a tuple: (topic, partition, offset, end_ts_ms,
                       consumer_config, gateway_id_filter, end_offset)
    `offset` is the start offset at the window boundary; `end_offset` is
    the broker high watermark captured at pass start, used only for the
    progress/ETA suffix in heartbeats.

    Returns: (counts, fees, message_count, filtered_count)
    """
    topic, partition, start_offset, end_ts_ms, config, gw_filter, end_offset = args

    counts: Dict[Tuple[bytes, bytes], int] = defaultdict(int)
    fees: Dict[bytes, float] = defaultdict(float)
    stats = _PartitionReadStats()

    queries = _iter_partition_queries(
        "count",
        topic,
        partition,
        start_offset,
        end_ts_ms,
        config,
        gw_filter,
        end_offset,
        pairs=counts,
        stats=stats,
    )
    with closing(queries):
        for query, _ts_ms in queries:
            for attempt in query.indexer_queries:
                idx_bytes = bytes(attempt.indexer)
                dep_bytes = bytes(attempt.deployment)
                if len(dep_bytes) == 32 and len(idx_bytes) == 20:
                    counts[(dep_bytes, idx_bytes)] += 1
                    fees[idx_bytes] += attempt.fee_grt

    return (counts, fees, stats.messages, stats.filtered)


def _with_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else url + "/"


class _InternCache(dict[str, str]):
    """Maps a raw string to the interned form of canonical(raw), computing it on first sight.

    A hit is a plain dict subscript that never calls back into Python; only a miss runs
    __missing__, which interns, stores and returns the canonical form.
    """

    def __init__(self, canonical: Callable[[str], str]) -> None:
        super().__init__()
        self._canonical = canonical

    def __missing__(self, raw: str) -> str:
        interned = self[raw] = sys.intern(self._canonical(raw))
        return interned


def _sample_partition_worker(args: tuple) -> tuple:
    """
    Sample pass for a single partition. Runs in a child process.

    Args is a tuple: (topic, partition, offset, end_ts_ms,
                       consumer_config, gateway_id_filter, rows_to_use,
                       seed, end_offset)
    `offset` is the start offset; `end_offset` is the broker high watermark
    captured at pass start, used only for the progress/ETA suffix in
    heartbeats.

    Returns: (reservoirs, counts, filtered_count)
    """
    topic, partition, start_offset, end_ts_ms, config, gw_filter, rows_to_use, seed, end_offset = (
        args
    )

    rng = random.Random(seed + partition)
    reservoirs: Dict[Tuple[bytes, bytes], List[tuple]] = defaultdict(list)
    counts: Dict[Tuple[bytes, bytes], int] = defaultdict(int)
    stats = _PartitionReadStats()

    # Protobuf returns a fresh str per message even for repeated values, so each value is
    # interned once on first sight and every later row is a dict hit (2.76B attempts in
    # production against ~10 statuses, ~10 chains and ~13k urls).
    url_cache = _InternCache(_with_trailing_slash)
    status_cache = _InternCache(_map_result_to_status)
    chain_cache = _InternCache(str)

    queries = _iter_partition_queries(
        "sample",
        topic,
        partition,
        start_offset,
        end_ts_ms,
        config,
        gw_filter,
        end_offset,
        pairs=reservoirs,
        stats=stats,
    )
    with closing(queries):
        for query, ts_ms in queries:
            for attempt in query.indexer_queries:
                idx_bytes = bytes(attempt.indexer)
                dep_bytes = bytes(attempt.deployment)
                url = attempt.url

                if len(dep_bytes) != 32 or len(idx_bytes) != 20 or not url:
                    continue

                key = (dep_bytes, idx_bytes)
                n = counts[key]

                # query_id is unique per query, so interning it would cost cycles for no saving.
                row = (
                    query.query_id,
                    attempt.fee_grt,
                    ts_ms,
                    attempt.blocks_behind,
                    attempt.response_time_ms,
                    status_cache[attempt.result],
                    chain_cache[attempt.indexed_chain],
                    url_cache[url],
                )

                if n < rows_to_use:
                    reservoirs[key].append(row)
                elif (j := rng.randint(0, n)) < rows_to_use:
                    reservoirs[key][j] = row

                counts[key] += 1

    return (reservoirs, counts, stats.filtered)


# ---------------------------------------------------------------------------
# RedpandaProvider
# ---------------------------------------------------------------------------


class RedpandaProvider:
    """
    Data provider for compute_all_scores.

    Implements the interface expected by compute_all_scores:
      fetch_initial_query_results, fetch_combined_query_results,
      fetch_stake_to_fees, write_scores, scores_exist_for_today.

    Uses a two-pass replay: pass 1 counts pairs and fees (cheap), pass 2
    reservoir-samples rows capped at rows_to_use (from adjust_rows).
    """

    def __init__(self) -> None:
        self._bootstrap_servers = os.environ.get("REDPANDA_BOOTSTRAP_SERVERS", "")
        self._topic = os.environ.get("REDPANDA_TOPIC", "gateway_queries")
        self.graph_network_url = os.environ.get("GRAPH_NETWORK_SUBGRAPH_URL", "")
        self._iisa_api_url = os.environ.get("IISA_API_URL", "")
        self._iisa_push_token = get_push_token()

        # SASL authentication (optional — omit for plaintext local-network)
        self._sasl_username = os.environ.get("REDPANDA_SASL_USERNAME", "")
        self._sasl_password = os.environ.get("REDPANDA_SASL_PASSWORD", "")

        # Gateway ID filter (optional — defense in depth for mixed topics).
        # When set, only messages from the specified gateway(s) are processed.
        # Comma-separated, e.g. "mainnet-gw-1,mainnet-gw-2".
        _gw_ids = os.environ.get("REDPANDA_GATEWAY_IDS", "")
        self._gateway_id_filter: Optional[set] = (
            set(gid.strip() for gid in _gw_ids.split(",") if gid.strip()) or None
        )
        if self._gateway_id_filter:
            logger.info("Gateway ID filter active: %s", self._gateway_id_filter)
        else:
            logger.info("No REDPANDA_GATEWAY_IDS set — processing all gateways")

        # Count cache — populated by _count_pass
        self._count_cache: Dict[Tuple[str, str], int] = {}
        self._fees_per_indexer: Dict[str, float] = {}
        self._count_cache_start_date: Optional[date] = None
        self._count_cache_num_days: Optional[int] = None

        # Row cache — populated by _sample_pass
        self._row_cache_df: Optional[pd.DataFrame] = None
        self._row_cache_start_date: Optional[date] = None
        self._row_cache_num_days: Optional[int] = None
        self._row_cache_rows_to_use: Optional[int] = None

        # Partition offsets cached between passes
        self._cached_partitions: Optional[list] = None
        # High watermark per partition at the moment _resolve_partitions ran,
        # used only for the ETA/progress suffix in worker heartbeats.
        self._partition_ends: Dict[int, int] = {}

    def _consumer_config(self) -> dict:
        """Base librdkafka config shared by all consumers."""
        config = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": "iisa-score-computation-replay",
            "enable.auto.commit": False,
        }
        if self._sasl_username and self._sasl_password:
            config.update(
                {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "SCRAM-SHA-256",
                    "sasl.username": self._sasl_username,
                    "sasl.password": self._sasl_password,
                }
            )
        return config

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def fetch_initial_query_results(self, start_date: date, num_days: int) -> pd.DataFrame:
        """
        Return row counts per (deployment_hash, indexer) pair.

        Output schema: columns [deployment_hash, indexer, num_rows].
        """
        self._ensure_count_cache(start_date, num_days)

        rows = [
            {"deployment_hash": dep, "indexer": idx, "num_rows": count}
            for (dep, idx), count in self._count_cache.items()
        ]
        df = pd.DataFrame(rows, columns=["deployment_hash", "indexer", "num_rows"])
        if not df.empty:
            df.sort_values(by="num_rows", ascending=False, inplace=True, ignore_index=True)

        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info("Initial query results from Redpanda: %d pairs (%.1f MB)", len(df), memory_mb)
        return df

    def fetch_combined_query_results(
        self, start_date: date, num_days: int, rows_to_use: int
    ) -> pd.DataFrame:
        """
        Return sampled query rows for regression analysis.

        Caps each (deployment_hash, indexer) pair to rows_to_use rows via
        Algorithm R reservoir sampling during pass 2. The groupby truncation
        is kept as a safety net.

        Output schema: columns [query_id, deployment_hash, fee, timestamp, blocks_behind,
                 response_time_ms, indexer, status, day_partition,
                 subgraph_network, url].
        """
        self._ensure_count_cache(start_date, num_days)
        self._ensure_row_cache(start_date, num_days, rows_to_use)

        if self._row_cache_df is None or self._row_cache_df.empty:
            logger.warning("Row cache is empty — no data in the specified Redpanda window")
            return _empty_combined_df()

        # Safety net: truncate any pair that exceeds rows_to_use after merge
        df = (
            self._row_cache_df.groupby(["deployment_hash", "indexer"])
            .head(rows_to_use)
            .reset_index(drop=True)
        )

        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info("Combined query results from Redpanda: %d rows (%.1f MB)", len(df), memory_mb)
        return df

    def fetch_stake_to_fees(self, start_ts: str) -> pd.DataFrame:
        """
        Compute stake-to-fees ratio by combining subgraph stake data with
        fee totals accumulated during the Redpanda replay.

        stake_to_fees = (staked_tokens - locked_tokens) / total_fees_earned

        Stake comes from The Graph Network subgraph (current on-chain state).
        Fees come from self._fees_per_indexer, populated during _count_pass
        by summing fee_grt across all indexer attempts in the 28-day window.

        Returns a DataFrame indexed by 'indexer' with a 'stake_to_fees' column,
        matching the schema expected by compute_all_scores.
        """
        if not self.graph_network_url:
            logger.warning(
                "GRAPH_NETWORK_SUBGRAPH_URL not set — returning empty stake data. "
                "stake_to_fees will be NaN for all indexers."
            )
            return pd.DataFrame(columns=["stake_to_fees"])

        logger.info("Fetching stake data from %s", redact_url(self.graph_network_url))
        try:
            indexers = self._paginate_graphql_indexers()
        except Exception:
            logger.exception("Failed to fetch stake data from subgraph")
            return pd.DataFrame(columns=["stake_to_fees"])

        if not indexers:
            logger.warning("Subgraph returned no indexer records")
            return pd.DataFrame(columns=["stake_to_fees"])

        df = pd.DataFrame(indexers)
        df = df.rename(columns={"id": "indexer"})
        df["last_known_slashable_stake"] = df["stakedTokens"].astype(float) - df[
            "lockedTokens"
        ].astype(float)

        # Map fee totals from the Redpanda replay onto subgraph indexers.
        df["total_query_fees"] = df["indexer"].map(self._fees_per_indexer).fillna(0.0)

        # stake_to_fees = slashable_stake / total_fees. Indexers with zero fees
        # get NaN (division by zero produces NULL / NaN).
        df["stake_to_fees"] = df["last_known_slashable_stake"] / df["total_query_fees"].replace(
            0.0, float("nan")
        )

        df = df[["indexer", "stake_to_fees", "total_query_fees", "last_known_slashable_stake"]]
        df.set_index("indexer", inplace=True)

        matched = df["stake_to_fees"].notna().sum()
        logger.info(
            "Computed stake-to-fees for %d indexers (%d with fee data from replay)",
            len(df),
            matched,
        )
        return df

    def write_scores(self, scores_df: pd.DataFrame) -> None:
        """
        POST the scores DataFrame to the iisa HTTP service.

        iisa is the authoritative consumer; it persists the payload to its
        own cache PVC atomically and updates the in-memory DataFrame used
        to serve /select-indexers. The cronjob no longer touches a shared
        filesystem.

        Raises IISAPushError if iisa rejects the push or all retries are
        exhausted — the caller should let the job fail rather than silently
        running with stale data.
        """
        payload_json = scores_df.to_json(orient="records", date_format="iso", date_unit="s")
        assert payload_json is not None
        payload: list[dict] = json.loads(payload_json)

        memory_mb = scores_df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(
            "Pushing %d scores (%.2f MB) to iisa at %s",
            len(scores_df),
            memory_mb,
            self._iisa_api_url,
        )

        post_scores(self._iisa_api_url, self._iisa_push_token, payload)

    def scores_exist_for_today(self) -> bool:
        """
        Return True if iisa reports today's scores are already loaded.

        Queries GET /scores/status on iisa for the last computed_at timestamp.
        On any network or auth failure, returns False so the caller proceeds
        with a fresh run — idempotency is best-effort and the daily cadence
        plus concurrencyPolicy: Forbid on the CronJob prevents wasteful races.
        """
        try:
            status = get_scores_status(self._iisa_api_url, self._iisa_push_token)
        except IISAPushError as e:
            logger.warning("Could not check scores status on iisa: %s", e)
            return False

        computed_at_str = status.get("computed_at") if isinstance(status, dict) else None
        if not computed_at_str:
            return False
        computed_at = pd.to_datetime(computed_at_str, utc=True, errors="coerce")
        if pd.isna(computed_at):
            logger.warning("Unparseable computed_at from iisa: %r", computed_at_str)
            return False
        return computed_at.date() == datetime.now(timezone.utc).date()

    # -----------------------------------------------------------------------
    # Internal: cache management
    # -----------------------------------------------------------------------

    def _ensure_count_cache(self, start_date: date, num_days: int) -> None:
        """Trigger a count pass only if the cache doesn't cover this window."""
        if self._count_cache_start_date == start_date and self._count_cache_num_days == num_days:
            return
        self._count_pass(start_date, num_days)

    def _ensure_row_cache(self, start_date: date, num_days: int, rows_to_use: int) -> None:
        """Trigger a sample pass only if the cache doesn't cover this window/cap."""
        if (
            self._row_cache_df is not None
            and self._row_cache_start_date == start_date
            and self._row_cache_num_days == num_days
            and self._row_cache_rows_to_use == rows_to_use
        ):
            return
        self._sample_pass(start_date, num_days, rows_to_use)

    # -----------------------------------------------------------------------
    # Internal: partition resolution
    # -----------------------------------------------------------------------

    def _resolve_partitions(self, start_ts_ms: int) -> list:
        """
        Resolve timestamp-based offsets for all partitions of self._topic.

        Creates a temporary consumer for metadata and offset resolution,
        stores result in self._cached_partitions for reuse in pass 2.

        Also captures the broker high watermark per partition into
        self._partition_ends so workers can report progress and ETA against
        a fixed target (messages arriving after pass start aren't counted,
        matching the window-end semantics already enforced by end_ts_ms).

        Both list_topics and offsets_for_times are synchronous broker calls
        with 30s timeouts — logging each stage makes a slow or unreachable
        broker observable in cronjob logs rather than surfacing as a silent
        ~60s gap before pass 1.
        """
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(self._consumer_config())
        try:
            logger.info("Resolving partitions: list_topics(%s)...", self._topic)
            t0 = time.monotonic()
            meta = consumer.list_topics(self._topic, timeout=30)
            logger.info("list_topics returned in %.2fs", time.monotonic() - t0)

            if self._topic not in meta.topics:
                raise RuntimeError(f"Topic '{self._topic}' not found in Redpanda")

            partition_ids = list(meta.topics[self._topic].partitions.keys())
            seek_tps = [TopicPartition(self._topic, pid, start_ts_ms) for pid in partition_ids]

            logger.info(
                "offsets_for_times across %d partitions (start_ts_ms=%d)...",
                len(partition_ids),
                start_ts_ms,
            )
            t0 = time.monotonic()
            resolved = consumer.offsets_for_times(seek_tps, timeout=30)
            logger.info("offsets_for_times returned in %.2fs", time.monotonic() - t0)

            valid = [
                TopicPartition(tp.topic, tp.partition, tp.offset)
                for tp in resolved
                if tp.offset >= 0
            ]

            # High watermarks for ETA reporting. get_watermark_offsets is a
            # cheap per-partition broker call. If it fails for a partition
            # we fall back to the start offset so span == 0, which yields
            # "100% (done)" in heartbeats rather than a division-by-zero —
            # the warning below is the signal that ETA data is missing.
            partition_ends: Dict[int, int] = {}
            for tp in valid:
                try:
                    _, hi = consumer.get_watermark_offsets(tp, timeout=10)
                except Exception as e:
                    logger.warning(
                        "get_watermark_offsets failed for p%d: %s — ETA will be unavailable",
                        tp.partition,
                        e,
                    )
                    hi = tp.offset
                partition_ends[tp.partition] = hi

            logger.info(
                "Resolved %d partitions with valid offsets (%d partitions had no data in window)",
                len(valid),
                len(resolved) - len(valid),
            )
        finally:
            consumer.close()

        self._cached_partitions = valid
        self._partition_ends = partition_ends
        return valid

    # -----------------------------------------------------------------------
    # Internal: steps shared by both passes
    # -----------------------------------------------------------------------

    @staticmethod
    def _pass_window(start_date: date) -> Tuple[datetime, int, int]:
        """Return (end_dt, start_ts_ms, end_ts_ms) for the window from start_date (UTC) to now."""
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.now(timezone.utc)
        start_ts_ms = int(start_dt.timestamp() * 1000)
        end_ts_ms = int(end_dt.timestamp() * 1000)
        return end_dt, start_ts_ms, end_ts_ms

    def _run_partition_workers(
        self,
        worker: Callable[[tuple], tuple],
        partitions: list,
        end_ts_ms: int,
        *extra_args: object,
    ) -> List[tuple]:
        """Run `worker` once per partition in a process pool, returning results in partition order.

        Each worker gets (topic, partition, start offset, end_ts_ms, consumer config, gateway
        filter, *extra_args, end offset), where extra_args holds the pass's own settings.
        """
        # Consume partitions in parallel across processes to bypass the GIL.
        config = self._consumer_config()
        gw_filter = self._gateway_id_filter
        args = [
            (
                tp.topic,
                tp.partition,
                tp.offset,
                end_ts_ms,
                config,
                gw_filter,
                *extra_args,
                self._partition_ends.get(tp.partition, tp.offset),
            )
            for tp in partitions
        ]
        with ProcessPoolExecutor(max_workers=min(len(partitions), MAX_PARTITION_WORKERS)) as pool:
            return list(pool.map(worker, args))

    # -----------------------------------------------------------------------
    # Internal: Pass 1 — count
    # -----------------------------------------------------------------------

    def _count_pass(self, start_date: date, num_days: int) -> None:
        """
        Pass 1: lightweight scan counting (deployment, indexer) pairs.

        Uses extract_keys_and_fees for minimal parsing, raw byte keys,
        batch polling, and parallel partition consumption.
        """
        end_dt, start_ts_ms, end_ts_ms = self._pass_window(start_date)

        logger.info(
            "Starting count pass: topic=%s, window=%s to %s",
            self._topic,
            start_date,
            end_dt.isoformat(),
        )

        partitions = self._resolve_partitions(start_ts_ms)

        if not partitions:
            logger.warning(
                "No Redpanda messages found for the requested time window — start_ts_ms=%d",
                start_ts_ms,
            )
            self._count_cache = {}
            self._fees_per_indexer = {}
            self._count_cache_start_date = start_date
            self._count_cache_num_days = num_days
            return

        results = self._run_partition_workers(_count_partition_worker, partitions, end_ts_ms)

        # Merge per-partition counts and fees
        merged_counts: Dict[Tuple[bytes, bytes], int] = defaultdict(int)
        merged_fees: Dict[bytes, float] = defaultdict(float)
        total_messages = 0
        total_filtered = 0
        for counts, fees, msg_count, filtered in results:
            for key, val in counts.items():
                merged_counts[key] += val
            for idx_bytes, fee in fees.items():
                merged_fees[idx_bytes] += fee
            total_messages += msg_count
            total_filtered += filtered

        # Convert byte keys to string keys — only ~N_pairs conversions
        self._count_cache = {}
        for (dep_bytes, idx_bytes), count in merged_counts.items():
            dep_str = _bytes_to_cid(dep_bytes)
            idx_str = _bytes_to_hex(idx_bytes)
            if dep_str and idx_str:
                self._count_cache[(dep_str, idx_str)] = count

        self._fees_per_indexer = {}
        for idx_bytes, fee in merged_fees.items():
            idx_str = _bytes_to_hex(idx_bytes)
            if idx_str:
                self._fees_per_indexer[idx_str] = fee

        self._count_cache_start_date = start_date
        self._count_cache_num_days = num_days

        logger.info(
            "Count pass complete: %s messages (%s filtered by gateway ID), "
            "%s attempts across %d (deployment, indexer) pairs",
            f"{total_messages:,}",
            f"{total_filtered:,}",
            f"{sum(self._count_cache.values()):,}",
            len(self._count_cache),
        )

    # -----------------------------------------------------------------------
    # Internal: Pass 2 — sample
    # -----------------------------------------------------------------------

    def _sample_pass(self, start_date: date, num_days: int, rows_to_use: int) -> None:
        """
        Pass 2: reservoir sampling with cap = rows_to_use.

        Uses extract_sample_fields for selective parsing, raw byte keys,
        batch polling, cached partitions, deferred string conversion,
        parallel partition consumption, and process-local PRNG.
        """
        end_dt, start_ts_ms, end_ts_ms = self._pass_window(start_date)

        logger.info(
            "Starting sample pass: topic=%s, window=%s to %s, rows_to_use=%d",
            self._topic,
            start_date,
            end_dt.isoformat(),
            rows_to_use,
        )

        # Reuse cached partitions from pass 1 if available
        partitions = self._cached_partitions
        if partitions is None:
            partitions = self._resolve_partitions(start_ts_ms)

        if not partitions:
            logger.warning("No valid partitions for sample pass")
            self._row_cache_df = _empty_combined_df()
            self._row_cache_start_date = start_date
            self._row_cache_num_days = num_days
            self._row_cache_rows_to_use = rows_to_use
            return

        seed = int(os.environ.get("SCORING_SEED", start_date.strftime("%Y%m%d")))
        results = self._run_partition_workers(
            _sample_partition_worker, partitions, end_ts_ms, rows_to_use, seed
        )

        # Merge per-partition reservoirs using Algorithm R streaming.
        #
        # The previous implementation extended every worker's rows into a single
        # list per pair, then shuffled and truncated to rows_to_use. That held
        # up to N_workers * rows_to_use rows per pair in the parent process
        # before truncation — a transient ~8x spike that ran the 50Gi pod OOM
        # for windows with heavy message volume.
        #
        # The streaming form below absorbs each worker's rows one at a time and
        # applies Algorithm R at the parent, so the per-pair footprint is
        # bounded by rows_to_use from the first overflow onwards. Statistical
        # semantics are preserved: each retained row is uniformly sampled from
        # the union of all worker reservoirs, matching the old shuffle-then-head.
        merge_rng = random.Random(seed)
        merged_reservoirs: Dict[Tuple[bytes, bytes], List[tuple]] = defaultdict(list)
        merged_absorbed: Dict[Tuple[bytes, bytes], int] = defaultdict(int)
        total_filtered = 0
        # Pop from the list as we go so each worker's reservoir can be freed
        # once absorbed, rather than pinning all N worker results until the
        # loop completes.
        while results:
            reservoirs, _counts, filtered = results.pop()
            for key, rows in reservoirs.items():
                merged = merged_reservoirs[key]
                absorbed = merged_absorbed[key]
                for row in rows:
                    if absorbed < rows_to_use:
                        merged.append(row)
                    else:
                        j = merge_rng.randint(0, absorbed)
                        if j < rows_to_use:
                            merged[j] = row
                    absorbed += 1
                merged_absorbed[key] = absorbed
            total_filtered += filtered

        total_rows = sum(len(rows) for rows in merged_reservoirs.values())
        if total_rows:
            self._row_cache_df = _build_dataframe(merged_reservoirs)
        else:
            self._row_cache_df = _empty_combined_df()

        self._row_cache_start_date = start_date
        self._row_cache_num_days = num_days
        self._row_cache_rows_to_use = rows_to_use

        logger.info(
            "Sample pass complete: %s rows buffered across %d pairs "
            "(%s messages filtered by gateway ID)",
            f"{total_rows:,}",
            len(merged_reservoirs),
            f"{total_filtered:,}",
        )

    # -----------------------------------------------------------------------
    # Internal: GraphQL pagination
    # -----------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(
            (
                ConnectionError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError,
            )
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=60),
        reraise=True,
    )
    def _paginate_graphql_indexers(self) -> List[dict]:
        """Paginate through all indexers from the Graph Network subgraph.

        Uses cursor-based pagination (id_gt) instead of skip-based to avoid
        progressively slower queries as the offset grows.
        """
        query = """
        query($first: Int!, $lastId: String!) {
          indexers(first: $first, where: { id_gt: $lastId }, orderBy: id) {
            id
            stakedTokens
            lockedTokens
          }
        }
        """
        indexers: List[dict] = paginate_subgraph_query(
            self.graph_network_url, query, entity="indexers"
        )
        return indexers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_dataframe(
    reservoirs: Dict[Tuple[bytes, bytes], List[tuple]],
) -> pd.DataFrame:
    """
    Convert per-pair reservoirs of row tuples into the final DataFrame.

    The worker reservoirs are keyed by (deployment_bytes, indexer_bytes) and
    each value is a list of row tuples in _ROW_COLUMNS order — deployment and
    indexer bytes are NOT stored in the row to save memory, so we broadcast
    them from the outer key during frame construction.

    Memory-conscious construction: rather than materialise a flattened list
    of ~20M 10-tuples at prod scale (~2 GB of transient tuple-container
    overhead held simultaneously with the source reservoirs and the nascent
    DataFrame), we preallocate one numpy array per column sized to total_rows
    and fill per-pair. Numerics use typed dtypes (float64/int64) so they stay
    unboxed in the array instead of going through a list of Python int/float
    objects before pandas eventually converts them.

    Fill is done per-pair with `zip(*rows)` + slice assignment so the inner
    loop runs in C rather than Python. Per-pair transient is one `list(zip)`
    of pointers — ~rows_to_use * 8 pointers ≈ tens of KB per pair, freed
    before the next pair — negligible against the preallocated arrays.

    Performs deferred string conversions in bulk rather than per-message
    during streaming.
    """
    total_rows = sum(len(rows) for rows in reservoirs.values())

    deployment_bytes_arr = np.empty(total_rows, dtype=object)
    indexer_bytes_arr = np.empty(total_rows, dtype=object)
    query_id_arr = np.empty(total_rows, dtype=object)
    fee_arr = np.empty(total_rows, dtype=np.float64)
    ts_ms_arr = np.empty(total_rows, dtype=np.int64)
    blocks_behind_arr = np.empty(total_rows, dtype=np.int64)
    response_time_ms_arr = np.empty(total_rows, dtype=np.int64)
    status_arr = np.empty(total_rows, dtype=object)
    subgraph_network_arr = np.empty(total_rows, dtype=object)
    url_arr = np.empty(total_rows, dtype=object)

    i = 0
    for (dep_bytes, idx_bytes), rows in reservoirs.items():
        n = len(rows)
        if not n:
            continue
        cols = list(zip(*rows))
        deployment_bytes_arr[i : i + n] = dep_bytes
        indexer_bytes_arr[i : i + n] = idx_bytes
        query_id_arr[i : i + n] = cols[0]
        fee_arr[i : i + n] = cols[1]
        ts_ms_arr[i : i + n] = cols[2]
        blocks_behind_arr[i : i + n] = cols[3]
        response_time_ms_arr[i : i + n] = cols[4]
        status_arr[i : i + n] = cols[5]
        subgraph_network_arr[i : i + n] = cols[6]
        url_arr[i : i + n] = cols[7]
        i += n

    df = pd.DataFrame(
        {
            "deployment_bytes": deployment_bytes_arr,
            "indexer_bytes": indexer_bytes_arr,
            "query_id": query_id_arr,
            "fee": fee_arr,
            "ts_ms": ts_ms_arr,
            "blocks_behind": blocks_behind_arr,
            "response_time_ms": response_time_ms_arr,
            "status": status_arr,
            "subgraph_network": subgraph_network_arr,
            "url": url_arr,
        }
    )

    dep_map = {b: _bytes_to_cid(b) for b in df["deployment_bytes"].unique()}
    idx_map = {b: _bytes_to_hex(b) for b in df["indexer_bytes"].unique()}
    df["deployment_hash"] = df["deployment_bytes"].map(dep_map)
    df["indexer"] = df["indexer_bytes"].map(idx_map)
    df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["day_partition"] = df["timestamp"].dt.date

    df = df.drop(columns=["deployment_bytes", "indexer_bytes", "ts_ms"])
    return df


def _empty_combined_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "query_id",
            "deployment_hash",
            "fee",
            "timestamp",
            "blocks_behind",
            "response_time_ms",
            "indexer",
            "status",
            "day_partition",
            "subgraph_network",
            "url",
        ]
    )
