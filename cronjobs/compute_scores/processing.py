"""
Processing logic for computing indexer scores.

This module contains functions extracted and adapted from the IISA codebase
for computing latency regression, uptime, success rate, and stake-to-fees metrics.
"""

import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import socket
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import airportsdata
import geoip2.database
import geoip2.errors
import numpy as np
import pandas as pd
from numpy.linalg import pinv
from scipy.stats import t
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from subgraph import paginate_subgraph_query

if TYPE_CHECKING:
    import aiohttp

logger = logging.getLogger(__name__)

# Constants
LATENCY_COEFFICIENT_STANDARD_ERROR_MULTIPLIER = 1.5
# Column names the latency regression uses for each indexer's coefficient and its
# standard error.
LATENCY_COEFFICIENT_COLUMN = "Latency Coefficient"
STANDARD_ERROR_COLUMN = "Standard Error"
REQUEST_STATUS_OK = "200 OK"
REQUEST_STATUS_UNAVAILABLE_MISSING_BLOCK = "Unavailable(MissingBlock)"

# GeoIP database paths (MaxMind GeoLite2, bundled in Docker image)
# Attribution: This product includes GeoLite2 data created by MaxMind, available from https://www.maxmind.com
GEOIP_CITY_DATABASE_PATH = os.environ.get("GEOIP_CITY_DATABASE_PATH", "/app/GeoLite2-City.mmdb")
GEOIP_ASN_DATABASE_PATH = os.environ.get("GEOIP_ASN_DATABASE_PATH", "/app/GeoLite2-ASN.mmdb")

# Global GeoIP readers (lazy initialized)
_geoip_city_reader: Optional[geoip2.database.Reader] = None
_geoip_asn_reader: Optional[geoip2.database.Reader] = None


def validate_geoip_databases() -> bool:
    """Validate that GeoIP databases exist and are readable.

    Returns True if databases are available, False otherwise.
    Logs warnings for missing or unreadable databases instead of raising.
    """
    errors = []

    # Check City database
    if not os.path.exists(GEOIP_CITY_DATABASE_PATH):
        errors.append(f"GeoLite2-City database not found at {GEOIP_CITY_DATABASE_PATH}")
    else:
        try:
            reader = geoip2.database.Reader(GEOIP_CITY_DATABASE_PATH)
            reader.close()
            logger.info(f"  [OK] GeoLite2-City database: {GEOIP_CITY_DATABASE_PATH}")
        except Exception as e:
            errors.append(f"GeoLite2-City database unreadable: {e}")

    # Check ASN database
    if not os.path.exists(GEOIP_ASN_DATABASE_PATH):
        errors.append(f"GeoLite2-ASN database not found at {GEOIP_ASN_DATABASE_PATH}")
    else:
        try:
            reader = geoip2.database.Reader(GEOIP_ASN_DATABASE_PATH)
            reader.close()
            logger.info(f"  [OK] GeoLite2-ASN database: {GEOIP_ASN_DATABASE_PATH}")
        except Exception as e:
            errors.append(f"GeoLite2-ASN database unreadable: {e}")

    if errors:
        for error in errors:
            logger.warning(f"GeoIP validation: {error}")
        return False

    logger.info("GeoIP database validation passed")
    return True


def get_geoip_city_reader() -> geoip2.database.Reader:
    """Get or create the GeoIP City database reader."""
    global _geoip_city_reader
    if _geoip_city_reader is None:
        if not os.path.exists(GEOIP_CITY_DATABASE_PATH):
            raise FileNotFoundError(
                f"GeoIP City database not found at {GEOIP_CITY_DATABASE_PATH}. "
                "Ensure GeoLite2-City.mmdb is bundled in the Docker image."
            )
        _geoip_city_reader = geoip2.database.Reader(GEOIP_CITY_DATABASE_PATH)
        logger.info(f"Loaded GeoIP City database from {GEOIP_CITY_DATABASE_PATH}")
    return _geoip_city_reader


def get_geoip_asn_reader() -> geoip2.database.Reader:
    """Get or create the GeoIP ASN database reader."""
    global _geoip_asn_reader
    if _geoip_asn_reader is None:
        if not os.path.exists(GEOIP_ASN_DATABASE_PATH):
            raise FileNotFoundError(
                f"GeoIP ASN database not found at {GEOIP_ASN_DATABASE_PATH}. "
                "Ensure GeoLite2-ASN.mmdb is bundled in the Docker image."
            )
        _geoip_asn_reader = geoip2.database.Reader(GEOIP_ASN_DATABASE_PATH)
        logger.info(f"Loaded GeoIP ASN database from {GEOIP_ASN_DATABASE_PATH}")
    return _geoip_asn_reader


# Iterative filter constants
ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS = 2
ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER = 1
ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER = 250
ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT = 250


# Column rename mappings for geolocation data
def _geoip_column_mapping(prefix: str) -> dict:
    return {
        "country": f"{prefix}_country",
        "latitude": f"{prefix}_lat",
        "longitude": f"{prefix}_lon",
    }


GEOIP_DST_COLUMN_MAPPING = _geoip_column_mapping("dst")
GEOIP_SRC_COLUMN_MAPPING = _geoip_column_mapping("src")


def _empty_geoip_result(ip_addr: Optional[str] = None) -> dict:
    """Return a GeoIP result dict with optional ip_addr and None for other fields."""
    return {"ip_addr": ip_addr, "org": None, "country": None, "latitude": None, "longitude": None}


DIPS_INFO_FETCH_TIMEOUT = int(os.environ.get("DIPS_INFO_FETCH_TIMEOUT", "10"))
DIPS_INFO_MAX_CONCURRENCY = int(os.environ.get("DIPS_INFO_MAX_CONCURRENCY", "100"))
DIPS_INFO_MAX_RETRIES = int(os.environ.get("DIPS_INFO_MAX_RETRIES", "5"))
DIPS_INFO_RETRY_BACKOFF_MULTIPLIER = int(os.environ.get("DIPS_INFO_RETRY_BACKOFF_MULTIPLIER", "1"))
DIPS_INFO_RETRY_BACKOFF_MAX = int(os.environ.get("DIPS_INFO_RETRY_BACKOFF_MAX", "5"))

# Graph-node version fetch settings. Mirror the dips-info constants so a slow
# fleet of /status endpoints can be tuned independently if it ever becomes a
# bottleneck.
GRAPH_NODE_VERSION_FETCH_TIMEOUT = int(os.environ.get("GRAPH_NODE_VERSION_FETCH_TIMEOUT", "10"))
GRAPH_NODE_VERSION_MAX_CONCURRENCY = int(
    os.environ.get("GRAPH_NODE_VERSION_MAX_CONCURRENCY", "100")
)
GRAPH_NODE_VERSION_MAX_RETRIES = int(os.environ.get("GRAPH_NODE_VERSION_MAX_RETRIES", "3"))
GRAPH_NODE_VERSION_RETRY_BACKOFF_MULTIPLIER = int(
    os.environ.get("GRAPH_NODE_VERSION_RETRY_BACKOFF_MULTIPLIER", "1")
)
GRAPH_NODE_VERSION_RETRY_BACKOFF_MAX = int(
    os.environ.get("GRAPH_NODE_VERSION_RETRY_BACKOFF_MAX", "5")
)
# Byte cap on the /status response: real payload is ~150 bytes, the cap
# protects against a malicious or misconfigured endpoint streaming a huge
# body and exhausting cron memory.
GRAPH_NODE_VERSION_MAX_RESPONSE_BYTES = int(
    os.environ.get("GRAPH_NODE_VERSION_MAX_RESPONSE_BYTES", str(64 * 1024))
)

# Falsy values that disable strict mode; anything else defaults to strict.
# Matches typical Helm/values.yaml toggle conventions; we lower-and-strip
# the env value before comparison so case and whitespace don't bite.
_MIN_GRAPH_NODE_VERSION_STRICT_FALSE_VALUES = frozenset({"false", "0", "no", "off"})


def _get_min_graph_node_version() -> str:
    """Operator policy: minimum graph-node version an indexer must be running
    to be eligible for DIPs selection. Empty string disables the filter.
    Read at call time so tests can drive the value via `monkeypatch.setenv`.
    """
    return os.environ.get("MIN_GRAPH_NODE_VERSION", "").strip()


def _get_min_graph_node_version_strict() -> bool:
    """Exclude indexers whose version is unknown — endpoint unreachable,
    malformed response, or missing the version field. Defaults to strict;
    set the env var to "false"/"0"/"no"/"off" to keep unknowns in the
    pool (the rollout-window posture).
    """
    raw = os.environ.get("MIN_GRAPH_NODE_VERSION_STRICT", "").strip().lower()
    return raw not in _MIN_GRAPH_NODE_VERSION_STRICT_FALSE_VALUES


def discover_indexers_from_network_subgraph(network_subgraph_url: str) -> Dict[str, str]:
    """Query the Graph Network subgraph for indexer addresses and service URLs.

    The subgraph is the source of truth — Redpanda only sees indexers the gateway
    has routed to, so newly registered ones are invisible without this lookup.
    Returns {address: url} for indexers with a URL, or {} on any failure.
    """
    if not network_subgraph_url:
        logger.warning("GRAPH_NETWORK_SUBGRAPH_URL not set, cannot discover indexers from subgraph")
        return {}

    query = """
    query($first: Int!, $lastId: String!) {
      indexers(first: $first, where: { id_gt: $lastId, url_not: "" }, orderBy: id) {
        id
        url
      }
    }
    """
    try:
        raw_indexers = paginate_subgraph_query(network_subgraph_url, query, entity="indexers")
    except Exception as e:
        logger.warning(f"Failed to query network subgraph: {e}")
        return {}

    all_indexers: Dict[str, str] = {}
    for indexer in raw_indexers:
        url = indexer.get("url", "")
        if url:
            all_indexers[indexer["id"]] = url

    logger.info(f"Discovered {len(all_indexers)} indexers from network subgraph")
    return all_indexers


def _extract_dips_prices(data: dict) -> tuple:
    """Pull the two min-price fields out of a /dips/info response.

    Handles legacy nested-under-``pricing`` and current flat-at-top shapes
    (the indexer fleet doesn't upgrade in lockstep, so a single pass hits
    both). Returns ``(min_prices_dict_or_empty, min_entity_price_or_none)``.
    """
    pricing = data.get("pricing")
    if isinstance(pricing, dict):
        min_prices = pricing.get("min_grt_per_30_days", {})
        min_entity_price = pricing.get("min_grt_per_billion_entities_per_30_days")
    else:
        min_prices = data.get("min_grt_per_30_days", {})
        min_entity_price = data.get("min_grt_per_billion_entities_per_30_days")
    return min_prices, min_entity_price


async def _fetch_single_dips_info_async(
    session: "aiohttp.ClientSession",
    indexer: str,
    url: str,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Fetch /dips/info from a single indexer URL with retry logic."""
    import aiohttp

    dips_url = url.rstrip("/") + "/dips/info"

    async def do_fetch() -> dict:
        async with semaphore:
            async with session.get(
                dips_url,
                timeout=aiohttp.ClientTimeout(total=DIPS_INFO_FETCH_TIMEOUT),
            ) as resp:
                if resp.status >= 500:
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=f"Server error {resp.status}",
                    )
                resp.raise_for_status()
                body: dict = await resp.json()
                return body

    last_error: Optional[Exception] = None
    for attempt in range(DIPS_INFO_MAX_RETRIES):
        try:
            data = await do_fetch()
            min_prices, min_entity_price = _extract_dips_prices(data)
            supported_networks = data.get("supported_networks", [])
            # If supported_networks not explicitly provided, infer from price keys
            if not supported_networks and isinstance(min_prices, dict):
                supported_networks = list(min_prices.keys())
            return {
                "indexer": indexer,
                "dips_info_available": True,
                "dips_min_grt_per_30_days": json.dumps(min_prices)
                if isinstance(min_prices, dict)
                else "{}",
                "dips_min_grt_per_billion_entities_per_30_days": str(min_entity_price)
                if min_entity_price is not None
                else None,
                "dips_supported_networks": json.dumps(supported_networks),
            }
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_error = e
            if attempt < DIPS_INFO_MAX_RETRIES - 1:
                delay = min(
                    DIPS_INFO_RETRY_BACKOFF_MAX,
                    DIPS_INFO_RETRY_BACKOFF_MULTIPLIER * (2**attempt),
                )
                await asyncio.sleep(delay)

    logger.debug(f"Failed to fetch /dips/info from {url} for indexer {indexer}: {last_error}")
    return {
        "indexer": indexer,
        "dips_info_available": False,
        "dips_min_grt_per_30_days": "{}",
        "dips_min_grt_per_billion_entities_per_30_days": None,
        "dips_supported_networks": "[]",
    }


async def _fetch_all_dips_info_async(indexer_urls: Dict[str, str]) -> List[dict]:
    """Fetch /dips/info from all indexers concurrently."""
    import aiohttp

    semaphore = asyncio.Semaphore(DIPS_INFO_MAX_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_single_dips_info_async(session, indexer, url, semaphore)
            for indexer, url in indexer_urls.items()
        ]
        return await asyncio.gather(*tasks)


def fetch_dips_info(indexer_urls: Dict[str, str]) -> pd.DataFrame:
    """Concurrently fetch /dips/info from each indexer (asyncio).

    `indexer_urls` maps indexer address -> URL. Returns columns: indexer,
    dips_info_available, dips_min_grt_per_30_days,
    dips_min_grt_per_billion_entities_per_30_days, dips_supported_networks.
    """
    if not indexer_urls:
        return pd.DataFrame(
            columns=[
                "indexer",
                "dips_info_available",
                "dips_min_grt_per_30_days",
                "dips_min_grt_per_billion_entities_per_30_days",
                "dips_supported_networks",
            ]
        )

    results = asyncio.run(_fetch_all_dips_info_async(indexer_urls))

    available_count = sum(1 for r in results if r["dips_info_available"])
    logger.info(f"DIP info fetched for {len(results)} indexers ({available_count} available)")

    return pd.DataFrame(results)


async def _fetch_single_graph_node_version_async(
    session: "aiohttp.ClientSession",
    indexer: str,
    url: str,
    semaphore: asyncio.Semaphore,
) -> dict:
    """POST `{ version { version commit } }` to <url>/status and extract the result.

    Returns strings on success, None on any failure (timeout, non-2xx,
    malformed, missing field). The caller decides how to treat None — see
    `filter_by_min_graph_node_version`.
    """
    import aiohttp

    status_url = url.rstrip("/") + "/status"
    payload = {"query": "{ version { version commit } }"}

    async def do_fetch() -> dict:
        async with semaphore:
            async with session.post(
                status_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=GRAPH_NODE_VERSION_FETCH_TIMEOUT),
            ) as resp:
                if resp.status >= 500:
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=f"Server error {resp.status}",
                    )
                resp.raise_for_status()
                # Explicit byte cap so a pathological indexer can't stream a
                # huge body and exhaust cron memory; reading N+1 lets us
                # detect overflow by comparing the returned length to N.
                raw = await resp.content.read(GRAPH_NODE_VERSION_MAX_RESPONSE_BYTES + 1)
                if len(raw) > GRAPH_NODE_VERSION_MAX_RESPONSE_BYTES:
                    raise aiohttp.ClientPayloadError(
                        f"Response exceeded {GRAPH_NODE_VERSION_MAX_RESPONSE_BYTES} bytes"
                    )
                body: dict = json.loads(raw)
                return body

    last_error: Optional[Exception] = None
    for attempt in range(GRAPH_NODE_VERSION_MAX_RETRIES):
        try:
            data = await do_fetch()
            # Defensive shape check around `{"data": {"version": {...}}}`:
            # forks may flatten `version`, proxies may return arrays or HTML.
            # Validating each level lets us return all-None instead of crashing.
            data_envelope = data.get("data") if isinstance(data, dict) else None
            version_obj = data_envelope.get("version") if isinstance(data_envelope, dict) else None
            if not isinstance(version_obj, dict):
                version_obj = {}
            return {
                "indexer": indexer,
                "graph_node_version": version_obj.get("version"),
                "graph_node_commit": version_obj.get("commit"),
            }
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            ValueError,
            UnicodeDecodeError,
            AttributeError,
        ) as e:
            # Catch tuple deliberately broad: a single misbehaving indexer
            # returning HTML / invalid UTF-8 / a malformed shape would
            # otherwise propagate through gather() and crash the whole run.
            last_error = e
            # 4xx is deterministic: the endpoint either doesn't exist or
            # rejects the query and won't change shape on retry. Skip the
            # remaining attempts so we don't waste backoff time on a wall.
            if isinstance(e, aiohttp.ClientResponseError) and 400 <= e.status < 500:
                break
            if attempt < GRAPH_NODE_VERSION_MAX_RETRIES - 1:
                delay = min(
                    GRAPH_NODE_VERSION_RETRY_BACKOFF_MAX,
                    GRAPH_NODE_VERSION_RETRY_BACKOFF_MULTIPLIER * (2**attempt),
                )
                await asyncio.sleep(delay)

    logger.debug(
        f"Failed to fetch graph-node version from {url} for indexer {indexer}: {last_error}"
    )
    return {"indexer": indexer, "graph_node_version": None, "graph_node_commit": None}


async def _fetch_all_graph_node_versions_async(
    indexer_urls: Dict[str, str],
) -> List[dict]:
    """Fan-out graph-node version fetches across the indexer fleet."""
    import aiohttp

    semaphore = asyncio.Semaphore(GRAPH_NODE_VERSION_MAX_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_single_graph_node_version_async(session, indexer, url, semaphore)
            for indexer, url in indexer_urls.items()
        ]
        return await asyncio.gather(*tasks)


def fetch_graph_node_versions(indexer_urls: Dict[str, str]) -> pd.DataFrame:
    """Concurrently fetch /status graph-node version from each indexer (asyncio).

    Returns columns: indexer, graph_node_version, graph_node_commit. Indexers
    whose /status was unreachable or malformed appear with None values; the
    version filter decides their fate.
    """
    if not indexer_urls:
        return pd.DataFrame(columns=["indexer", "graph_node_version", "graph_node_commit"])

    results = asyncio.run(_fetch_all_graph_node_versions_async(indexer_urls))

    # Aggregate counts so the cron log shows the breakdown at a glance.
    # The unparseable bucket separates git-describe-style strings
    # (e.g. `0.40.1-1-gabcdef`) from genuine "version below the bar".
    known_count = sum(1 for r in results if r["graph_node_version"] is not None)
    unknown_count = len(results) - known_count
    min_version = _get_min_graph_node_version()
    if min_version:
        counts = {"meets": 0, "below": 0, "unparseable": 0}
        for r in results:
            reported = r["graph_node_version"]
            if reported is None:
                continue
            counts[_classify_graph_node_version(reported, min_version)] += 1
        logger.info(
            f"Graph-node version probe: {len(results)} indexers — "
            f"{counts['meets']} meet {min_version}, {counts['below']} below, "
            f"{counts['unparseable']} unparseable, {unknown_count} unknown"
        )
    else:
        logger.info(
            f"Graph-node version probe: {len(results)} indexers — "
            f"{known_count} reported, {unknown_count} unknown"
        )

    return pd.DataFrame(results)


def fetch_and_filter_graph_node_versions(
    df: pd.DataFrame, indexer_urls: Dict[str, str]
) -> pd.DataFrame:
    """Attach version + commit columns and apply the configured minimum filter.

    Shared between full and degraded pipelines. With no MIN_GRAPH_NODE_VERSION
    or no indexers, neither probe nor filter runs but None-valued columns are
    still attached so downstream schema code can rely on their presence.
    """
    min_version = _get_min_graph_node_version()
    strict = _get_min_graph_node_version_strict()

    # Skip probe and filter when there's nothing to compare against (filter
    # disabled) or no indexers to probe (subgraph empty). Still attach the
    # version columns so downstream code carries them through unconditionally.
    if not min_version or not indexer_urls:
        if min_version and not indexer_urls:
            # Filter configured but the network subgraph returned no indexers
            # to probe. Logged at INFO so this case is distinguishable from a
            # deliberately-disabled run (which would emit nothing).
            logger.info(
                "Graph-node version filter skipped — no indexers from network "
                f"subgraph to probe (would otherwise apply min={min_version}, "
                f"strict={strict})"
            )
        df = df.copy()
        df["graph_node_version"] = None
        df["graph_node_commit"] = None
        return df

    versions_df = fetch_graph_node_versions(indexer_urls)

    if not versions_df.empty:
        # `validate="m:1"` enforces unique indexer keys in versions_df (true
        # by construction). If upstream ever widens to multiple URLs per
        # indexer, this raises rather than silently row-multiplying scores.
        df = pd.merge(df, versions_df, on="indexer", how="left", validate="m:1")
    else:
        df = df.copy()
        df["graph_node_version"] = None
        df["graph_node_commit"] = None

    return filter_by_min_graph_node_version(df, min_version, strict)


def _classify_graph_node_version(reported: Optional[str], minimum: str) -> str:
    """Classify reported graph-node version: ``meets`` / ``below`` / ``unparseable``.

    ``unparseable`` covers missing / non-string / non-PEP-440 (e.g. git-describe
    like ``0.40.1-1-gabcdef``). The probe-summary log uses this to keep
    "non-PEP-440 string" distinct from "parseable but too old".
    """
    # Reject anything that isn't a non-empty string up front. pandas can hand
    # us NaN (a float, which is truthy in Python's `not` check), and that
    # would otherwise reach Version() and explode with TypeError.
    if not isinstance(reported, str) or not reported or not minimum:
        return "unparseable"
    try:
        from packaging.version import InvalidVersion, Version

        return "meets" if Version(reported) >= Version(minimum) else "below"
    except InvalidVersion:
        return "unparseable"


def _meets_min_graph_node_version(reported: Optional[str], minimum: str) -> bool:
    """True iff both versions parse as PEP 440 and `reported` >= `minimum`.

    Any parsing failure (unknown / malformed) returns False so the caller
    treats the indexer as not-eligible in strict mode.
    """
    return _classify_graph_node_version(reported, minimum) == "meets"


def filter_by_min_graph_node_version(
    scores_df: pd.DataFrame,
    min_version: str,
    strict: bool,
) -> pd.DataFrame:
    """Drop indexers whose `graph_node_version` falls below `min_version`.

    No-op when `min_version` is empty. Indexers with no reported version
    (None / NaN) are dropped only when `strict` is true — fail-open lets
    the filter ship while the fleet rolls out /status version support.
    """
    if not min_version:
        return scores_df
    if "graph_node_version" not in scores_df.columns:
        logger.warning(
            "MIN_GRAPH_NODE_VERSION is set but graph_node_version column is missing; "
            "skipping filter"
        )
        return scores_df

    before = len(scores_df)
    reported = scores_df["graph_node_version"]
    # List comprehension instead of pandas `.map`: the helper already
    # returns False for NaN / non-string, so `.map` would only add a
    # fillna step and trigger the deprecated object-dtype downcast.
    meets = pd.Series(
        [_meets_min_graph_node_version(v, min_version) for v in reported],
        index=reported.index,
        dtype=bool,
    )

    # Two reasons a row can fall below the bar: missing version (unknown)
    # or known-and-too-old. Strict mode drops both; fail-open keeps the
    # unknowns and only drops the known-and-too-old.
    if strict:
        keep = meets
    else:
        keep = meets | reported.isna()

    dropped = scores_df.loc[~keep, ["indexer", "graph_node_version"]]
    # Per-row detail at DEBUG so a steady-state filter doesn't drown the
    # log in WARNING noise; the aggregate WARNING below carries the
    # signal that something was excluded.
    for row in dropped.itertuples(index=False):
        logger.debug(
            "Excluding indexer below graph-node minimum: "
            f"indexer={row.indexer} reported={row.graph_node_version} "
            f"minimum={min_version} strict={strict}"
        )

    after = before - len(dropped)
    if len(dropped) > 0:
        # Split dropped into "known but below the bar" vs "unknown / probe
        # didn't answer" so triage doesn't need DEBUG. Unknown is always
        # zero in fail-open mode (kept); in strict it's missing /status.
        is_unknown = reported.isna()
        below_count = int((~meets & ~is_unknown).sum())
        unknown_dropped = int(is_unknown.sum()) if strict else 0
        logger.warning(
            f"Graph-node version filter dropped {len(dropped)} of {before} indexers "
            f"({below_count} below, {unknown_dropped} unknown) "
            f"(min={min_version}, strict={strict}); enable DEBUG for per-indexer detail"
        )
    else:
        logger.info(
            f"Graph-node version filter: {after}/{before} indexers retained "
            f"(min={min_version}, strict={strict})"
        )
    return scores_df.loc[keep].reset_index(drop=True)


def diagnose_geoip_failure(combined_queries: pd.DataFrame) -> str:
    """Diagnostic suffix categorising why GeoIP failed for every indexer.

    Categories: ``unresolved`` (DNS gaierror), ``private`` (RFC 1918 / loopback
    / Docker bridge — the local-network case), ``public`` (resolved but missing
    from GeoLite2, i.e. stale database). Sample capped at 5 rows.
    """
    deduplicated = combined_queries[["indexer", "ip_addr"]].drop_duplicates(subset="indexer")
    sample = deduplicated.head(5)
    null_count = 0
    private_count = 0
    public_count = 0
    for ip in deduplicated["ip_addr"]:
        if pd.isna(ip) or not ip:
            null_count += 1
        elif is_private_ip(str(ip)):
            private_count += 1
        else:
            public_count += 1

    sample_rows = "\n".join(
        f"    {str(row.indexer)[:10]}...  ->  "
        f"{str(row.ip_addr) if not pd.isna(row.ip_addr) else '(no IP resolved)'}"
        for row in sample.itertuples(index=False)
    )

    if private_count >= max(null_count, public_count):
        diagnosis = (
            "indexer URLs resolve to private/internal IPs (Docker bridge "
            "networks, RFC 1918, or loopback ranges) that have no public "
            "geolocation. This is expected for local-network / Docker Compose "
            "setups; latency scoring requires public-internet indexer URLs."
        )
    elif null_count >= max(private_count, public_count):
        diagnosis = (
            "indexer URL hostnames failed to resolve to any IP (DNS lookup "
            "returned nothing). Check that the URLs in the network subgraph "
            "are reachable from this environment."
        )
    elif public_count > 0:
        diagnosis = (
            "indexer URLs resolved to public IPs, but none had entries in "
            "the GeoLite2-City database. The databases load successfully, "
            "but the IPs fall outside indexed ranges, or the databases are "
            "stale (older than MaxMind's 30-day refresh)."
        )
    else:
        diagnosis = "no IPs in the data to inspect; this is unexpected."

    total = len(deduplicated)
    return (
        f"\nDiagnosis: {diagnosis}\n"
        f"Counts across {total} unique indexer(s): "
        f"private={private_count}, public={public_count}, unresolved={null_count}\n"
        f"Sample (first {min(5, total)}):\n{sample_rows}"
    )


def default_scoring_seed(start_date: date) -> int:
    """Seed to use when the caller does not supply one: the window's start date as YYYYMMDD.

    This mirrors the convention the Redpanda provider uses for its own row sampling, so a
    run over a given window samples the same rows whether or not a seed was passed in.
    """
    return int(start_date.strftime("%Y%m%d"))


def _resolve_geoip_or_demote(combined_queries: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """Merge indexer and query geolocation into the queries; return (queries, geoip_available).

    When no indexer resolved to a public location (the normal local-network / Docker case,
    where every indexer sits on a bridge-network IP) the queries are returned untouched with
    geoip_available=False so the caller takes the partial path instead of a distance
    pipeline that would throw anyway.
    """
    indexers_df = resolve_indexer_geoip(combined_queries)

    if indexers_df["dst_lat"].notna().sum() == 0:
        combined_queries_with_indexers = merge_in_indexers_info(combined_queries, indexers_df)
        logger.warning(
            "GeoIP resolution succeeded for 0/%d indexers; demoting to "
            "partial mode (latency scores neutral, all other metrics "
            "from real query data).%s",
            len(indexers_df),
            diagnose_geoip_failure(combined_queries_with_indexers),
        )
        return combined_queries, False

    combined_queries = merge_in_indexers_info(combined_queries, indexers_df)
    return merge_in_query_geolocation_info(combined_queries), True


def _add_neutral_geo_columns(combined_queries: pd.DataFrame) -> None:
    """Add the columns GeoIP resolution would have produced, as NaN, so later steps find them."""
    for col in ["dst_lat", "dst_lon", "dst_country", "org", "ip_addr"]:
        combined_queries[col] = np.nan
    combined_queries["indexer_network"] = "arbitrum"
    # Source geo columns from merge_in_query_geolocation_info
    combined_queries["IATA_code"] = combined_queries["query_id"].str[-3:]
    for col in ["src_lat", "src_lon", "src_country"]:
        combined_queries[col] = np.nan


def _fit_latency_rankings(
    combined_queries: pd.DataFrame,
    target_rows_per_subgraph: int,
    start_date: date,
    seed: Optional[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Full path: distances, filtering, sampling and the latency regression.

    Returns (latency_rankings, indexer_query_count). Raises RuntimeError when the data thins
    out to nothing at any stage, since a regression on an empty frame would be meaningless.
    """
    combined_queries = calculate_distances(combined_queries)

    logger.info(f"Before filter_successful_queries: {len(combined_queries)} rows")
    dst_lat_nan_count = combined_queries["dst_lat"].isna().sum()
    src_lat_nan_count = combined_queries["src_lat"].isna().sum()
    logger.info(f"  src_lat NaN: {src_lat_nan_count}, dst_lat NaN: {dst_lat_nan_count}")

    if dst_lat_nan_count == len(combined_queries):
        raise RuntimeError(
            "All dst_lat values are NaN - GeoIP resolution failed for all indexers."
            + diagnose_geoip_failure(combined_queries)
        )

    combined_queries_filtered = filter_successful_queries(combined_queries)
    logger.info(f"After filter_successful_queries: {len(combined_queries_filtered)} rows")

    predictor = ["response_time_ms"]
    categorical = ["indexer", "deployment_hash", "indexer_network", "query_id"]
    numeric = ["distance_miles", "fee"]

    filtered_data = combined_queries_filtered[predictor + categorical + numeric]
    logger.info(f"After column selection: {len(filtered_data)} rows")
    dist_nan = filtered_data["distance_miles"].isna().sum()
    fee_nan = filtered_data["fee"].isna().sum()
    logger.info(f"  NaN counts - distance_miles: {dist_nan}, fee: {fee_nan}")
    filtered_data = filtered_data.dropna(subset=numeric)
    logger.info(f"After dropna(numeric): {len(filtered_data)} rows")

    if len(filtered_data) == 0:
        raise RuntimeError(
            "No rows remain after dropping NaN values in numeric "
            "columns (distance_miles, fee). This typically means "
            "GeoIP resolution failed (all distances are NaN) or "
            "there's a data quality issue with the source tables."
        )

    filtered_data = iterative_filter(
        filtered_data,
        ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS,
        ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER,
        ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER,
        ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT,
    )

    if len(filtered_data) == 0:
        raise RuntimeError(
            "No rows remain after iterative filtering. Either the data volume is too low "
            "or the filter thresholds are too strict for the current dataset."
        )

    if seed is None:
        seed = default_scoring_seed(start_date)
        logger.info("No scoring seed supplied; using %d derived from the start date", seed)
    rng = np.random.default_rng(seed)
    filtered_data, integer_root = strategic_sample(filtered_data, target_rows_per_subgraph, rng=rng)
    filtered_data = hash_sampled_queries(filtered_data, integer_root)

    categorical = [
        "indexer",
        "deployment_hash",
        "indexer_network",
        "sampled_query_id_hashed_mod_integer_root",
    ]

    latency_rankings, _ = perform_latency_linear_regression(
        filtered_data, predictor, categorical, numeric
    )
    indexer_query_count = filtered_data.groupby("indexer").size().reset_index(name="query_count")
    return latency_rankings, indexer_query_count


def _neutral_latency_rankings(combined_queries: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Partial path: a neutral latency row per indexer and a 0 query count for each."""
    unique_indexers = combined_queries["indexer"].unique()
    latency_rankings = pd.DataFrame(
        {
            "indexer": unique_indexers,
            LATENCY_COEFFICIENT_COLUMN: 0.0,
            STANDARD_ERROR_COLUMN: 0.0,
            "p-value": 1.0,
            "Latency Coefficient + Error Confidence Interval": 0.0,
        }
    )
    indexer_query_count = pd.DataFrame({"indexer": unique_indexers, "query_count": 0})
    return latency_rankings, indexer_query_count


def _attach_dips_info(merged: pd.DataFrame, indexer_urls: Dict[str, str]) -> pd.DataFrame:
    """Merge each indexer's DIP pricing into the scores, or fill the columns with no-data values."""
    if indexer_urls:
        dips_info_df = fetch_dips_info(indexer_urls)
        merged = pd.merge(merged, dips_info_df, on="indexer", how="left")
        merged["dips_info_available"] = merged["dips_info_available"].fillna(False)
        return merged

    merged["dips_info_available"] = False
    merged["dips_min_grt_per_30_days"] = "{}"
    merged["dips_min_grt_per_billion_entities_per_30_days"] = None
    merged["dips_supported_networks"] = "[]"
    return merged


def compute_all_scores(
    provider,
    start_date: date,
    start_ts: str,
    num_days: int,
    target_rows: int,
    geoip_available: bool = True,
    seed: Optional[int] = None,
) -> pd.DataFrame:
    """Orchestrate full / partial scoring; return an indexer_scores DataFrame.

    Full (geoip_available=True): GeoIP + latency regression + uptime + success + stake.
    Partial: latency neutral (0.5), other metrics from real Redpanda data.
    """
    # Fetch initial query results to determine sampling
    initial_query_results = provider.fetch_initial_query_results(start_date, num_days)
    target_rows_per_subgraph = adjust_rows(initial_query_results, target_rows)

    # Fetch combined query data (~20M rows)
    combined_queries = provider.fetch_combined_query_results(
        start_date, num_days, target_rows_per_subgraph
    )

    if geoip_available:
        combined_queries, geoip_available = _resolve_geoip_or_demote(combined_queries)

    if not geoip_available:
        # No GeoIP (configured off, or demoted from full mode above)
        logger.warning(
            "GeoIP unavailable, skipping geo resolution. Latency scores will be neutral."
        )
        _add_neutral_geo_columns(combined_queries)

    # Save data for uptime calculations before filtering
    data_for_uptime = combined_queries[["indexer", "status", "timestamp"]].copy()

    if geoip_available:
        latency_rankings, indexer_query_count = _fit_latency_rankings(
            combined_queries, target_rows_per_subgraph, start_date, seed
        )
    else:
        latency_rankings, indexer_query_count = _neutral_latency_rankings(combined_queries)

    # GeoIP-independent metrics: always computed from real Redpanda data
    indexer_success_rate = calculate_indexer_success_rate(combined_queries)
    indexer_uptime = calculate_indexer_uptime(data_for_uptime)

    stake_to_fees = provider.fetch_stake_to_fees(start_ts)

    agg_df = aggregate_indexer_info(combined_queries)

    # Merge all data together
    merged = merge_and_prepare_dataframes(
        indexer_uptime,
        latency_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
        indexer_query_count,
        drop_missing_latency=geoip_available,
    )

    # DIP pricing fetched directly from indexers; the indexer list comes from
    # the network subgraph (Redpanda only has indexers the gateway has queried,
    # so newly registered ones would be invisible without this lookup).
    indexer_urls = discover_indexers_from_network_subgraph(provider.graph_network_url)
    merged = _attach_dips_info(merged, indexer_urls)

    # Attach graph-node versions and drop indexers below the configured
    # minimum (no-op when MIN_GRAPH_NODE_VERSION is unset).
    merged = fetch_and_filter_graph_node_versions(merged, indexer_urls)

    # Transform to indexer_scores schema with pre-normalized values
    scores_df = transform_to_scores_schema(merged)
    scores_df["scoring_mode"] = "full" if geoip_available else "partial_no_geoip"

    mode = "full" if geoip_available else "partial"
    logger.info(
        f"Score computation complete: {len(scores_df)} indexers, "
        f"mode={mode}, columns: {list(scores_df.columns)}"
    )
    return scores_df


def transform_to_scores_schema(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the merged DataFrame to the indexer_scores output schema.

    Includes pre-normalizing static metrics.
    """
    # Build the output DataFrame
    scores = pd.DataFrame()

    # Indexer identification
    scores["indexer"] = merged["indexer"]
    scores["url"] = merged.get("url", None)

    # Latency metrics
    scores["lat_lin_reg_coefficient"] = merged.get(LATENCY_COEFFICIENT_COLUMN)
    scores["lat_coefficient_std_error"] = merged.get(STANDARD_ERROR_COLUMN)
    scores["lat_coefficient_upper_bound"] = merged.get(
        "Latency Coefficient + Error Confidence Interval"
    )

    # Compute normalized latency score (lower latency = higher score)
    lat_raw = merged.get("Latency Coefficient + Error Confidence Interval")
    if lat_raw is not None:
        scores["lat_normalized_score"] = normalize_to_0_1_inverted(lat_raw)
    else:
        scores["lat_normalized_score"] = None

    # Uptime metrics
    scores["uptime_score"] = merged.get("% up_x", 0) / 100.0  # Convert percentage to 0-1
    scores["observed_duration_seconds"] = merged.get("observed_duration_restricted")
    scores["uptime_duration_seconds"] = merged.get("uptime_duration_restricted")

    # Success rate
    scores["success_rate"] = merged.get("average_status")

    # Economic security metrics
    scores["stake_to_fees"] = merged.get("stake_to_fees")
    scores["total_query_fees"] = merged.get("total_query_fees", 0.0)
    scores["last_known_slashable_stake"] = merged.get("last_known_slashable_stake", 0.0)

    # Pre-normalized scores
    scores["norm_uptime_score"] = normalize_to_0_1(scores["uptime_score"])
    scores["norm_success_rate"] = normalize_to_0_1(scores["success_rate"])

    # Organization/location
    scores["org"] = merged.get("org")
    scores["dst_lat"] = merged.get("dst_lat")
    scores["dst_lon"] = merged.get("dst_lon")

    # DIP pricing info (fetched from indexer /dips/info endpoints)
    scores["dips_info_available"] = merged.get("dips_info_available", False)
    scores["dips_min_grt_per_30_days"] = merged.get("dips_min_grt_per_30_days", "{}")
    scores["dips_min_grt_per_billion_entities_per_30_days"] = merged.get(
        "dips_min_grt_per_billion_entities_per_30_days"
    )
    scores["dips_supported_networks"] = merged.get("dips_supported_networks", "[]")

    # Graph-node version (from /status). Carried through so the published
    # JSON shape stays the same whether the filter ran or not — degraded
    # also writes these, so omitting them would split the schema by mode.
    scores["graph_node_version"] = merged.get("graph_node_version")
    scores["graph_node_commit"] = merged.get("graph_node_commit")

    # Metadata
    scores["computed_at"] = datetime.now(timezone.utc)
    scores["query_count"] = merged.get("query_count")  # Per-indexer query count used in regression

    return scores


def normalize_to_0_1(series: pd.Series) -> pd.Series:
    """Normalize a series to 0-1 range using min-max scaling."""
    if series is None or series.empty:
        return series
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(series))
    normalized: pd.Series = (series - min_val) / (max_val - min_val)
    return normalized


def normalize_to_0_1_inverted(series: pd.Series) -> pd.Series:
    """Normalize and invert (lower = better becomes higher score)."""
    if series is None or series.empty:
        return series
    normalized = normalize_to_0_1(series)
    return 1 - normalized


def resolve_indexer_geoip(combined_queries: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique indexers from query data and resolve their GeoIP information.

    Uses bundled MaxMind GeoLite2 databases for offline lookups.
    """
    # Get unique indexer/url pairs
    unique_indexers = combined_queries[["indexer", "url"]].drop_duplicates()
    unique_indexers = unique_indexers.dropna(subset=["url"])

    logger.info(f"Resolving GeoIP for {len(unique_indexers)} unique indexers")

    # Create GeoIP resolver with caching
    geoip_cache: Dict[str, dict] = {}

    def resolve_url(url: str) -> dict:
        if url in geoip_cache:
            return geoip_cache[url]

        result = resolve_url_geoip(url)
        geoip_cache[url] = result
        return result

    # Resolve GeoIP for each indexer
    geoip_data = unique_indexers["url"].apply(resolve_url)
    geoip_df = pd.DataFrame(geoip_data.tolist())

    # Combine with indexer info
    indexers_df = pd.concat([unique_indexers.reset_index(drop=True), geoip_df], axis=1)
    indexers_df["indexer_network"] = "arbitrum"

    # Rename columns to match expected schema
    indexers_df = indexers_df.rename(columns=GEOIP_DST_COLUMN_MAPPING)

    # Log GeoIP resolution summary
    total = len(indexers_df)
    failed = indexers_df["dst_lat"].isna().sum()
    resolved = total - failed
    logger.info(f"GeoIP resolution complete: {resolved}/{total} resolved, {failed} failed")

    return indexers_df


def resolve_url_geoip(url: str) -> dict:
    """Resolve GeoIP information for a URL using local GeoLite2 databases."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            return _empty_geoip_result()

        # Resolve hostname to IP
        try:
            _, _, ip_addrs = socket.gethostbyname_ex(host)
            if not ip_addrs:
                return _empty_geoip_result()
            ip_addr = sorted(ip_addrs)[0]
        except socket.gaierror:
            return _empty_geoip_result()

        # Check for private IP
        if is_private_ip(ip_addr):
            return _empty_geoip_result(ip_addr)

        # Lookup in local GeoLite2 databases
        return lookup_geoip(ip_addr)

    except Exception as e:
        logger.warning(f"GeoIP resolution failed for {url}: {e}")
        return _empty_geoip_result()


def is_private_ip(ip_addr: str) -> bool:
    """True for any address that legitimately has no public geolocation.

    Delegates to ``ipaddress.ip_address(...).is_private`` (covers RFC 1918,
    loopback, link-local, CGNAT, IPv6 ULA / link-local / loopback).
    Returns False for malformed input.
    """
    try:
        return ipaddress.ip_address(ip_addr).is_private
    except ValueError as e:
        logger.debug(f"is_private_ip check failed for {ip_addr}: {e}")
        return False


def lookup_geoip(ip_addr: str) -> dict:
    """Look up GeoIP information from MaxMind GeoLite2 databases."""
    try:
        # Location from City database
        city_reader = get_geoip_city_reader()
        city_response = city_reader.city(ip_addr)

        # Org from ASN database
        org = None
        try:
            asn_reader = get_geoip_asn_reader()
            asn_response = asn_reader.asn(ip_addr)
            org = asn_response.autonomous_system_organization
        except geoip2.errors.AddressNotFoundError:
            pass  # IP not in ASN database

        return {
            "ip_addr": ip_addr,
            "org": org,
            "country": city_response.country.iso_code,
            "latitude": city_response.location.latitude,
            "longitude": city_response.location.longitude,
        }
    except geoip2.errors.AddressNotFoundError:
        logger.debug(f"IP address not found in GeoIP database: {ip_addr}")
        return _empty_geoip_result(ip_addr)
    except Exception as e:
        logger.warning(f"GeoIP lookup failed for {ip_addr}: {e}")
        return _empty_geoip_result(ip_addr)


# --- Adapted from IISA processing.py ---


def adjust_rows(initial_query_results: pd.DataFrame, target_rows: int) -> int:
    """Adjust rows per group to approximate target total rows."""
    if target_rows < 0:
        raise ValueError("Target rows must be non-negative")

    df = initial_query_results.copy()
    x = 1_000
    df["num_rows_restricted"] = df["num_rows"].clip(upper=x)
    tolerance = target_rows * 0.01
    max_iterations = 1_000

    for _ in range(max_iterations):
        current_sum = df["num_rows_restricted"].sum()
        if target_rows - tolerance <= current_sum <= target_rows + tolerance:
            break
        if current_sum > target_rows:
            x = int(x * 0.99)
        else:
            x = int(x * 1.01)
        df["num_rows_restricted"] = df["num_rows"].clip(upper=x)

    max_val = df["num_rows_restricted"].max()
    result = 0 if pd.isna(max_val) else int(max_val)
    logger.info(f"Calculated target rows per subgraph: {result}")
    return result


def merge_in_indexers_info(combined_queries: pd.DataFrame, indexers: pd.DataFrame) -> pd.DataFrame:
    """Merge indexer GeoIP info into combined queries."""
    right_df = indexers.rename(columns=GEOIP_DST_COLUMN_MAPPING)
    return pd.merge(combined_queries, right_df, on=["indexer", "url"], how="left")


def merge_in_query_geolocation_info(combined_queries: pd.DataFrame) -> pd.DataFrame:
    """Merge IATA geolocation info based on query_id suffix."""
    combined_queries["IATA_code"] = combined_queries["query_id"].str[-3:]

    iata_info = load_iata_data()
    right_df = iata_info.rename(columns=GEOIP_SRC_COLUMN_MAPPING)

    return pd.merge(combined_queries, right_df, on="IATA_code", how="left")


def load_iata_data() -> pd.DataFrame:
    """Load IATA airport data from airportsdata package."""
    airportsdata_csv = Path(airportsdata.__file__).parent / "airports.csv"

    iata_df = pd.read_csv(
        airportsdata_csv,
        usecols=["iata", "lat", "lon", "country"],
        na_values={"iata": [""], "country": [""]},
        keep_default_na=False,
    )
    iata_df.rename(
        columns={"iata": "IATA_code", "lat": "latitude", "lon": "longitude"},
        inplace=True,
    )
    iata_df.dropna(subset=["IATA_code"], inplace=True)
    return iata_df


def calculate_distances(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate haversine distances between source and destination."""
    data["distance_miles"] = haversine_vectorized(
        data["src_lon"], data["src_lat"], data["dst_lon"], data["dst_lat"]
    )
    data["distance_miles"] = data["distance_miles"].apply(
        lambda val: round(val / 250.0) * 250.0 if pd.notna(val) else val
    )
    return data


def haversine_vectorized(lon1, lat1, lon2, lat2):
    """Vectorized haversine distance calculation."""
    lon1, lat1, lon2, lat2 = [
        np.radians(np.asarray(x, dtype=float)) for x in [lon1, lat1, lon2, lat2]
    ]
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return c * 3956  # Earth radius in miles


def filter_successful_queries(data: pd.DataFrame) -> pd.DataFrame:
    """Filter to only successful queries (200 OK)."""
    return data[data["status"] == REQUEST_STATUS_OK].copy()


def iterative_filter(
    df: pd.DataFrame,
    min_deployment_indexers: int,
    min_deployments_per_indexer: int,
    min_queries_per_indexer: int,
    min_queries_per_deployment: int,
) -> pd.DataFrame:
    """Iteratively filter data based on minimum thresholds."""
    logger.info(f"iterative_filter starting with {len(df)} rows")
    iteration = 0
    while True:
        initial_len = len(df)
        iteration += 1

        # Ensure deployments have minimum indexers
        indexer_per_deployment = df.groupby("deployment_hash")["indexer"].nunique()
        df = df[df["deployment_hash"].map(indexer_per_deployment) >= min_deployment_indexers]
        logger.info(
            f"  iter {iteration} after min_deployment_indexers "
            f"({min_deployment_indexers}): {len(df)} rows"
        )

        # Ensure indexers serve minimum deployments
        deployment_per_indexer = df.groupby("indexer")["deployment_hash"].nunique()
        df = df[df["indexer"].map(deployment_per_indexer) >= min_deployments_per_indexer]
        logger.info(
            f"  iter {iteration} after min_deployments_per_indexer "
            f"({min_deployments_per_indexer}): {len(df)} rows"
        )

        # Ensure indexers serve minimum queries
        queries_per_indexer = df.groupby("indexer")["query_id"].nunique()
        df = df[df["indexer"].map(queries_per_indexer) >= min_queries_per_indexer]
        logger.info(
            f"  iter {iteration} after min_queries_per_indexer "
            f"({min_queries_per_indexer}): {len(df)} rows"
        )

        # Ensure deployments have minimum queries
        query_counts = df.groupby("deployment_hash").size()
        df = df[df["deployment_hash"].map(query_counts) >= min_queries_per_deployment]
        logger.info(
            f"  iter {iteration} after min_queries_per_deployment "
            f"({min_queries_per_deployment}): {len(df)} rows"
        )

        if len(df) == initial_len:
            break

    logger.info(f"iterative_filter finished with {len(df)} rows after {iteration} iterations")
    return pd.DataFrame(df)


def strategic_sample(
    df: pd.DataFrame, target_rows_per_subgraph: int, rng: Optional[np.random.Generator] = None
) -> Tuple[pd.DataFrame, int]:
    """Sample queries to create balanced representation across indexers."""
    if rng is None:
        rng = np.random.default_rng()

    if df.empty:
        df["sampled_query_id"] = pd.Series(dtype="float64")
        return df, 0

    indexers_per_subgraph = df.groupby("deployment_hash")["indexer"].nunique()
    cap_per_indexer = indexers_per_subgraph.map(
        lambda x: target_rows_per_subgraph // x if x else 0
    ).to_dict()

    query_counts = (
        df.groupby(["deployment_hash", "indexer"])["query_id"]
        .agg(lambda x: list(x.unique()))
        .reset_index(name="unique_query_ids")
    )
    query_counts["cap"] = query_counts["deployment_hash"].map(cap_per_indexer)

    def sample_queries(query_ids, cap):
        query_ids = list(np.concatenate(query_ids)) if isinstance(query_ids[0], list) else query_ids
        return rng.choice(query_ids, size=min(len(query_ids), cap), replace=False)

    query_counts["sampled_query_id_list"] = query_counts.apply(
        lambda x: sample_queries(x["unique_query_ids"], x["cap"]), axis=1
    )

    sampled_ids = set(np.concatenate(query_counts["sampled_query_id_list"].values))
    df["sampled_query_id"] = df["query_id"].apply(lambda x: x if x in sampled_ids else None)

    integer_root = int(np.sqrt(len(sampled_ids)))
    return df, integer_root


def hash_sampled_queries(df: pd.DataFrame, integer_root: int) -> pd.DataFrame:
    """Hash sampled query IDs for regression."""
    result_df = df.copy()
    mask = result_df["sampled_query_id"].notna()
    result_df.loc[mask, "sampled_query_id_hashed_mod_integer_root"] = result_df.loc[
        mask, "sampled_query_id"
    ].apply(
        lambda x: int.from_bytes(hashlib.sha256(str(x).encode()).digest()[:8], byteorder="big")
        % integer_root
    )
    return result_df


def perform_latency_linear_regression(
    df: pd.DataFrame, predictor: list, categorical: list, numeric: list
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Perform latency linear regression analysis."""
    model_columns = categorical + numeric
    x = df[model_columns]
    y = df[predictor]

    preprocessor = ColumnTransformer(
        transformers=[
            ("one_hot", OneHotEncoder(handle_unknown="ignore", drop="first"), categorical),
            ("scaler", StandardScaler(), numeric),
        ],
        remainder="passthrough",
    )

    pipeline = Pipeline([("preprocessor", preprocessor), ("regressor", LinearRegression())])
    try:
        logger.info(
            f"Fitting linear regression model with {len(x)} samples, "
            f"{len(categorical)} categorical and {len(numeric)} numeric "
            f"features"
        )
        pipeline.fit(x, y)
    except Exception:
        logger.exception("Linear regression fitting failed")
        raise RuntimeError(
            "Latency linear regression failed. This may indicate data quality issues "
            "(e.g., insufficient variance, collinearity, or too few samples)."
        )
    y_pred = pipeline.predict(x)

    # Analyze results
    mse = mean_squared_error(y, y_pred)
    feature_names = pipeline.named_steps["preprocessor"].get_feature_names_out()
    coefficients = pipeline.named_steps["regressor"].coef_.flatten()

    x_transformed = pipeline.named_steps["preprocessor"].transform(x)
    xtx_inv = pinv(np.dot(x_transformed.T, x_transformed) + np.eye(x_transformed.shape[1]) * 1.0)
    var_covar_matrix = mse * xtx_inv
    std_errors = np.sqrt(np.diag(var_covar_matrix))

    deg_freedom = len(y) - len(coefficients)
    t_scores = coefficients / std_errors
    p_values = [2 * (1 - t.cdf(abs(ts), deg_freedom)) for ts in t_scores]

    results_df = pd.DataFrame(
        {
            "Variable": feature_names,
            LATENCY_COEFFICIENT_COLUMN: coefficients,
            STANDARD_ERROR_COLUMN: std_errors,
            "p-value": p_values,
        }
    )

    # Calculate robust normalized coefficients
    indexer_rankings = results_df[
        (results_df["Variable"].str.startswith("one_hot__indexer_"))
        & (~results_df["Variable"].str.startswith("one_hot__indexer_network_"))
    ].sort_values(by=LATENCY_COEFFICIENT_COLUMN)

    indexer_rankings = indexer_rankings.reset_index(drop=True)
    indexer_rankings["Variable"] = indexer_rankings["Variable"].str.replace("one_hot__indexer_", "")
    indexer_rankings.rename(columns={"Variable": "indexer"}, inplace=True)
    indexer_rankings.dropna(
        subset=[LATENCY_COEFFICIENT_COLUMN, STANDARD_ERROR_COLUMN, "p-value"], inplace=True
    )

    indexer_rankings["Latency Coefficient + Error Confidence Interval"] = (
        indexer_rankings[LATENCY_COEFFICIENT_COLUMN]
        + LATENCY_COEFFICIENT_STANDARD_ERROR_MULTIPLIER * indexer_rankings[STANDARD_ERROR_COLUMN]
    )

    return indexer_rankings, results_df


def calculate_indexer_success_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate success rate for each indexer."""
    df_filtered = df[["indexer", "status"]].copy()
    df_filtered["status_numeric"] = df_filtered["status"].apply(
        lambda x: 1 if x in [REQUEST_STATUS_OK, REQUEST_STATUS_UNAVAILABLE_MISSING_BLOCK] else 0
    )
    return (
        df_filtered.groupby("indexer").agg(average_status=("status_numeric", "mean")).reset_index()
    )


def calculate_indexer_uptime(df: pd.DataFrame, threshold_seconds: int = 120) -> pd.DataFrame:
    """Calculate indexer uptime based on query timestamps and statuses."""
    df_copy = df.copy()
    df_copy["timestamp"] = pd.to_datetime(df_copy["timestamp"])
    df_copy.sort_values(by=["indexer", "timestamp"], inplace=True)

    df_copy["next_timestamp"] = df_copy.groupby("indexer")["timestamp"].shift(-1)
    df_copy["previous_timestamp"] = df_copy.groupby("indexer")["timestamp"].shift(1)

    df_copy["gap_to_next_query"] = (
        df_copy["next_timestamp"] - df_copy["timestamp"]
    ).dt.total_seconds()
    df_copy["gap_to_previous_query"] = (
        df_copy["timestamp"] - df_copy["previous_timestamp"]
    ).dt.total_seconds()

    df_copy["next_midpoint"] = df_copy["timestamp"] + pd.to_timedelta(
        df_copy["gap_to_next_query"] / 2, unit="s"
    )
    df_copy["next_midpoint"] = df_copy["next_midpoint"].fillna(df_copy["timestamp"])

    df_copy["previous_midpoint"] = df_copy["timestamp"] - pd.to_timedelta(
        df_copy["gap_to_previous_query"] / 2, unit="s"
    )
    df_copy["previous_midpoint"] = df_copy["previous_midpoint"].fillna(df_copy["timestamp"])

    df_copy["is_up"] = df_copy["status"].isin(
        [REQUEST_STATUS_OK, REQUEST_STATUS_UNAVAILABLE_MISSING_BLOCK]
    )

    df_copy["uptime_duration_full"] = (
        (df_copy["next_midpoint"] - df_copy["previous_midpoint"])
        .dt.total_seconds()
        .where(df_copy["is_up"], 0)
    )
    df_copy["uptime_duration_restricted"] = np.minimum(
        (df_copy["next_midpoint"] - df_copy["previous_midpoint"])
        .dt.total_seconds()
        .where(df_copy["is_up"], 0),
        threshold_seconds,
    )

    df_copy["observed_duration_full"] = (
        df_copy["next_midpoint"] - df_copy["previous_midpoint"]
    ).dt.total_seconds()
    df_copy["observed_duration_restricted"] = np.minimum(
        (df_copy["next_midpoint"] - df_copy["previous_midpoint"]).dt.total_seconds(),
        threshold_seconds,
    )

    # Aggregate by indexer
    uptime_full = df_copy.groupby("indexer")["uptime_duration_full"].sum()
    uptime_restricted = df_copy.groupby("indexer")["uptime_duration_restricted"].sum()
    observed_full = df_copy.groupby("indexer")["observed_duration_full"].sum()
    observed_restricted = df_copy.groupby("indexer")["observed_duration_restricted"].sum()

    merged_restricted = pd.merge(
        observed_restricted, uptime_restricted, on="indexer", how="left"
    ).reset_index()
    merged_restricted["% up"] = round(
        merged_restricted["uptime_duration_restricted"]
        / merged_restricted["observed_duration_restricted"]
        * 100,
        3,
    )
    merged_restricted = merged_restricted.sort_values(by="% up", ascending=False)

    merged_full = pd.merge(observed_full, uptime_full, on="indexer", how="left").reset_index()
    merged_full["% up"] = round(
        merged_full["uptime_duration_full"] / merged_full["observed_duration_full"] * 100, 3
    )
    merged_full = merged_full.sort_values(by="% up", ascending=False)

    return pd.merge(merged_restricted, merged_full, on="indexer", how="left")


def aggregate_indexer_info(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate organizational and location info per indexer."""

    def round_to_20(x):
        return x if pd.isna(x) else round(x / 20) * 20

    def first_non_null(x: pd.Series):
        """Return first non-null value, or NaN if all null."""
        non_null = x.dropna()
        return non_null.iloc[0] if len(non_null) > 0 else np.nan

    def first_mode(x: pd.Series):
        """Return the most common value, or NaN if the series is empty."""
        modes = x.mode()
        return modes[0] if not modes.empty else np.nan

    agg_df = (
        df.groupby("indexer")
        .agg(
            {
                "url": first_non_null,  # Take first non-null URL for this indexer
                "org": first_mode,
                "dst_lat": first_mode,
                "dst_lon": first_mode,
            }
        )
        .reset_index()
    )

    agg_df["dst_lat"] = agg_df["dst_lat"].apply(round_to_20)
    agg_df["dst_lon"] = agg_df["dst_lon"].apply(round_to_20)

    return agg_df


def merge_and_prepare_dataframes(
    indexer_uptime: pd.DataFrame,
    indexer_rankings: pd.DataFrame,
    agg_df: pd.DataFrame,
    indexer_success_rate: pd.DataFrame,
    stake_to_fees: pd.DataFrame,
    indexer_query_count: pd.DataFrame,
    drop_missing_latency: bool = True,
) -> pd.DataFrame:
    """Merge all indexer data into a single DataFrame.

    When drop_missing_latency=True (full GeoIP mode), drops rows where latency
    columns are NaN. When False (partial mode with synthetic latency), skips
    the dropna since synthetic values have no NaN by construction.
    """
    merged = pd.merge(indexer_uptime, indexer_rankings, on="indexer", how="left")

    columns_to_drop = ["observed_duration_full", "uptime_duration_full", "% up_y"]
    merged = merged.drop(columns=[c for c in columns_to_drop if c in merged.columns])

    if drop_missing_latency:
        columns_to_check = [LATENCY_COEFFICIENT_COLUMN, STANDARD_ERROR_COLUMN, "p-value"]
        existing = [c for c in columns_to_check if c in merged.columns]
        if existing:
            merged = merged.dropna(subset=existing)

    merged = pd.merge(merged, agg_df, on="indexer", how="left")
    merged = pd.merge(merged, indexer_success_rate, on="indexer", how="left")
    merged = pd.merge(merged, stake_to_fees, on="indexer", how="left")
    merged = pd.merge(merged, indexer_query_count, on="indexer", how="left")

    return merged


def compute_degraded_scores(graph_network_subgraph_url: str) -> pd.DataFrame:
    """Equal quality metrics for every indexer, real per-indexer pricing from /dips/info.

    Last-resort fallback when the full pipeline can't run. Indexer list comes
    from the network subgraph; per-indexer pricing from /dips/info.
    """
    indexer_urls = discover_indexers_from_network_subgraph(graph_network_subgraph_url)
    if not indexer_urls:
        return pd.DataFrame()

    dips_info_df = fetch_dips_info(indexer_urls)
    now = datetime.now(timezone.utc)

    scores = pd.DataFrame(
        {
            "indexer": list(indexer_urls.keys()),
            "url": list(indexer_urls.values()),
        }
    )

    # Equal quality metrics — all indexers treated identically
    scores["lat_lin_reg_coefficient"] = 0.0
    scores["lat_coefficient_std_error"] = 0.0
    scores["lat_coefficient_upper_bound"] = 0.0
    scores["lat_normalized_score"] = 0.5
    scores["uptime_score"] = 0.5
    scores["observed_duration_seconds"] = None
    scores["uptime_duration_seconds"] = None
    scores["success_rate"] = 0.5
    scores["stake_to_fees"] = None
    scores["norm_uptime_score"] = 0.5
    scores["norm_success_rate"] = 0.5
    scores["norm_stake_to_fees"] = 0.5
    scores["org"] = None
    scores["dst_lat"] = None
    scores["dst_lon"] = None
    scores["total_query_fees"] = 0.0
    scores["last_known_slashable_stake"] = 0.0
    scores["computed_at"] = now
    scores["query_count"] = 0

    # Merge real pricing data from /dips/info
    if not dips_info_df.empty:
        scores = pd.merge(scores, dips_info_df, on="indexer", how="left")
        scores["dips_info_available"] = scores["dips_info_available"].fillna(False)
    else:
        scores["dips_info_available"] = False
        scores["dips_min_grt_per_30_days"] = "{}"
        scores["dips_min_grt_per_billion_entities_per_30_days"] = None
        scores["dips_supported_networks"] = "[]"

    # Attach graph-node versions and drop indexers below the configured
    # minimum (no-op when MIN_GRAPH_NODE_VERSION is unset).
    scores = fetch_and_filter_graph_node_versions(scores, indexer_urls)

    available = (
        scores["dips_info_available"].sum() if "dips_info_available" in scores.columns else 0
    )
    logger.info(f"Degraded scoring complete: {len(scores)} indexers, {available} with pricing data")
    return scores
