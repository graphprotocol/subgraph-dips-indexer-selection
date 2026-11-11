"""Score computation cronjob entry point.

One-shot: compute scores, push to iisa, exit. Must not linger because
GKE Autopilot bills the pod's 50 GiB memory request for its full lifetime.
Exit codes: 0 ok; 1 config/run failure; 2 IISA_REQUIRE_PUSH_TOKEN unprovisioned.
"""

import logging
import os
import random
import resource
import sys
import time
from datetime import date, datetime, timedelta
from typing import Dict, Optional

import pandas as pd
from carry_over import QUALITY_MEASURED_AT, carried_over
from iisa_client import IISAPushError, get_push_token
from processing import (
    compute_all_scores,
    compute_carried_over_scores,
    compute_degraded_scores,
    validate_geoip_databases,
)
from redpanda import RedpandaProvider
from stake_reads import STAKE_READ_AT, utc_now
from subgraph import discover_indexers_from_network_subgraph, subgraph_url_problem

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

NUM_DAYS = int(os.environ.get("NUM_DAYS", "28"))
TARGET_ROWS = int(os.environ.get("TARGET_ROWS", "20000000"))
GRAPH_NETWORK_SUBGRAPH_URL = os.environ.get("GRAPH_NETWORK_SUBGRAPH_URL", "")
IISA_API_URL = os.environ.get("IISA_API_URL", "")
IISA_REQUIRE_PUSH_TOKEN = os.environ.get("IISA_REQUIRE_PUSH_TOKEN", "").lower() == "true"
IISA_PUSH_TOKEN = get_push_token()


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""


MODE_FULL = "full"
MODE_PARTIAL = "partial"
MODE_CARRIED_OVER = "carried_over"
MODE_DEGRADED = "degraded"
MODE_FAILED = "failed"


def validate_configuration() -> None:
    """Validate required configuration before starting.

    Raises ConfigurationError if any required config is missing or invalid.
    """
    errors = []

    if NUM_DAYS < 1:
        errors.append(f"NUM_DAYS must be >= 1, got {NUM_DAYS}")

    if TARGET_ROWS < 1000:
        errors.append(f"TARGET_ROWS must be >= 1000, got {TARGET_ROWS}")

    if not os.environ.get("REDPANDA_BOOTSTRAP_SERVERS"):
        errors.append("REDPANDA_BOOTSTRAP_SERVERS is required")

    # No default on purpose: falling back to the mainnet topic would quietly score the wrong
    # network in any environment whose config forgot to name its own.
    if not os.environ.get("REDPANDA_TOPIC"):
        errors.append("REDPANDA_TOPIC is required")

    if not IISA_API_URL:
        errors.append("IISA_API_URL is required")

    # Without indexer URLs every indexer fails its DIPs probe, selection picks no one and
    # dipper cancels every agreement, so refuse to start rather than score without them.
    url_problem = subgraph_url_problem(GRAPH_NETWORK_SUBGRAPH_URL)
    if url_problem:
        errors.append(f"GRAPH_NETWORK_SUBGRAPH_URL {url_problem}")

    if errors:
        for error in errors:
            logger.error("Configuration error: %s", error)
        raise ConfigurationError(f"Found {len(errors)} configuration error(s)")

    logger.info("Configuration validation passed")


def get_peak_memory_mb() -> float:
    """Get peak memory usage in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / 1024 / 1024
    return usage / 1024


def _mode_from_scores(scores_df: pd.DataFrame, geoip_available: bool) -> str:
    """Read the mode the pipeline actually ran in from its output.

    compute_all_scores may demote internally to partial when every GeoIP lookup failed,
    so the summary must reflect what was published, not what was requested.
    """
    if "scoring_mode" not in scores_df.columns:
        return MODE_FULL if geoip_available else MODE_PARTIAL
    actual = scores_df["scoring_mode"].iloc[0]
    return MODE_PARTIAL if actual == "partial_no_geoip" else MODE_FULL


def _discover_indexer_urls() -> Optional[Dict[str, str]]:
    """Read indexer URLs once, before any scoring; None, after logging why, if that fails.

    Without them every indexer fails its DIPs probe, so the run stops and iisa keeps its
    last scores rather than spend the replay on scores it must not push.
    """
    try:
        return discover_indexers_from_network_subgraph(GRAPH_NETWORK_SUBGRAPH_URL)
    except Exception as e:
        logger.error("Failed to read indexer URLs from the network subgraph: %s", e, exc_info=True)
        return None


def _run_full_pipeline(
    provider: RedpandaProvider, geoip_available: bool, seed: int, indexer_urls: Dict[str, str]
) -> tuple[Optional[pd.DataFrame], str]:
    """Run compute_all_scores over the last NUM_DAYS; return (scores, mode).

    Returns (None, MODE_FAILED) when the pipeline raises or produces no rows, so the
    caller can fall back to degraded scoring. compute_all_scores handles both the full
    and the partial (no GeoIP) modes itself.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=NUM_DAYS)

    mode_label = "full" if geoip_available else "partial (no GeoIP)"
    logger.info("Attempting %s pipeline for %s to %s", mode_label, start_date, end_date)
    try:
        scores_df = compute_all_scores(
            provider=provider,
            indexer_urls=indexer_urls,
            start_date=start_date,
            num_days=NUM_DAYS,
            target_rows=TARGET_ROWS,
            geoip_available=geoip_available,
            seed=seed,
        )
        if scores_df.empty:
            logger.warning("Pipeline returned empty results")
            return None, MODE_FAILED
        return scores_df, _mode_from_scores(scores_df, geoip_available)
    except Exception as e:
        logger.warning("Pipeline failed: %s", e, exc_info=True)
        return None, MODE_FAILED


def _run_carried_over_pipeline(
    provider: RedpandaProvider, indexer_urls: Dict[str, str]
) -> tuple[Optional[pd.DataFrame], str]:
    """For a run that measured nothing: today's prices with the quality, stake and fees an
    earlier run measured, taken from the scores iisa holds where they are recent enough.

    Returns (None, MODE_FAILED) when nothing can be carried over, so the caller falls back to
    degraded scoring.
    """
    logger.info("Carrying over what an earlier run measured, with today's prices")
    try:
        carried = carried_over(provider.held_scores(), indexer_urls, utc_now())
        if carried.empty:
            logger.warning("iisa holds nothing recent enough to carry over")
            return None, MODE_FAILED
        scores_df = compute_carried_over_scores(indexer_urls, carried)
    except Exception as e:
        logger.exception("Carrying scores over failed: %s", e)
        return None, MODE_FAILED

    if scores_df.empty:
        return None, MODE_FAILED
    return scores_df, MODE_CARRIED_OVER


def _run_degraded_pipeline(indexer_urls: Dict[str, str]) -> tuple[Optional[pd.DataFrame], str]:
    """Fallback scoring: equal quality metrics plus real pricing, no Redpanda data needed."""
    logger.info("Running degraded scoring (equal quality + real pricing)")
    try:
        scores_df = compute_degraded_scores(indexer_urls)
    except Exception as e:
        logger.exception("Degraded scoring also failed: %s", e)
        return None, MODE_FAILED

    if scores_df is None or scores_df.empty:
        return None, MODE_FAILED
    return scores_df, MODE_DEGRADED


def _push_scores(provider: RedpandaProvider, scores_df: pd.DataFrame) -> bool:
    """Push the scores to iisa; return False instead of raising when the push fails.

    A push failure (auth, validation, or retry exhaustion) must not escape run accounting:
    the caller marks the run failed and exits non-zero so the CronJob's
    failedJobsHistoryLimit captures it.
    """
    try:
        provider.write_scores(scores_df)
    except IISAPushError as e:
        logger.error("Failed to push scores to iisa: %s", e)
        return False
    return True


def _warn_about_mode(mode: str) -> None:
    """Tell operators when a run published anything less than full scores."""
    if mode == MODE_PARTIAL:
        logger.warning(
            "Scoring ran without GeoIP — latency scores are neutral (0.5). "
            "Install MaxMind GeoLite2 databases for full scoring."
        )
    elif mode == MODE_CARRIED_OVER:
        logger.warning(
            "Scoring carried over — full pipeline unavailable, pushed today's prices with the "
            "quality, stake and fees an earlier run measured."
        )
    elif mode == MODE_DEGRADED:
        logger.warning("Scoring degraded — full pipeline unavailable, pushed real pricing only.")
    elif mode == MODE_FAILED:
        logger.error("Scoring failed")


def _warn_about_missing_stake(
    scores_df: Optional[pd.DataFrame], mode: str, run_started: datetime
) -> None:
    """Tell operators when measured scores went out with stake an earlier run read, or with no
    stake for any indexer, both of which happen when this run could not read stake.
    """
    if scores_df is None or mode not in (MODE_FULL, MODE_PARTIAL):
        return
    stake = scores_df.get("last_known_slashable_stake")
    if stake is None or stake.empty:
        return
    no_read_times = pd.Series(None, index=scores_df.index, dtype=object)
    read_at = pd.to_datetime(scores_df.get(STAKE_READ_AT, no_read_times), utc=True, errors="coerce")
    reused = read_at < run_started
    if reused.any():
        oldest = read_at[reused].min()
        logger.warning(
            "Stake read at %s (%.0f hours before this run) reused for %d indexer(s), as this run "
            "could not read it",
            oldest.isoformat(),
            (run_started - oldest).total_seconds() / 3600,
            int(reused.sum()),
        )
    elif stake.isna().all():
        logger.warning(
            "Scores pushed without stake data: no indexer has stake or query fees in this "
            "run, so stake-to-fees scores every indexer the same until a run reads stake."
        )


def _warn_about_carried_over(
    scores_df: Optional[pd.DataFrame], mode: str, listed_today: int, run_started: datetime
) -> None:
    """Tell operators how old the carried-over measurements are and how many of today's
    indexers went out with them; the rest have no score until a run measures them.
    """
    if scores_df is None or mode != MODE_CARRIED_OVER:
        return
    oldest = pd.to_datetime(scores_df[QUALITY_MEASURED_AT], utc=True).min()
    logger.warning(
        "Quality measured at %s (%.0f hours before this run) carried over for %d of today's %d "
        "indexer(s), as this run could measure nothing; the rest are left out",
        oldest.isoformat(),
        (run_started - oldest).total_seconds() / 3600,
        len(scores_df),
        listed_today,
    )


def run_scoring() -> bool:
    """Run one scoring cycle. Returns True on success."""
    pipeline_start = time.time()
    run_started = utc_now()
    logger.info("Starting score computation")

    # Seed RNGs for deterministic scoring given the same input data.
    # Set SCORING_SEED to replay a previous run's exact sampling.
    seed = int(os.environ.get("SCORING_SEED", date.today().strftime("%Y%m%d")))
    random.seed(seed)
    logger.info("RNG seed: %d", seed)

    geoip_available = validate_geoip_databases()
    if not geoip_available:
        logger.warning("GeoIP databases unavailable, latency scores will be neutral")

    provider = RedpandaProvider()

    indexer_urls = _discover_indexer_urls()
    if indexer_urls is None:
        scores_df, mode = None, MODE_FAILED
    else:
        scores_df, mode = _run_full_pipeline(provider, geoip_available, seed, indexer_urls)
        if scores_df is None:
            scores_df, mode = _run_carried_over_pipeline(provider, indexer_urls)
        if scores_df is None:
            scores_df, mode = _run_degraded_pipeline(indexer_urls)

    elapsed = time.time() - pipeline_start
    success = scores_df is not None

    if scores_df is not None and not _push_scores(provider, scores_df):
        success = False
        mode = MODE_FAILED

    _warn_about_mode(mode)
    _warn_about_missing_stake(scores_df, mode, run_started)
    _warn_about_carried_over(scores_df, mode, len(indexer_urls or {}), run_started)

    logger.info(
        "Scoring complete: mode=%s, indexers=%d, elapsed=%.1fs, peak_memory=%.0fMB",
        mode,
        len(scores_df) if scores_df is not None else 0,
        elapsed,
        get_peak_memory_mb(),
    )

    return success


def main() -> int:
    """Main entry point for the score computation cronjob."""
    logger.info("Score computation cronjob starting")

    if IISA_API_URL:
        logger.info("IISA push target: %s", IISA_API_URL)

    if IISA_REQUIRE_PUSH_TOKEN and not IISA_PUSH_TOKEN:
        logger.critical(
            "IISA_REQUIRE_PUSH_TOKEN is true but IISA_PUSH_TOKEN is unset; "
            "refusing to run. Provision the iisa-push-token Secret or "
            "set IISA_REQUIRE_PUSH_TOKEN=false for local development."
        )
        return 2

    try:
        validate_configuration()
    except ConfigurationError:
        return 1

    success = run_scoring()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
