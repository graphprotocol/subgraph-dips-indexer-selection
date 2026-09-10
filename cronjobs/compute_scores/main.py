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
from datetime import date, timedelta
from typing import Optional

import pandas as pd
from iisa_client import IISAPushError, get_push_token
from processing import compute_all_scores, compute_degraded_scores, validate_geoip_databases
from redpanda import RedpandaProvider

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

    if not IISA_API_URL:
        errors.append("IISA_API_URL is required")

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


def _run_full_pipeline(
    provider: RedpandaProvider, geoip_available: bool, seed: int
) -> tuple[Optional[pd.DataFrame], str]:
    """Run compute_all_scores over the last NUM_DAYS; return (scores, mode).

    Returns (None, MODE_FAILED) when the pipeline raises or produces no rows, so the
    caller can fall back to degraded scoring. compute_all_scores handles both the full
    and the partial (no GeoIP) modes itself.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=NUM_DAYS)
    start_ts = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    mode_label = "full" if geoip_available else "partial (no GeoIP)"
    logger.info("Attempting %s pipeline for %s to %s", mode_label, start_date, end_date)
    try:
        scores_df = compute_all_scores(
            provider=provider,
            start_date=start_date,
            start_ts=start_ts,
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
        logger.warning("Pipeline failed: %s", e)
        return None, MODE_FAILED


def _run_degraded_pipeline() -> tuple[Optional[pd.DataFrame], str]:
    """Fallback scoring: equal quality metrics plus real pricing, no Redpanda data needed."""
    logger.info("Running degraded scoring (equal quality + real pricing)")
    try:
        scores_df = compute_degraded_scores(GRAPH_NETWORK_SUBGRAPH_URL)
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
    elif mode == MODE_DEGRADED:
        logger.warning("Scoring degraded — full pipeline unavailable, pushed real pricing only.")
    elif mode == MODE_FAILED:
        logger.error("Scoring failed")


def run_scoring() -> bool:
    """Run one scoring cycle. Returns True on success."""
    pipeline_start = time.time()
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

    scores_df, mode = _run_full_pipeline(provider, geoip_available, seed)
    if scores_df is None:
        scores_df, mode = _run_degraded_pipeline()

    elapsed = time.time() - pipeline_start
    success = scores_df is not None

    if scores_df is not None and not _push_scores(provider, scores_df):
        success = False
        mode = MODE_FAILED

    _warn_about_mode(mode)

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
