"""
IISA HTTP API - FastAPI endpoints for the Indexing Indexer Selection Algorithm.

This module exposes the indexer-selection HTTP endpoints the dipper-iisa Rust client calls.

Endpoints:
- GET /health - Health check, reports if data is loaded
- POST /scores - Push computed indexer scores from the cronjob (bearer-auth)
- GET /scores - Return the current scores snapshot (bearer-auth)
- GET /scores/status - Return last computed_at; lets the cronjob skip a redundant run (bearer-auth)
- GET /scores/weighted - Bulk weighted scores for every loaded indexer (bearer-auth)
- GET /dips-indexers - Indexers selection would pick for a required ?chain (bearer-auth)
- POST /sync-status - Push sync-status snapshot from the fetcher (bearer-auth)
- POST /get-score - Return weighted score and components for one indexer (bearer-auth)
- POST /select-indexers - Select optimal indexers for a deployment
"""

import hmac
import json
import logging
import os
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Optional, cast

import pandas as pd
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from .indexer_selection import EthAddressStr, IndexerSelector, IpfsHashStr
from .score_loader import (
    STALE_SCORES_CRITICAL_HOURS,
    DataManager,
    FileScoreLoader,
    ScoresPayloadError,
)
from .sync_status_loader import SyncStatusData

__all__ = ["app", "Settings", "get_settings"]


# =============================================================================
# Configuration
# =============================================================================


class Settings(BaseSettings):
    """
    Service configuration loaded from environment variables.

    All settings are prefixed with IISA_ in environment variables.
    For example, IISA_HOST sets the host field.
    """

    model_config = SettingsConfigDict(
        env_prefix="IISA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Service configuration
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"
    sync_status_file_path: str = "/app/scores/sync_status.json"
    sync_status_staleness_hours: float = 6.0
    # Bearer token required on every endpoint except /health and /select-indexers.
    # When unset, those endpoints accept unauthenticated requests and a WARNING
    # is logged at startup — local dev convenience only.
    push_token: Optional[str] = None
    # When true, startup fails hard if push_token is unset. Set in k8s so a
    # misconfigured Secret is caught at rollout rather than leaving production
    # iisa accepting unauthenticated pushes. Default off for compose/local dev.
    require_push_token: bool = False


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Settings are loaded once and cached for the lifetime of the process.
    """
    return Settings()


# =============================================================================
# Request/Response Models
# =============================================================================


class SelectionRequest(BaseModel):
    """
    Request body for indexer selection endpoint.

    Matches the SelectionRequest struct in the Rust HTTP client.
    """

    deployment_id: str
    existing_indexers: Optional[list[str]] = None
    pending_agreements: Optional[dict[str, list[str]]] = None
    num_candidates: int  # Target group size (required)
    blocklist: Optional[list[str]] = None
    declined_indexers: Optional[dict[str, list[str]]] = None
    chain_id: Optional[str] = None  # e.g., "arbitrum-one"
    max_grt_per_30_days: Optional[float] = None  # e.g., 4500.0
    optimistic_dips_fees: Optional[dict[str, float]] = None  # indexer address -> GRT per 30 days


class SelectedIndexer(BaseModel):
    """Indexer entry in the selection response, including pricing info."""

    id: str
    min_grt_per_30_days: Optional[float] = None
    min_grt_per_billion_entities_per_30_days: Optional[float] = None


class SelectionResponse(BaseModel):
    """Response for /select-indexers: the SHOULD-be-assigned indexer set.

    Determined entirely by the currently loaded scores plus the request
    inputs, so a new /scores push between calls can change the response.
    Callers diff against their actual current state to compute adds/cancels.
    """

    deployment_id: str
    indexers: list[SelectedIndexer]


class HealthResponse(BaseModel):
    """
    Response for the /health endpoint.
    """

    status: str
    data_loaded: bool
    sync_status_loaded: bool = False
    # When the currently loaded scores were computed, and how old they are.
    # status flips to "degraded" once the age crosses the critical threshold,
    # which usually means the daily score push has stopped arriving.
    computed_at: Optional[str] = None
    scores_age_hours: Optional[float] = None


class ScoreRequest(BaseModel):
    """
    Request body for the /get-score endpoint.
    """

    indexer_id: str


class ScoreResponse(BaseModel):
    """
    Response for the /get-score endpoint.

    Returns the weighted score and component scores for an indexer.
    """

    indexer_id: str
    weighted_score: Optional[float] = None
    components: Optional[dict[str, float]] = None
    found: bool


class ScoresStatusResponse(BaseModel):
    """
    Response for the GET /scores/status endpoint.

    Used by the cronjob to decide whether today's scores have already been
    computed and pushed — lets the job skip a redundant run.
    """

    computed_at: Optional[str] = None
    rows: int = 0


class ScoresAcceptedResponse(BaseModel):
    """Response returned by POST /scores on success."""

    status: str
    rows: int


class ScoresSnapshotResponse(BaseModel):
    """Response for GET /scores: full snapshot of loaded scored indexers.

    Intended for ops debugging, external monitoring, and local-network test
    harnesses. The shape mirrors the POST /scores payload (list of records)
    so the round-trip is symmetric.
    """

    computed_at: Optional[str] = None
    count: int = 0
    scores: list[dict[str, Any]] = []


class WeightedScoreEntry(BaseModel):
    """One indexer's weighted aggregate plus the component scores feeding it."""

    indexer: str
    weighted_score: Optional[float] = None
    components: dict[str, float] = {}


class WeightedScoresResponse(BaseModel):
    """Response for GET /scores/weighted: bulk weighted-scores snapshot.

    Same per-row shape as POST /get-score but for every loaded indexer in
    a single call. `computed_at` mirrors the underlying push so callers
    can detect stale data without a second round-trip.
    """

    computed_at: Optional[str] = None
    count: int = 0
    scores: list[WeightedScoreEntry] = []


class DipsIndexersResponse(BaseModel):
    """Response for GET /dips-indexers: the indexers selection would pick for the
    requested chain — answered their probe, support it, priced it. ``computed_at``
    mirrors the push so callers can reject a stale snapshot. Example::

        {"computed_at": "2026-06-23T09:00:00+00:00", "count": 2, "indexers": ["0xaa", "0xbb"]}
    """

    computed_at: Optional[str] = None
    count: int = 0
    indexers: list[str] = []


class SyncStatusAcceptedResponse(BaseModel):
    """Response returned by POST /sync-status on success."""

    status: str
    indexers: int


# =============================================================================
# Service State
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("iisa-service")


class IISAState:
    """
    Holds the IISA service state including initialized providers and cached data.
    """

    def __init__(self) -> None:
        self.settings: Optional[Settings] = None
        self.data_manager: Optional[DataManager] = None
        self._history: Optional[pd.DataFrame] = None
        self._sync_status: Optional["SyncStatusData"] = None
        self._initialized: bool = False

    def initialize(self, settings: Settings) -> bool:
        """
        Initialize the IISA providers.

        Returns True if initialization succeeded, False if we should fall back
        to random selection mode.
        """
        self.settings = settings

        try:
            logger.info("Initializing IISA providers...")

            provider = FileScoreLoader()
            logger.info("Score source: cache file (iisa-owned RWO PVC)")

            self.data_manager = DataManager(provider)

            logger.info("IISA providers initialized successfully")
            self._initialized = True
            return True

        except Exception as e:
            logger.warning("Failed to initialize IISA providers: %s", e)
            logger.warning("Service will operate in random selection fallback mode")
            self._initialized = False
            return False

    def refresh_data(self) -> bool:
        """Load pre-computed indexer scores from the cache file on disk.

        Called on startup to recover the last successful push. FileScoreLoader
        handles graceful empty fallback on a cache miss. Returns True on
        success, False otherwise.
        """
        if not self._initialized or self.data_manager is None:
            logger.warning("Cannot refresh data: DataManager not initialized")
            return False

        try:
            logger.info("Loading pre-computed indexer scores from cache...")
            success = self.data_manager.load_scores()

            if success:
                self._history = self.data_manager.get_data()
                if self._history is not None:
                    logger.info("Scores loaded successfully: %d indexers", len(self._history))
                    return True

            logger.warning("Failed to load scores")
            return False

        except Exception as e:
            logger.error("Failed to load scores: %s", e)
            return False

    def load_scores_from_records(self, records: list[dict[str, Any]]) -> int:
        """Accept a pushed scores payload from the cronjob; return row count.

        Dry-run-then-commit: parse, transform a local copy, then write cache,
        then commit in-memory. Transform failures raise before any disk I/O,
        keeping the cache file valid. Raises on parse/transform/write failure.
        """
        if self.data_manager is None:
            raise RuntimeError("DataManager not initialized")

        scores_path = self.data_manager.scores_file_path
        if scores_path is None:
            raise RuntimeError("DataManager has no file-backed provider; cannot persist push")

        scores_df = pd.DataFrame(records)
        computed_at = _extract_computed_at(scores_df)

        # Dry-run: transform_scores_df is a pure function that raises on
        # failure without touching state. If the payload is schema-invalid
        # (including empty), this raises and the next two lines never execute.
        transformed = self.data_manager.transform_scores_df(scores_df)

        # Transform succeeded — the payload is known to be loadable.
        # Safe to write to disk; a restart will now reload successfully.
        _atomic_write_json(scores_path, records)

        # Commit the already-validated transformed frame to in-memory state.
        self.data_manager.commit_scores(transformed, computed_at)
        self._history = self.data_manager.get_data()
        return len(self._history) if self._history is not None else 0

    def refresh_sync_status(self) -> bool:
        """Load sync status from the cache file. Returns True on success."""
        if self.settings is None:
            return False

        from .sync_status_loader import SyncStatusLoader

        loader = SyncStatusLoader(self.settings.sync_status_file_path)
        data = loader.load(self.settings.sync_status_staleness_hours)
        if data is not None:
            self._sync_status = data
            return True
        return False

    def load_sync_status_from_dict(self, raw: dict[str, Any]) -> int:
        """Accept a pushed sync-status payload.

        Parse-first ordering: SyncStatusData is constructed in memory before
        touching disk; once parsing succeeds, the raw payload is written
        atomically. Returns the indexer count that passed staleness filter.
        """
        if self.settings is None:
            raise RuntimeError("Settings not initialized")

        data = SyncStatusData(raw, self.settings.sync_status_staleness_hours)

        _atomic_write_json(self.settings.sync_status_file_path, raw)

        self._sync_status = data
        return data.total_indexers

    @property
    def sync_status(self):
        """Get the cached SyncStatusData, or None."""
        return getattr(self, "_sync_status", None)

    @property
    def history(self) -> Optional[pd.DataFrame]:
        """Get the cached history DataFrame."""
        return self._history

    @property
    def is_ready(self) -> bool:
        """Check if the service has data loaded and is ready."""
        return self._history is not None and not self._history.empty


def _atomic_write_json(path: str, payload: Any) -> None:
    """Write JSON to `path` atomically via tmp + os.replace.

    The temporary file lives in the same directory so the rename is within
    a single filesystem. Callers must hold no other reference to `path`
    during the replace.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(payload, f, default=str)
    os.replace(tmp_path, path)


def _extract_computed_at(scores_df: pd.DataFrame) -> Optional[datetime]:
    """Extract the first record's computed_at as a UTC datetime, or None.

    errors="coerce" turns any unparseable value into NaT, so the pd.isna
    check below captures all parse failures without an explicit except.
    """
    if "computed_at" not in scores_df.columns or scores_df.empty:
        return None
    series = pd.to_datetime(scores_df["computed_at"], utc=True, errors="coerce")
    first = series.iloc[0]
    if pd.isna(first):
        return None
    return first.to_pydatetime()


def _format_computed_at(computed_at: Optional[datetime]) -> Optional[str]:
    """Serialize a snapshot's ``computed_at`` as a UTC-aware ISO-8601 string, or
    ``None``. A naive datetime is assumed to be UTC. Shared by every endpoint that
    reports snapshot freshness so their timestamp format cannot drift apart.
    """
    if computed_at is None:
        return None
    ts = computed_at if computed_at.tzinfo else computed_at.replace(tzinfo=timezone.utc)
    return ts.isoformat()


def _parse_supported_networks(networks_json: Any) -> list[str]:
    """Parse a ``dips_supported_networks`` cell into a list of chain ids; empty,
    missing, or malformed values yield ``[]``. Examples::

        '["arbitrum-one","mainnet"]'   -> ["arbitrum-one", "mainnet"]
        "[]" / "" / None / NaN / "x{"  -> []
    """
    try:
        networks = json.loads(networks_json) if isinstance(networks_json, str) else []
    except (json.JSONDecodeError, TypeError):
        return []
    return networks if isinstance(networks, list) else []


def _supports_chain(networks_json: Any, chain_id: str) -> bool:
    """True when ``chain_id`` is in the indexer's supported-networks list.
    Examples::

        _supports_chain('["arbitrum-one"]', "arbitrum-one") -> True
        _supports_chain(None, "arbitrum-one")               -> False
    """
    return chain_id in _parse_supported_networks(networks_json)


def _require_push_token(authorization: Optional[str]) -> None:
    """Validate the bearer token against IISA_PUSH_TOKEN.

    When IISA_PUSH_TOKEN is unset, auth is off (local dev convenience). Uses
    hmac.compare_digest to dodge timing oracles. "Bearer" prefix matched
    case-insensitively per RFC 6750 §2.1; the token itself is case-sensitive.
    """
    expected = get_settings().push_token
    if not expected:
        return
    if not authorization or authorization[:7].lower() != "bearer ":
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header")
    provided = authorization[7:].strip()
    if not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Invalid bearer token")


# Global state
_state = IISAState()


# =============================================================================
# FastAPI Application
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan: load settings, init loader + DataManager, recover the
    last cached scores and sync-status from disk, warn if IISA_PUSH_TOKEN is unset.

    The cronjob POSTs new data directly — no background polling. Restarts
    recover state from the cache mount written by previous successful POSTs.
    """
    global _state

    settings = get_settings()

    # Set log level from settings
    logging.getLogger().setLevel(getattr(logging, settings.log_level))
    logger.setLevel(getattr(logging, settings.log_level))

    logger.info("Starting IISA service...")

    if settings.require_push_token and not settings.push_token:
        logger.critical(
            "IISA_REQUIRE_PUSH_TOKEN is true but IISA_PUSH_TOKEN is unset; "
            "refusing to start. Provision the iisa-push-token Secret or "
            "set IISA_REQUIRE_PUSH_TOKEN=false for local development."
        )
        raise RuntimeError("IISA_PUSH_TOKEN is required but unset")

    if not settings.push_token:
        logger.warning(
            "IISA_PUSH_TOKEN is not set; push endpoints will accept unauthenticated "
            "requests. This is acceptable in local development only."
        )

    # Initialize providers
    if not _state.initialize(settings):
        logger.error("IISA initialization failed - cannot start service")
        raise RuntimeError("Failed to initialize IISA providers")

    # Recover last cached scores from the RWO cache mount. Missing/empty cache
    # is acceptable — the service comes up in fallback mode and the next push
    # from the cronjob will populate it.
    logger.info("Attempting to recover cached scores from disk...")
    if _state.refresh_data():
        logger.info("Recovered cached scores on startup")
    else:
        logger.warning(
            "No cached scores found on startup; serving in random-selection "
            "fallback mode until the first POST /scores arrives."
        )

    # Sync status: also optional on startup
    if _state.refresh_sync_status():
        logger.info("Recovered cached sync status on startup")
    else:
        logger.info("No cached sync status on startup (optional)")

    logger.info("IISA service ready")

    yield

    logger.info("Shutting down IISA service...")


app = FastAPI(
    title="IISA Service",
    description="Indexing Indexer Selection Algorithm for The Graph DIPs service",
    version="0.1.0",
    lifespan=lifespan,
)


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Report data-loaded state and how old the loaded scores are.

    status flips to "degraded" past the critical staleness threshold. Always
    200 (all three k8s probes hit this path; a restart reloads the same stale
    file, so a non-200 would crashloop without fixing anything).
    """
    status = "healthy"
    computed_at_iso: Optional[str] = None
    age_hours: Optional[float] = None

    data_manager = _state.data_manager
    if data_manager is not None:
        computed_at = data_manager.snapshot.computed_at
        # isinstance guards against a leaked mock in tests and a None snapshot.
        if isinstance(computed_at, datetime):
            ts = computed_at if computed_at.tzinfo else computed_at.replace(tzinfo=timezone.utc)
            computed_at_iso = ts.isoformat()
            age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if age_hours > STALE_SCORES_CRITICAL_HOURS:
                status = "degraded"

    return HealthResponse(
        status=status,
        data_loaded=_state.is_ready,
        sync_status_loaded=_state.sync_status is not None,
        computed_at=computed_at_iso,
        scores_age_hours=round(age_hours, 1) if age_hours is not None else None,
    )


@app.post("/scores", response_model=ScoresAcceptedResponse)
def push_scores(
    payload: list[dict[str, Any]],
    authorization: Optional[str] = Header(None),
) -> ScoresAcceptedResponse:
    """Accept a pushed scores snapshot from the cronjob.

    Order: token → body → DataFrame → atomic write to cache PVC → in-memory
    update → row count. Parse-before-disk means a malformed payload 422s
    without mutating disk. Sync def so the blocking write hits the threadpool.
    """
    _require_push_token(authorization)

    if not payload:
        raise HTTPException(
            status_code=422,
            detail="scores payload must be a non-empty list of records",
        )

    if not _state._initialized:
        raise HTTPException(
            status_code=503,
            detail="IISA providers not initialized",
        )

    try:
        rows = _state.load_scores_from_records(payload)
    except ScoresPayloadError as e:
        # Wrong-shape body: the caller's problem, so 422 not 500.
        logger.warning("Rejected pushed scores: %s", e)
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception:
        logger.exception("Failed to accept pushed scores")
        raise HTTPException(status_code=500, detail="Failed to accept scores")

    logger.info("Accepted pushed scores: %d rows", rows)
    return ScoresAcceptedResponse(status="success", rows=rows)


@app.get("/scores/status", response_model=ScoresStatusResponse)
def scores_status(
    authorization: Optional[str] = Header(None),
) -> ScoresStatusResponse:
    """Report computed_at of the currently loaded scores.

    The cronjob GETs this before running so it can skip recomputation when
    today's scores are already pushed. Sync def for consistency with the
    other push endpoints — no awaitable work in the body.
    """
    _require_push_token(authorization)

    if _state.data_manager is None:
        return ScoresStatusResponse(computed_at=None, rows=0)

    snap = _state.data_manager.snapshot
    computed_at = _format_computed_at(snap.computed_at)

    rows = len(snap.data) if snap.data is not None else 0
    return ScoresStatusResponse(computed_at=computed_at, rows=rows)


@app.get("/scores", response_model=ScoresSnapshotResponse)
def scores_snapshot(
    authorization: Optional[str] = Header(None),
) -> ScoresSnapshotResponse:
    """Return the loaded scores snapshot as a list of records.

    Symmetric with POST /scores — what the cronjob pushed is what's served
    here. For ops debugging and external monitoring, not the selection hot
    path. Sync def so the DataFrame→JSON conversion runs on the threadpool.
    """
    _require_push_token(authorization)

    if _state.data_manager is None:
        return ScoresSnapshotResponse(computed_at=None, count=0, scores=[])

    snap = _state.data_manager.snapshot

    computed_at = _format_computed_at(snap.computed_at)

    if snap.data is None:
        return ScoresSnapshotResponse(computed_at=computed_at, count=0, scores=[])

    # Round-trip via to_json/loads so NaN/NaT serialize as null and
    # pandas Timestamp columns serialize to ISO-8601 — same pattern the
    # cronjob uses when pushing, keeping the round-trip symmetric.
    payload_json = snap.data.to_json(orient="records", date_format="iso", date_unit="s")
    assert payload_json is not None
    scores = cast(list[dict[str, Any]], json.loads(payload_json))

    return ScoresSnapshotResponse(
        computed_at=computed_at,
        count=len(scores),
        scores=scores,
    )


@app.get("/scores/weighted", response_model=WeightedScoresResponse)
def scores_weighted(
    authorization: Optional[str] = Header(None),
) -> WeightedScoresResponse:
    """Bulk weighted-score snapshot — same shape as /get-score, every indexer.

    Single pass across the loaded snapshot — saves N round-trips to /get-score.
    Selection can pass `max_grt_per_30_days` as a `price_ceiling`; this endpoint
    cannot, so prices are normalised against the observed max instead.
    """
    _require_push_token(authorization)

    if _state.data_manager is None:
        return WeightedScoresResponse(computed_at=None, count=0, scores=[])

    snap = _state.data_manager.snapshot

    computed_at = _format_computed_at(snap.computed_at)

    if snap.data is None or snap.data.empty:
        return WeightedScoresResponse(computed_at=computed_at, count=0, scores=[])

    from .indexer_selection import (
        DEFAULT_WEIGHTS,
        _calculate_weighted_scores,
        _normalize_metrics,
    )

    normalized = _normalize_metrics(snap.data.copy())

    component_keys = [
        ("norm_lat_lin_reg_coefficient", "latency"),
        ("norm_uptime_score", "uptime"),
        ("norm_success_rate", "success_rate"),
        ("norm_stake_to_fees", "stake_to_fees"),
        ("norm_base_price_per_epoch", "base_price"),
        ("norm_price_per_entity", "price_per_entity"),
    ]

    # Score every indexer in one vectorised pass. _normalize_metrics fills all
    # norm_ columns, so the zero-weight case can't arise here; degrade to
    # unscored rather than 500 if it somehow does.
    try:
        weighted_scores = _calculate_weighted_scores(normalized, DEFAULT_WEIGHTS).to_numpy()
    except ValueError as e:
        logger.warning("bulk weighted scoring failed: %s", e)
        weighted_scores = pd.Series(float("nan"), index=normalized.index).to_numpy()

    entries: list[WeightedScoreEntry] = []
    for position, (_, row) in enumerate(normalized.iterrows()):
        indexer_id = row.get("indexer")
        if indexer_id is None or (isinstance(indexer_id, float) and pd.isna(indexer_id)):
            logger.debug("skipping row with missing indexer field")
            continue

        score = weighted_scores[position]
        weighted = float(score) if pd.notna(score) else None

        components = {
            name: float(row[col])
            for col, name in component_keys
            if col in row.index and pd.notna(row[col])
        }
        entries.append(
            WeightedScoreEntry(
                indexer=str(indexer_id),
                weighted_score=weighted,
                components=components,
            )
        )

    return WeightedScoresResponse(
        computed_at=computed_at,
        count=len(entries),
        scores=entries,
    )


@app.get("/dips-indexers", response_model=DipsIndexersResponse)
def dips_indexers(
    chain: str,
    max_grt_per_30_days: Optional[float] = None,
    authorization: Optional[str] = Header(None),
) -> DipsIndexersResponse:
    """Indexers selection would pick for ``chain``: answered their DIPs probe, support
    the chain, and priced it within the optional ``max_grt_per_30_days`` ceiling (omit
    to ignore price). ``chain`` is required; omitting it is a 422. Example::

        GET /dips-indexers?chain=arbitrum-one&max_grt_per_30_days=4500 -> count 2
    """
    _require_push_token(authorization)

    if _state.data_manager is None:
        return DipsIndexersResponse(computed_at=None, count=0, indexers=[])

    snap = _state.data_manager.snapshot

    computed_at = _format_computed_at(snap.computed_at)

    if snap.data is None or snap.data.empty or "dips_supported_networks" not in snap.data.columns:
        return DipsIndexersResponse(computed_at=computed_at, count=0, indexers=[])

    # Reuse the selection path's test so this endpoint and selection can't disagree:
    # an indexer accepts DIPs for a chain only if it answered its probe, supports the
    # chain, and priced it within the optional ceiling (None skips the ceiling).
    matched, _ = _filter_by_price(snap.data, chain, max_grt_per_30_days)

    indexers: list[str] = []
    for value in matched.get("indexer", pd.Series(dtype=object)):
        # Skip rows missing an indexer id; mirrors the guard in /scores/weighted.
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        indexers.append(str(value))

    return DipsIndexersResponse(
        computed_at=computed_at,
        count=len(indexers),
        indexers=indexers,
    )


@app.post("/sync-status", response_model=SyncStatusAcceptedResponse)
def push_sync_status(
    payload: dict[str, Any],
    authorization: Optional[str] = Header(None),
) -> SyncStatusAcceptedResponse:
    """Accept a pushed sync-status snapshot from sync_status_fetcher.

    Empty payloads mean "no indexers currently synced" — accepted. Sync def
    so the blocking _atomic_write_json runs on the threadpool rather than
    the event loop.
    """
    _require_push_token(authorization)

    if _state.settings is None:
        raise HTTPException(
            status_code=503,
            detail="IISA service not initialized",
        )

    try:
        indexer_count = _state.load_sync_status_from_dict(payload)
    except Exception:
        logger.exception("Failed to accept pushed sync status")
        raise HTTPException(status_code=500, detail="Failed to accept sync status")

    logger.info("Accepted pushed sync status: %d indexers", indexer_count)
    return SyncStatusAcceptedResponse(status="success", indexers=indexer_count)


@app.post("/get-score", response_model=ScoreResponse)
async def get_score(
    request: ScoreRequest,
    authorization: Optional[str] = Header(None),
) -> ScoreResponse:
    """Return one indexer's weighted score plus the component scores feeding it.

    Useful for debugging selection decisions and monitoring per-indexer
    performance without running a full selection request.
    """
    _require_push_token(authorization)

    if not _state.is_ready or _state.history is None:
        raise HTTPException(status_code=503, detail="IISA data not loaded")

    from .indexer_selection import (
        DEFAULT_WEIGHTS,
        _calculate_weighted_scores,
        _normalize_metrics,
    )

    # Normalisation is relative (min-max across all indexers), so scoring one
    # row alone is degenerate. Normalise the whole table like /scores/weighted,
    # then pull this indexer's row, so the two endpoints agree.
    indexer_id = request.indexer_id.lower()
    normalized = _normalize_metrics(_state.history.copy())
    match = normalized[normalized["indexer"] == indexer_id]

    if match.empty:
        return ScoreResponse(indexer_id=request.indexer_id, found=False)

    row = match.iloc[0]
    component_keys = [
        ("norm_lat_lin_reg_coefficient", "latency"),
        ("norm_uptime_score", "uptime"),
        ("norm_success_rate", "success_rate"),
        ("norm_stake_to_fees", "stake_to_fees"),
        ("norm_base_price_per_epoch", "base_price"),
        ("norm_price_per_entity", "price_per_entity"),
    ]
    components = {
        name: float(row[col])
        for col, name in component_keys
        if col in row.index and pd.notna(row[col])
    }

    # _normalize_metrics fills every norm_ column, so a zero weight total can't
    # arise here; degrade to unscored rather than 500 if it somehow does.
    try:
        weighted_score: Optional[float] = float(
            _calculate_weighted_scores(match, DEFAULT_WEIGHTS).iloc[0]
        )
    except ValueError as e:
        logger.warning("weighted scoring failed for %s: %s", indexer_id, e)
        weighted_score = None

    return ScoreResponse(
        indexer_id=request.indexer_id,
        weighted_score=weighted_score,
        components=components,
        found=True,
    )


@app.post("/select-indexers", response_model=SelectionResponse)
async def select_indexers(request: SelectionRequest) -> SelectionResponse:
    """Select N indexers for a deployment via weighted scoring.

    Returns the SHOULD-be-assigned set; caller diffs against current state.
    Picks the top N by weighted aggregate, preferring >1 unique org / location
    when N > 1 (best-effort). Pass existing_indexers=[] for a fresh selection.
    """
    if not _state.is_ready or _state.history is None:
        raise HTTPException(status_code=503, detail="IISA data not loaded")

    if request.num_candidates <= 0:
        return SelectionResponse(deployment_id=request.deployment_id, indexers=[])

    logger.info(
        "select-indexers request: deployment=%s chain=%s num_candidates=%d "
        "existing=%d blocked=%d budget=%s",
        request.deployment_id,
        request.chain_id,
        request.num_candidates,
        len(request.existing_indexers or []),
        len(request.blocklist or []),
        f"{request.max_grt_per_30_days} GRT/30d" if request.max_grt_per_30_days else "none",
    )

    try:
        response = _select_with_processor(request)
        indexer_ids = [i.id for i in response.indexers]
        logger.info(
            "Selected %d indexers for deployment %s: %s",
            len(response.indexers),
            request.deployment_id,
            indexer_ids,
        )
        return response
    except Exception as e:
        logger.exception("Selection failed for deployment %s", request.deployment_id)
        raise HTTPException(status_code=500, detail=f"Selection failed: {e}")


def _extract_chain_price(dips_min_grt_json: str, chain_id: str) -> Optional[float]:
    """Extract the price for a specific chain from the JSON price map."""
    try:
        prices = json.loads(dips_min_grt_json) if isinstance(dips_min_grt_json, str) else {}
        val = prices.get(chain_id)
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _keep_with_dips_info(
    df: pd.DataFrame, chain_id: str, max_grt_per_30_days: Optional[float]
) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers that answered their DIP info probe."""
    initial_count = len(df)
    df = df[df["dips_info_available"] == True]  # noqa: E712
    logger.debug("price filter: %d/%d indexers have DIP info", len(df), initial_count)
    if df.empty:
        return df, f"all {initial_count} indexers lack DIP info (dips_info_available=False)"
    return df, ""


def _keep_supporting_chain(
    df: pd.DataFrame, chain_id: str, max_grt_per_30_days: Optional[float]
) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers whose supported networks include the chain."""
    if "dips_supported_networks" not in df.columns:
        return df, ""
    pre_filter = len(df)
    df = df[df["dips_supported_networks"].apply(lambda v: _supports_chain(v, chain_id))]
    logger.debug("price filter: %d/%d indexers support chain '%s'", len(df), pre_filter, chain_id)
    if df.empty:
        return df, f"none of {pre_filter} indexers support chain '{chain_id}'"
    return df, ""


def _keep_with_chain_price(
    df: pd.DataFrame, chain_id: str, max_grt_per_30_days: Optional[float]
) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers that have a price set for the chain."""
    if "dips_min_grt_per_30_days" not in df.columns:
        return df, ""

    def has_chain_price(prices_json: Any) -> bool:
        return _extract_chain_price(prices_json, chain_id) is not None

    pre_filter = len(df)
    df = df[df["dips_min_grt_per_30_days"].apply(has_chain_price)]
    if df.empty:
        return df, f"none of {pre_filter} indexers have pricing configured for chain '{chain_id}'"
    return df, ""


def _keep_within_budget(
    df: pd.DataFrame, chain_id: str, max_grt_per_30_days: Optional[float]
) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers priced at or below max_grt_per_30_days for the chain."""
    if max_grt_per_30_days is None or "dips_min_grt_per_30_days" not in df.columns:
        return df, ""
    max_budget = max_grt_per_30_days

    def within_budget(prices_json: Any) -> bool:
        price = _extract_chain_price(prices_json, chain_id)
        return price is not None and price <= max_budget

    pre_filter = len(df)
    df = df[df["dips_min_grt_per_30_days"].apply(within_budget)]
    logger.debug(
        "price filter: %d/%d indexers within budget of %s GRT/30d for chain '%s'",
        len(df),
        pre_filter,
        max_grt_per_30_days,
        chain_id,
    )
    if df.empty:
        return (
            df,
            f"all {pre_filter} indexers exceed payment "
            f"ceiling of {max_grt_per_30_days} GRT/30d "
            f"for chain '{chain_id}'",
        )
    return df, ""


def _filter_by_price(
    history: pd.DataFrame,
    chain_id: Optional[str],
    max_grt_per_30_days: Optional[float],
) -> tuple[pd.DataFrame, str]:
    """Drop indexers that lack DIP info, don't support the chain, lack a
    chain price, or exceed max_grt_per_30_days for the chain.

    Returns (filtered_df, reason). ``reason`` is empty unless filtering left
    no candidates, in which case it explains why.
    """
    if chain_id is None:
        return history, ""

    df = history.copy()

    # Only filter if we have the DIP info columns
    if "dips_info_available" not in df.columns:
        return df, ""

    steps = (
        _keep_with_dips_info,
        _keep_supporting_chain,
        _keep_with_chain_price,
        _keep_within_budget,
    )
    for step in steps:
        df, reason = step(df, chain_id, max_grt_per_30_days)
        if df.empty:
            return df, reason
    return df, ""


def _enrich_with_chain_prices(
    history: pd.DataFrame,
    chain_id: Optional[str],
) -> pd.DataFrame:
    """
    Add base_price_per_epoch and price_per_entity columns for scoring.

    Extracts the chain-specific price from the JSON fields.
    """
    df = history.copy()

    if chain_id is None or "dips_min_grt_per_30_days" not in df.columns:
        df["base_price_per_epoch"] = 0.0
        df["price_per_entity"] = 0.0
        return df

    def extract_price(prices_json):
        price_str = _extract_chain_price(prices_json, chain_id)
        try:
            return float(price_str) if price_str is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    df["base_price_per_epoch"] = df["dips_min_grt_per_30_days"].apply(extract_price)

    if "dips_min_grt_per_billion_entities_per_30_days" in df.columns:
        df["price_per_entity"] = pd.to_numeric(
            df["dips_min_grt_per_billion_entities_per_30_days"], errors="coerce"
        ).fillna(0.0)
    else:
        df["price_per_entity"] = 0.0

    return df


def _indexer_prices(
    history: pd.DataFrame, idx_id: str, chain_id: Optional[str]
) -> tuple[Optional[float], Optional[float]]:
    """Return (min_grt_per_30_days for the chain, min_grt_per_billion_entities_per_30_days)
    from the indexer's first row in ``history``.

    Both are None without a chain_id or a row; either is None when its column is missing.
    """
    rows = history[history["indexer"] == idx_id]
    if rows.empty or chain_id is None:
        return None, None
    row = rows.iloc[0]

    min_grt = None
    if "dips_min_grt_per_30_days" in row.index:
        min_grt = _extract_chain_price(row["dips_min_grt_per_30_days"], chain_id)

    min_entity = None
    if "dips_min_grt_per_billion_entities_per_30_days" in row.index:
        val = row["dips_min_grt_per_billion_entities_per_30_days"]
        try:
            min_entity = float(val) if val is not None and pd.notna(val) else None
        except (TypeError, ValueError):
            min_entity = None

    return min_grt, min_entity


def _build_selected_indexers(
    indexer_ids: list[str],
    history: pd.DataFrame,
    chain_id: Optional[str],
) -> list[SelectedIndexer]:
    """Build SelectedIndexer entries with pricing info."""
    results = []
    for idx_id in indexer_ids:
        min_grt, min_entity = _indexer_prices(history, idx_id, chain_id)
        results.append(
            SelectedIndexer(
                id=idx_id,
                min_grt_per_30_days=min_grt,
                min_grt_per_billion_entities_per_30_days=min_entity,
            )
        )
    return results


def _price_filtered_candidates(
    history: pd.DataFrame, request: SelectionRequest
) -> Optional[pd.DataFrame]:
    """Apply the request's price constraints to ``history`` before scoring.

    Returns None, after logging why, when no candidate survives the filter.
    """
    filtered_history, filter_reason = _filter_by_price(
        history,
        request.chain_id,
        request.max_grt_per_30_days,
    )

    if filtered_history.empty:
        if filter_reason:
            logger.warning(
                "No indexers available for deployment %s: %s",
                request.deployment_id,
                filter_reason,
            )
        else:
            logger.warning(
                "No indexers available for deployment %s (unknown reason)",
                request.deployment_id,
            )
        return None

    logger.info(
        "deployment=%s proceeding with %d candidates after price filtering (from %d total)",
        request.deployment_id,
        len(filtered_history),
        len(history),
    )
    return filtered_history


def _synced_indexers_for(deployment_id: str) -> set[str]:
    """Look up which indexers are already synced for this deployment."""
    synced_indexers: set[str] = set()
    if _state.sync_status is not None:
        synced_indexers = _state.sync_status.synced_indexers_for(deployment_id)
        if synced_indexers:
            logger.info(
                "deployment=%s %d synced indexers available",
                deployment_id,
                len(synced_indexers),
            )
    return synced_indexers


def _build_selector(
    request: SelectionRequest,
    enriched_history: pd.DataFrame,
    synced_indexers: set[str],
) -> IndexerSelector:
    """Construct the IndexerSelector for this request; it selects on construction."""
    # Build existing_agreements dict from request
    existing_agreements: dict[str, list[str]] = {}
    if request.existing_indexers:
        existing_agreements[request.deployment_id] = request.existing_indexers

    # Build pending_agreements dict - convert to expected format
    pending_agreements: dict[str, list[str]] = request.pending_agreements or {}

    return IndexerSelector(
        history=enriched_history,
        deployment_id=cast(IpfsHashStr, request.deployment_id),
        existing_agreements=cast(dict[IpfsHashStr, list[EthAddressStr]], existing_agreements),
        pending_agreements=cast(dict[IpfsHashStr, list[EthAddressStr]], pending_agreements),
        declined_indexers=cast(
            dict[IpfsHashStr, list[EthAddressStr]],
            request.declined_indexers or {},
        ),
        indexer_denylist=cast(list[EthAddressStr], request.blocklist or []),
        target_size=request.num_candidates,
        optimistic_dips_fees=request.optimistic_dips_fees,
        price_ceiling=request.max_grt_per_30_days,
        synced_indexers=cast(set[EthAddressStr], synced_indexers),
    )


# Normalised metric columns paired with the label each is logged under. The weight
# key for each metric is its column name without the "norm_" prefix, which matches
# the keys in IndexerSelector.weights / DEFAULT_WEIGHTS.
_BREAKDOWN_COMPONENTS = (
    ("norm_stake_to_fees", "stake_to_fees"),
    ("norm_base_price_per_epoch", "base_price"),
    ("norm_lat_lin_reg_coefficient", "latency"),
    ("norm_uptime_score", "uptime"),
    ("norm_success_rate", "success_rate"),
    ("norm_price_per_entity", "price_per_entity"),
)


def _score_breakdown(
    row: pd.Series, weights: Mapping[str, object]
) -> tuple[Optional[float], dict[str, float], dict[str, float], dict[str, float]]:
    """Return (weighted_score, components, weights, contributions) for one scored
    indexer row, rounded for logging. Metrics the row lacks or that carry no
    weight are left out; weighted_score is None when the row has none.
    """
    # Each present metric, paired with its normalised value and its active weight.
    present = [
        (label, float(row[col]), float(cast(float, weights[col[len("norm_") :]])))
        for col, label in _BREAKDOWN_COMPONENTS
        if col in row.index and pd.notna(row[col]) and col[len("norm_") :] in weights
    ]
    components = {label: round(value, 3) for label, value, _ in present}
    present_weights = {label: round(weight, 3) for label, _, weight in present}
    # Each metric's weighted contribution to the score: value * weight
    # renormalised over the weights of the metrics this indexer has, so the
    # contributions sum to the logged score (mirrors _calculate_weighted_scores).
    weight_total = sum(weight for _, _, weight in present)
    contributions = {
        label: round(value * weight / weight_total, 4)
        for label, value, weight in present
        if weight_total > 0
    }
    weighted = (
        round(float(row["weighted_score"]), 4)
        if "weighted_score" in row.index and pd.notna(row["weighted_score"])
        else None
    )
    return weighted, components, present_weights, contributions


def _log_selection_reasoning(processor: IndexerSelector, deployment_id: str) -> None:
    """Log each selected indexer's score breakdown for auditability."""
    if processor.data is None or processor.data.empty or not processor.current_group:
        return

    scored = processor.data[processor.data["indexer"].isin(processor.current_group)]
    for _, row in scored.iterrows():
        weighted, components, weights, contributions = _score_breakdown(row, processor.weights)
        logger.info(
            "selected indexer=%s score=%.4f components=%s weights=%s contributions=%s "
            "deployment=%s",
            row["indexer"],
            weighted if weighted is not None else 0.0,
            components,
            weights,
            contributions,
            deployment_id,
        )


def _select_with_processor(request: SelectionRequest) -> SelectionResponse:
    """Run IndexerSelector and return a SelectionResponse for the deployment.

    The selector weights stake-to-fees, base price, latency, uptime,
    success rate, and price-per-entity; lower is better for prices and
    latency, higher for the rest.
    """
    if _state.history is None:
        return SelectionResponse(deployment_id=request.deployment_id, indexers=[])

    # Filter by price constraints before scoring
    filtered_history = _price_filtered_candidates(_state.history, request)
    if filtered_history is None:
        return SelectionResponse(deployment_id=request.deployment_id, indexers=[])

    # Enrich with chain-specific price columns for normalization
    enriched_history = _enrich_with_chain_prices(filtered_history, request.chain_id)
    synced_indexers = _synced_indexers_for(request.deployment_id)
    processor = _build_selector(request, enriched_history, synced_indexers)
    _log_selection_reasoning(processor, request.deployment_id)

    # Build response with pricing info
    selected = _build_selected_indexers(
        list(processor.current_group),
        enriched_history,
        request.chain_id,
    )

    return SelectionResponse(
        deployment_id=request.deployment_id,
        indexers=selected,
    )


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    logger.info("Starting IISA service on %s:%d", settings.host, settings.port)
    uvicorn.run(
        "iisa.iisa_http_endpoints:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )
