"""IISA HTTP API - FastAPI endpoints for the Indexing Indexer Selection Algorithm, which the
dipper-iisa Rust client calls.

Endpoints ("push token" means IISA_PUSH_TOKEN only, "read token" either it or IISA_READ_TOKEN):
- GET /health - Health check, reports if data is loaded
- POST /scores - Push computed indexer scores from the cronjob (push token)
- GET /scores - Return the current scores snapshot (read token)
- GET /scores/status - Return when the loaded scores were computed, for monitoring (read token)
- GET /scores/weighted - Bulk weighted scores for every loaded indexer (read token)
- GET /dips-indexers - Indexers offering DIPs on a required ?chain (read token)
- POST /sync-status - Push sync-status snapshot from the fetcher (push token)
- POST /freshness - Push 1 minute of freshness outcomes from the prober (push token)
- GET /freshness - Return each indexer's stale mark and freshness counts, by chain (read token)
- POST /get-score - Return weighted score and components for one indexer (read token)
- POST /select-indexers - Select optimal indexers for a deployment
"""

import hmac
import itertools
import json
import logging
import math
import numbers
import os
import threading
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache, partial
from typing import Annotated, Any, Literal, NamedTuple, Optional, Self, TypeGuard, Union, cast

import pandas as pd
from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    WithJsonSchema,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from . import score_columns as cols
from .freshness import (
    HISTORY_DAYS,
    MINUTES_PER_DAY,
    Check,
    Enforcement,
    FreshnessStore,
    load_state,
    minute_index,
    minute_text,
)
from .indexer_selection import EthAddressStr, IndexerSelector, IpfsHashStr
from .score_loader import (
    STALE_SCORES_CRITICAL_HOURS,
    DataManager,
    FileScoreLoader,
    ScoresPayloadError,
)
from .sync_check import MAX_CHECKS_PER_SELECTION, SyncClaimChecker
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
        # A settings error is logged when startup fails, and by default it prints the values it
        # was given, cut short: among them the tokens and the gateway API key.
        hide_input_in_errors=True,
    )

    # Service configuration
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"
    sync_status_file_path: str = "/app/scores/sync_status.json"
    sync_status_staleness_hours: float = 6.0
    freshness_file_path: str = "/app/scores/freshness.json"
    # Chains, comma-separated, on which a stale mark is only recorded and reported and freshness
    # isn't weighed. On every other chain an indexer marked stale isn't picked for new
    # agreements.
    freshness_exempt_chains: Annotated[frozenset[str], NoDecode] = frozenset()
    # How many minutes behind this service's clock a chain's last answered minute, the last in
    # which a check there passed or failed, may be. Past that its marks are too old to trust,
    # and selection there ignores them and leaves freshness out.
    freshness_max_age_minutes: int = Field(default=60, ge=1)
    # Bearer token for pushing scores and sync status, also accepted on every read.
    # /health and /select-indexers take no token. When it is unset, a WARNING is
    # logged at startup and the pushes accept unauthenticated requests, as do the
    # reads unless read_token is set: local dev convenience only.
    push_token: Optional[str] = None
    # When true, startup fails hard if push_token is unset. Set in k8s so a
    # misconfigured Secret is caught at rollout rather than leaving production
    # iisa accepting unauthenticated pushes. Default off for compose/local dev.
    require_push_token: bool = False
    # Optional bearer token for callers that only read, such as dipper. The endpoints that only
    # read accept it as well as push_token; POST /scores, POST /sync-status and POST /freshness
    # accept only push_token. Unset, reads need push_token as before.
    read_token: Optional[str] = None
    # Optional check, through the gateway, of a candidate's claim to have synced a deployment,
    # made before the claim can win it a place. It is on when the gateway's address and an API
    # key are both set, and each check is 1 paid query on that key. See sync_check.py.
    sync_check_gateway_url: Optional[str] = None
    sync_check_gateway_api_key: Optional[str] = None
    # How long the checks for 1 selection may take; they all start at once. Dipper waits 30
    # seconds for its answer, hence the upper limit.
    sync_check_timeout_seconds: float = Field(default=5.0, gt=0, le=10)
    # The oldest a deployment's latest block may be for a claim to be confirmed.
    sync_check_bar_seconds: float = Field(default=36.0, gt=0)

    @field_validator(
        "push_token", "read_token", "sync_check_gateway_url", "sync_check_gateway_api_key"
    )
    @classmethod
    def _strip_token(cls, token: Optional[str]) -> Optional[str]:
        """Drop whitespace around a configured token; 1 of only whitespace counts as unset.

        A secret made from a file often ends in a newline that no caller sends, and the token a
        caller sends is stripped before it is compared, so an unstripped one could never match.
        """
        if token is None:
            return None
        return token.strip() or None

    @model_validator(mode="after")
    def _sync_check_settings_are_whole(self) -> "Settings":
        """Refuse a gateway address without its API key, or the reverse, and an address that is
        not a web address, so a half-configured check stops the service at startup.
        """
        url, key = self.sync_check_gateway_url, self.sync_check_gateway_api_key
        if (url is None) != (key is None):
            raise ValueError(
                "IISA_SYNC_CHECK_GATEWAY_URL and IISA_SYNC_CHECK_GATEWAY_API_KEY must be set "
                "together"
            )
        if url is not None and not url.startswith(("http://", "https://")):
            raise ValueError("IISA_SYNC_CHECK_GATEWAY_URL must start with http:// or https://")
        return self

    @field_validator("freshness_exempt_chains", mode="before")
    @classmethod
    def _split_chains(cls, value: Any) -> Any:
        if isinstance(value, str):
            return frozenset(chain.strip() for chain in value.split(",") if chain.strip())
        return value


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Settings are loaded once and cached for the lifetime of the process.
    """
    return Settings()


def sync_claim_checker(settings: Settings) -> Optional[SyncClaimChecker]:
    """The checker of synced claims these settings ask for, or None when the check is off."""
    url, key = settings.sync_check_gateway_url, settings.sync_check_gateway_api_key
    if url is None or key is None:
        return None
    if url.startswith("http://"):
        # The address stays out of the log: some gateway addresses carry a key themselves.
        logger.warning(
            "IISA_SYNC_CHECK_GATEWAY_URL is a plain http:// address, so the gateway API key "
            "is sent unencrypted with every check of a synced claim"
        )
    return SyncClaimChecker(
        url, key, settings.sync_check_timeout_seconds, settings.sync_check_bar_seconds
    )


# =============================================================================
# Request/Response Models
# =============================================================================

# Limits on a selection request. /select-indexers takes no token, so without them a request
# could be any size. Each field limit is far above anything a real request holds.
# Dipper never asks for more than this many candidates.
MAX_NUM_CANDIDATES = 100
# A deployment's IPFS hash is 46 characters and an indexer address 42.
_MAX_ID_CHARS = 128
_MAX_CHAIN_ID_CHARS = 64
# Entries in a list or map of indexers, far more than the network has indexers.
_MAX_INDEXERS = 10_000
# Keys in a map keyed by deployment. Dipper sends every deployment with a pending agreement or a
# decline in the last 30 days, so these grow with all of dipper's work, not just this request.
# The body limit below binds first, since 100,000 deployment ids alone are about 5 MB; this one
# still holds if the body limit is ever raised.
_MAX_DEPLOYMENTS = 100_000
# The whole body, checked before it is read in full. Dipper sends the pending agreements and
# recent declines of every deployment it handles, though selection uses only this deployment's
# declines and the indexers named in the pending agreements, so today a request grows with
# dipper's whole workload. By 1 estimate it is about 0.14 MB at 1,000 deployments and 1.4 MB at
# 10,000, and reaches this limit somewhere between 7,000 and 31,000, after which every request
# is refused. Once dipper sends only what selection uses, a request is a few kilobytes and this
# limit is far out of reach.
MAX_SELECTION_BODY_BYTES = 4 * 1024 * 1024

# fail_fast stops checking a list or map at its first bad entry. Without it each bad entry is
# its own error, so a body of thousands of them builds thousands of errors in memory.
_IdText = Annotated[str, Field(max_length=_MAX_ID_CHARS)]
_IndexerList = Annotated[list[_IdText], Field(max_length=_MAX_INDEXERS, fail_fast=True)]
_IndexersByDeployment = Annotated[
    dict[_IdText, _IndexerList], Field(max_length=_MAX_DEPLOYMENTS, fail_fast=True)
]


class SelectionRequest(BaseModel):
    """
    Request body for indexer selection endpoint.

    Follows the SelectionRequest struct in dipper's Rust HTTP client, which types chain_id
    as optional. Dipper's reassess job always sets it, and a request without one is a 422.

    A field this model does not know is a 422 too. Dipper's struct is kept in step with this
    model by hand, so a field renamed on 1 side would otherwise be dropped without a word, and a
    dropped blocklist or price ceiling would select blocked or over-priced indexers.
    """

    model_config = ConfigDict(extra="forbid")

    deployment_id: _IdText
    existing_indexers: Optional[_IndexerList] = None
    pending_agreements: Optional[_IndexersByDeployment] = None
    # Target group size (required). 0 or below selects nobody.
    num_candidates: int = Field(le=MAX_NUM_CANDIDATES)
    blocklist: Optional[_IndexerList] = None
    declined_indexers: Optional[_IndexersByDeployment] = None
    # Required: the DIPs-info, chain-support and price filters all key on the chain, so a
    # request without one would skip them all. They also skip any step whose DIPs column is
    # missing from the loaded scores, and every step when dips_info_available is missing.
    chain_id: str = Field(min_length=1, max_length=_MAX_CHAIN_ID_CHARS)  # e.g., "arbitrum-one"
    max_grt_per_30_days: Optional[float] = Field(
        default=None, ge=0, allow_inf_nan=False
    )  # e.g., 4500.0
    max_grt_per_billion_entities_per_30_days: Optional[float] = Field(
        default=None, ge=0, allow_inf_nan=False
    )  # e.g., 2000.0
    # indexer address -> GRT per 30 days, added to the indexer's query fees before its
    # stake-to-fees is scored. A negative or infinite fee would skew that score and a NaN
    # one would be silently treated as 0, so each is a 422.
    optimistic_dips_fees: Optional[
        Annotated[
            dict[_IdText, Annotated[float, Field(ge=0, allow_inf_nan=False)]],
            Field(max_length=_MAX_INDEXERS, fail_fast=True),
        ]
    ] = None

    @model_validator(mode="before")
    @classmethod
    def _refuse_a_flood_of_fields(cls, data: Any) -> Any:
        """Refuse a body with far more fields than this model has, before checking any of them.

        Each unknown field is its own error, so thousands of them would build thousands of
        errors in memory. Up to twice the model's own fields, each unknown 1 is still named.
        """
        most = 2 * len(cls.model_fields)
        if isinstance(data, dict) and len(data) > most:
            raise ValueError(f"has {len(data):,} fields, more than the {most} a request may have")
        return data


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
    # True while the loaded sync status still has an entry young enough to count. Entries
    # stop counting 6 hours after they were fetched, so this turns false when pushes stop.
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


class ChainScore(BaseModel):
    """An indexer's weighted score and component scores, prices and freshness included, on one
    chain. Freshness is left out on a chain whose checks don't yet cover 7 days or have stopped
    counting.

    Scored against every indexer that supports the chain and has priced it, as selection does
    before it applies a request's budget, blocklist and expected fees.
    """

    weighted_score: Optional[float] = None
    components: dict[str, float] = {}


class ScoreResponse(BaseModel):
    """
    Response for the /get-score endpoint.

    weighted_score and components compare the indexer with every loaded indexer and leave out
    prices and freshness, which depend on the chain. chains has the full score for each chain
    it has priced.
    """

    indexer_id: str
    weighted_score: Optional[float] = None
    components: Optional[dict[str, float]] = None
    chains: Optional[dict[str, ChainScore]] = None
    found: bool


class ScoresStatusResponse(BaseModel):
    """
    Response for the GET /scores/status endpoint.

    Says when the loaded scores were computed and how many rows they hold, so monitoring can
    tell whether the daily push landed without fetching the whole snapshot.
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
    """One indexer's scores, shaped like the /get-score response."""

    indexer: str
    weighted_score: Optional[float] = None
    components: dict[str, float] = {}
    chains: dict[str, ChainScore] = {}


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
    """Response for GET /dips-indexers: the indexers offering DIPs on the chain. Unless the chain
    is exempt from freshness, selection also leaves out the ones marked stale. ``computed_at``
    mirrors the push so callers can reject an out-of-date snapshot. Example::

        {"computed_at": "2026-06-23T09:00:00+00:00", "count": 2, "indexers": ["0xaa", "0xbb"]}
    """

    computed_at: Optional[str] = None
    count: int = 0
    indexers: list[str] = []


class SyncStatusAcceptedResponse(BaseModel):
    """Response returned by POST /sync-status on success."""

    status: str
    indexers: int


# A minute this far ahead of the service's clock is refused: once applied, every real minute
# before it would look already applied and be ignored.
_FRESHNESS_MINUTE_LEEWAY = timedelta(minutes=5)


def _whole_utc_minute(value: Any) -> datetime:
    """Read a pushed minute: ISO 8601 text with a time zone, on a whole minute, and not more
    than _FRESHNESS_MINUTE_LEEWAY ahead of this service's clock. Returned in UTC.
    """
    if not isinstance(value, str):
        raise ValueError("minute must be ISO 8601 text")
    try:
        moment = datetime.fromisoformat(value)
        utc = moment.astimezone(timezone.utc) if moment.tzinfo is not None else None
    except (ValueError, OverflowError) as e:
        raise ValueError(f"minute is not a usable ISO 8601 time: {e}") from e
    if utc is None:
        raise ValueError("minute must carry a time zone, such as Z")
    if utc.second or utc.microsecond:
        raise ValueError("minute must fall on a whole minute")
    if utc > datetime.now(timezone.utc) + _FRESHNESS_MINUTE_LEEWAY:
        raise ValueError("minute is ahead of the service's clock")
    return utc


def _utf8_text(text: str) -> str:
    """Refuse text holding half of a UTF-16 surrogate pair. Stored, it could never be written
    out as UTF-8, and every later GET /freshness would fail.
    """
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as e:
        raise ValueError("text holds half of a UTF-16 surrogate pair") from e
    return text


_Utf8Text = Annotated[str, AfterValidator(_utf8_text)]


class FreshnessOutcome(BaseModel):
    """1 indexer's outcome on 1 chain in a freshness push. Numbers sent as text are refused."""

    model_config = ConfigDict(extra="forbid", strict=True)

    indexer: Annotated[str, Field(pattern=r"^0x[0-9a-fA-F]{40}$"), AfterValidator(str.lower)]
    chain: _Utf8Text = Field(min_length=1, max_length=64)
    outcome: Literal["pass", "fail", "no_evidence", "void"]
    reason: Optional[str] = Field(default=None, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    detail: Optional[_Utf8Text] = Field(default=None, max_length=300)
    deployment: Optional[_Utf8Text] = Field(default=None, max_length=64)
    age_seconds: Optional[float] = Field(default=None, ge=0, allow_inf_nan=False)


def _check_outcome(item: Any) -> FreshnessOutcome | str:
    """``item`` read as an outcome, or why it can't be, naming the field at fault."""
    try:
        return FreshnessOutcome.model_validate(item)
    except ValidationError as e:
        first = e.errors(include_url=False, include_input=False)[0]
        where = ".".join(str(part) for part in first["loc"])
        return f"breaks a rule{f' at {where}' if where else ''}: {first['msg']}"


def _log_safe(text: str, limit: int = 200) -> str:
    """``text`` fit for 1 log line: line breaks, other control characters and anything outside
    ASCII escaped, then cut to ``limit`` characters.
    """
    escaped = text.encode("unicode_escape").decode("ascii")
    return escaped if len(escaped) <= limit else escaped[:limit] + "..."


# Outcomes are checked 1 by 1 once the push is read, so any item is accepted here and refused
# later. The schema says so while still describing the outcome that gets applied.
_OutcomeItem = Annotated[Any, WithJsonSchema({"anyOf": [FreshnessOutcome.model_json_schema(), {}]})]


class FreshnessPush(BaseModel):
    """Body of POST /freshness: every outcome for the checks sent in 1 minute. Each outcome is
    checked on its own, and 1 that breaks FreshnessOutcome's rules or repeats an indexer and
    chain earlier in the push is refused and counted, so it can't cost the rest of the minute.
    """

    model_config = ConfigDict(extra="forbid")

    minute: Annotated[datetime, BeforeValidator(_whole_utc_minute)]
    outcomes: list[_OutcomeItem] = Field(max_length=50_000)
    _accepted: list[FreshnessOutcome] = PrivateAttr(default_factory=list)
    _refused: int = PrivateAttr(default=0)
    _first_refusal: Optional[str] = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _check_each_outcome(self) -> Self:
        first_at: dict[tuple[str, str], int] = {}
        for position, item in enumerate(self.outcomes):
            checked = _check_outcome(item)
            if isinstance(checked, FreshnessOutcome):
                pair = (checked.indexer, checked.chain)
                if pair not in first_at:
                    first_at[pair] = position
                    self._accepted.append(checked)
                    continue
                checked = f"repeats the indexer and chain of outcomes[{first_at[pair]}]"
            self._refused += 1
            if self._first_refusal is None:
                self._first_refusal = f"outcomes[{position}] {checked}"
        return self

    @property
    def accepted(self) -> list[FreshnessOutcome]:
        """The outcomes that passed their checks, in the order sent."""
        return self._accepted

    @property
    def refused(self) -> int:
        """How many outcomes were left out."""
        return self._refused

    @property
    def first_refusal(self) -> Optional[str]:
        """Which outcome was the first left out and why, holding text from the request."""
        return self._first_refusal


class FreshnessAcceptedResponse(BaseModel):
    """Response to POST /freshness. pairs is how many outcomes were applied, and refused how
    many were left out for breaking the rules or repeating an indexer and chain. applied is
    false for a minute at or before the last one applied, which changes nothing.
    """

    status: str
    minute: str
    applied: bool
    pairs: int
    refused: int


class FreshnessLastCheck(BaseModel):
    """The latest outcome recorded for an indexer on a chain."""

    minute: str
    outcome: str
    reason: Optional[str] = None
    detail: Optional[str] = None
    deployment: Optional[str] = None
    age_seconds: Optional[float] = None


class FreshnessPair(BaseModel):
    """An indexer's freshness on 1 chain. judged is false when fewer than 5 of the last 10
    minutes counted. marked then comes from its last 5 counted checks, recent_passed of the
    recent_counted no more than 60 of the chain's answered minutes old: it is unmarked only when
    all 5 are that recent and at least 3 passed. An indexer that never counted keeps its mark.
    last_counted_minute is null until one of its checks counts.
    """

    marked: bool
    judged: bool
    window_passed: int
    window_counted: int
    last: FreshnessLastCheck
    last_counted_minute: Optional[str] = None
    recent_passed: int = 0
    recent_counted: int = 0
    passed_28d: int
    counted_28d: int
    share_28d: Optional[float] = None


class FreshnessChain(BaseModel):
    """Freshness on 1 chain, by indexer. enforced says the chain isn't exempt, and enforcing
    that a check there also passed or failed recently enough, at last_answered_minute, for a
    marked indexer to be left out of new agreements. last_counted_minute is the last minute a
    check there counted, no evidence included. Each is null when no such check is on record.
    """

    enforced: bool = False
    enforcing: bool = False
    last_counted_minute: Optional[str] = None
    last_answered_minute: Optional[str] = None
    indexers: dict[str, FreshnessPair] = {}


class FreshnessResponse(BaseModel):
    """Response for GET /freshness. last_minute is null until the first push."""

    last_minute: Optional[str] = None
    chains: dict[str, FreshnessChain] = {}


class ErrorResponse(BaseModel):
    """Body of each error answer other than a 422: what went wrong, in words."""

    detail: str


_ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    400: {"model": ErrorResponse, "description": "The body could not be read as text"},
    401: {"model": ErrorResponse, "description": "The bearer token is missing or wrong"},
    413: {"model": ErrorResponse, "description": "The body is over the size limit"},
    503: {"model": ErrorResponse, "description": "The service has not started or has no scores"},
}


def _documented_errors(*codes: int) -> dict[int | str, dict[str, Any]]:
    """OpenAPI entries for the error answers a route gives besides FastAPI's own 422."""
    return {code: _ERROR_RESPONSES[code] for code in codes}


# =============================================================================
# Service State
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("iisa-service")


# The warning that freshness isn't enforced on a chain whose checks have stopped passing or
# failing is logged at most once per chain in this many minutes of the service's clock, since a
# prober that is down or off would otherwise log 1 on every selection for up to 28 days.
LAPSE_WARNING_INTERVAL_MINUTES = 10


class _PerChainLimit:
    """Lets a log line through at most once per chain every LAPSE_WARNING_INTERVAL_MINUTES,
    counting those held back in between, until the chain is reset, which starts it afresh. It
    has a lock of its own, so selections running on different threads can share it.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last: dict[str, int] = {}
        self._held: dict[str, int] = {}

    def allow(self, chain: str, now: int) -> Optional[int]:
        """How many lines on ``chain`` were held back since the last one let through, when a
        line in the minute ``now`` may be logged, or None when it is held back too.
        """
        with self._lock:
            last = self._last.get(chain)
            if last is not None and now - last < LAPSE_WARNING_INTERVAL_MINUTES:
                self._held[chain] = self._held.get(chain, 0) + 1
                return None
            self._last[chain] = now
            return self._held.pop(chain, 0)

    def reset(self, chain: str) -> None:
        """Forget ``chain``'s last line and the count held back since, so its next line goes
        through and counts only what is held back after it.
        """
        with self._lock:
            self._last.pop(chain, None)
            self._held.pop(chain, None)


class IISAState:
    """
    Holds the IISA service state including initialized providers and cached data.
    """

    def __init__(self) -> None:
        self.settings: Optional[Settings] = None
        self.data_manager: Optional[DataManager] = None
        self._history: Optional[pd.DataFrame] = None
        self._sync_status: Optional["SyncStatusData"] = None
        self.sync_checker: Optional[SyncClaimChecker] = None
        self.freshness = FreshnessStore()
        self.lapse_warnings = _PerChainLimit()
        self._initialized: bool = False

    def initialize(self, settings: Settings) -> bool:
        """
        Initialize the IISA providers.

        Returns True if initialization succeeded, False if we should fall back
        to random selection mode.
        """
        self.settings = settings
        self.sync_checker = sync_claim_checker(settings)
        logger.info(
            "Candidates' synced claims are %s",
            "checked through the gateway before they can win a place"
            if self.sync_checker
            else "taken on trust",
        )

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

    def refresh_freshness(self) -> bool:
        """Load the saved freshness state. Returns True when it holds at least 1 push."""
        if self.settings is None:
            return False
        self.freshness = FreshnessStore(load_state(self.settings.freshness_file_path))
        return self.freshness.last_minute is not None

    def apply_freshness(self, push: FreshnessPush) -> bool:
        """Apply a freshness push's accepted outcomes and save the state. False when the
        minute was ignored.
        """
        if self.settings is None:
            raise RuntimeError("Settings not initialized")
        checks = [Check(**outcome.model_dump()) for outcome in push.accepted]
        save = partial(_atomic_write_json, self.settings.freshness_file_path)
        return self.freshness.apply(minute_index(push.minute), checks, save)

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
    except (ValueError, TypeError, RecursionError):
        logger.debug("Ignoring unreadable supported networks %.200r", networks_json)
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
    """Validate the bearer token against IISA_PUSH_TOKEN, for the endpoints that push.

    When IISA_PUSH_TOKEN is unset, auth is off (local dev convenience).
    """
    _require_one_of([get_settings().push_token], authorization)


def _require_read_token(authorization: Optional[str]) -> None:
    """Validate the bearer token against IISA_PUSH_TOKEN or IISA_READ_TOKEN, for reads.

    When neither is set, auth is off (local dev convenience).
    """
    settings = get_settings()
    _require_one_of([settings.push_token, settings.read_token], authorization)


def _require_one_of(tokens: list[Optional[str]], authorization: Optional[str]) -> None:
    """Raise a 401 unless the bearer token is 1 of the tokens set; with none set, allow all.

    "Bearer" is matched in any letter case per RFC 6750 §2.1, the token exactly.
    hmac.compare_digest dodges timing oracles; it compares bytes, since it refuses text with
    non-ASCII letters. Every token is compared, so the time taken says nothing about which one
    the provided token was closest to.
    """
    expected = [token.encode() for token in tokens if token]
    if not expected:
        return
    if not authorization or authorization[:7].lower() != "bearer ":
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header")
    provided = authorization[7:].strip().encode()
    matches = [hmac.compare_digest(provided, token) for token in expected]
    if not any(matches):
        raise HTTPException(status_code=401, detail="Invalid bearer token")


# Global state
_state = IISAState()


# =============================================================================
# FastAPI Application
# =============================================================================


# The opt-in list of enforced chains that IISA_FRESHNESS_EXEMPT_CHAINS replaced. Startup warns
# when it is still set, as whoever set it may expect only the chains it lists to be enforced.
_RETIRED_ENFORCED_CHAINS = "IISA_FRESHNESS_ENFORCED_CHAINS"


def _warn_about_retired_settings(environ: Mapping[str, str]) -> None:
    if _RETIRED_ENFORCED_CHAINS in environ:
        logger.warning(
            "%s is set but no longer read: freshness is enforced on every chain not listed in "
            "IISA_FRESHNESS_EXEMPT_CHAINS",
            _RETIRED_ENFORCED_CHAINS,
        )


@asynccontextmanager
async def lifespan(_app: FastAPI):
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

    if settings.read_token and settings.read_token == settings.push_token:
        logger.warning(
            "IISA_READ_TOKEN is the same as IISA_PUSH_TOKEN, so whoever holds it can push "
            "scores and sync status too. Give readers a token of their own."
        )

    _warn_about_retired_settings(os.environ)

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

    if _state.refresh_freshness():
        logger.info("Recovered freshness state on startup")
    else:
        logger.info("No freshness state on startup; marks start again from the first push")

    logger.info("IISA service ready")

    yield

    logger.info("Shutting down IISA service...")


app = FastAPI(
    title="IISA Service",
    description="Indexing Indexer Selection Algorithm for The Graph DIPs service",
    version="0.1.0",
    lifespan=lifespan,
)


class _SelectionBodyLimit:
    """Refuse a /select-indexers body over MAX_SELECTION_BODY_BYTES with a 413 before it is parsed.

    The request's own checks run only once the whole body has been read and parsed, and this is
    the 1 endpoint that takes no token, so a huge body could use up the service's memory first.
    Only this route is covered, since the score job's POST /scores is legitimately large.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] != "/select-indexers":
            await self.app(scope, receive, send)
            return
        declared = dict(scope["headers"]).get(b"content-length", b"")
        if declared.isdigit() and int(declared) > MAX_SELECTION_BODY_BYTES:
            await _body_too_large(scope, receive, send)
            return

        # No length, or one within the limit: read the body, counting what arrives.
        pieces: list[bytes] = []
        received = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                return  # The client went away before sending the whole body.
            received += len(message.get("body", b""))
            if received > MAX_SELECTION_BODY_BYTES:
                await _body_too_large(scope, receive, send)
                return
            pieces.append(message.get("body", b""))
            if not message.get("more_body", False):
                break

        body = b"".join(pieces)
        # Let the pieces go, so the route runs holding 1 copy of the body rather than 2.
        del pieces
        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": body, "more_body": False}

        await self.app(scope, replay, send)


async def _body_too_large(scope: Scope, receive: Receive, send: Send) -> None:
    logger.warning("Refused a /select-indexers body over %s bytes", f"{MAX_SELECTION_BODY_BYTES:,}")
    response = JSONResponse(
        status_code=413,
        content={"detail": f"Request body is over the {MAX_SELECTION_BODY_BYTES:,} byte limit"},
    )
    await response(scope, receive, send)


app.add_middleware(_SelectionBodyLimit)


def _json_safe(value: Any) -> Any:
    """Replace NaN and infinite floats, at any depth, with their text form, and write half of a
    UTF-16 surrogate pair in text as a \\u escape, since UTF-8 can't encode it.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, str):
        return value.encode("utf-8", "backslashreplace").decode("utf-8")
    if isinstance(value, dict):
        return {_json_safe(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


# A 422 reports at most this many errors, each echoing only a short excerpt of what was sent,
# so a refusal stays small however much a request got wrong.
_MAX_REPORTED_ERRORS = 20
_MAX_EXCERPT_CHARS = 100
_MAX_EXCERPT_ITEMS = 5


def _excerpt(value: Any, depth: int = 0) -> Any:
    """A short excerpt of a value a request sent, of the same type where it can be.

    Text is cut to 100 characters, a list or map to its first 5 entries, and anything nested
    deeper than 2 levels is described rather than copied. Other values are returned as they are.
    """
    if isinstance(value, str):
        cut = len(value) > _MAX_EXCERPT_CHARS
        return value[:_MAX_EXCERPT_CHARS] + "..." if cut else value
    if isinstance(value, (list, tuple, dict)) and depth >= 2:
        return f"{len(value):,} entries"
    if isinstance(value, (list, tuple)):
        return _excerpt_list(value, depth)
    if isinstance(value, dict):
        return _excerpt_map(value, depth)
    return value


def _excerpt_list(value: list | tuple, depth: int) -> list:
    """The first entries of a list, each cut to an excerpt, and how many more there were."""
    kept = [_excerpt(item, depth + 1) for item in value[:_MAX_EXCERPT_ITEMS]]
    more = len(value) - len(kept)
    return kept + [f"... {more:,} more"] if more else kept


def _excerpt_map(value: dict, depth: int) -> dict:
    """The first entries of a map, keys and values cut to excerpts, and how many more there were."""
    first = itertools.islice(value.items(), _MAX_EXCERPT_ITEMS)
    kept = {_excerpt(str(key)): _excerpt(item, depth + 1) for key, item in first}
    more = len(value) - len(kept)
    return {**kept, "...": f"{more:,} more"} if more else kept


def _reported_error(error: Mapping[str, Any]) -> dict[str, Any]:
    """1 validation error as a 422 reports it: its input and location cut to excerpts."""
    reported = dict(error)
    if "input" in reported:
        reported["input"] = _excerpt(reported["input"])
    reported["loc"] = [_excerpt(part) for part in reported.get("loc", ())]
    return reported


@app.exception_handler(RequestValidationError)
async def _request_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    """FastAPI's own 422 echoes the rejected input, and a NaN or infinite float there
    can't be written as JSON, which turns the 422 into a 500. It also echoes every error in
    full, so a request with thousands of errors, or 1 huge value, got an answer far larger than
    itself; this reports the first 20, each with excerpts of what was sent.
    """
    reported = [_reported_error(error) for error in exc.errors()[:_MAX_REPORTED_ERRORS]]
    errors = _json_safe(jsonable_encoder(reported))
    return JSONResponse(status_code=422, content={"detail": errors})


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
        sync_status_loaded=_state.sync_status is not None and _state.sync_status.total_indexers > 0,
        computed_at=computed_at_iso,
        scores_age_hours=round(age_hours, 1) if age_hours is not None else None,
    )


@app.post(
    "/scores", response_model=ScoresAcceptedResponse, responses=_documented_errors(400, 401, 503)
)
def push_scores(
    payload: Annotated[list[dict[str, Any]], Body(min_length=1)],
    authorization: Optional[str] = Header(None),
) -> ScoresAcceptedResponse:
    """Accept a pushed scores snapshot from the cronjob.

    Order: token → body → DataFrame → atomic write to cache PVC → in-memory
    update → row count. Parse-before-disk means a malformed payload 422s
    without mutating disk. Sync def so the blocking write hits the threadpool.
    """
    _require_push_token(authorization)

    if not _state._initialized:
        raise HTTPException(
            status_code=503,
            detail="IISA providers not initialized",
        )

    try:
        rows = _state.load_scores_from_records(payload)
    except ScoresPayloadError as e:
        # Wrong-shape body: the caller's problem, so a 422 in the error list every 422 carries.
        logger.warning("Rejected pushed scores: %s", e)
        raise RequestValidationError(e.errors) from e
    except Exception:
        logger.exception("Failed to accept pushed scores")
        raise HTTPException(status_code=500, detail="Failed to accept scores")

    logger.info("Accepted pushed scores: %d rows", rows)
    return ScoresAcceptedResponse(status="success", rows=rows)


@app.get("/scores/status", response_model=ScoresStatusResponse, responses=_documented_errors(401))
def scores_status(
    authorization: Optional[str] = Header(None),
) -> ScoresStatusResponse:
    """Report computed_at and the row count of the currently loaded scores.

    For monitoring and operators checking that the daily push landed. The score job does not
    call it: a rerun on the same day computes and pushes again. Sync def, since the body has
    no awaitable work.
    """
    _require_read_token(authorization)

    if _state.data_manager is None:
        return ScoresStatusResponse(computed_at=None, rows=0)

    snap = _state.data_manager.snapshot
    computed_at = _format_computed_at(snap.computed_at)

    rows = len(snap.data) if snap.data is not None else 0
    return ScoresStatusResponse(computed_at=computed_at, rows=rows)


@app.get("/scores", response_model=ScoresSnapshotResponse, responses=_documented_errors(401))
def scores_snapshot(
    authorization: Optional[str] = Header(None),
) -> ScoresSnapshotResponse:
    """Return the loaded scores snapshot as a list of records.

    Symmetric with POST /scores — what the cronjob pushed is what's served
    here. For ops debugging and external monitoring, not the selection hot
    path. Sync def so the DataFrame→JSON conversion runs on the threadpool.
    """
    _require_read_token(authorization)

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


# Prices and freshness only exist once a chain is chosen, so the network-wide score leaves
# these weights out.
_CHAIN_WEIGHTS = ("base_price_per_epoch", "price_per_entity", "freshness")


def _scores_by_indexer(
    normalized: pd.DataFrame,
    weights: Mapping[str, object],
    indexer_id: Optional[str] = None,
) -> dict[str, ChainScore]:
    """Weighted score and component scores for each indexer in an already normalised table.

    Only the metrics in ``weights`` count. ``indexer_id``, when given, scores just that indexer.
    An indexer with more than 1 row is scored from its first.
    """
    from .indexer_selection import WeightsDict, _calculate_weighted_scores

    if indexer_id is not None:
        normalized = normalized[normalized["indexer"] == indexer_id]
    if normalized.empty:
        return {}

    # Score every indexer in one vectorised pass. _normalize_metrics fills every
    # weighted norm_ column, so the zero-weight case can't arise here; degrade to
    # unscored rather than 500 if it somehow does.
    try:
        weighted_scores = _calculate_weighted_scores(
            normalized, cast(WeightsDict, dict(weights))
        ).to_numpy()
    except ValueError as e:
        logger.warning("weighted scoring failed: %s", e)
        weighted_scores = pd.Series(float("nan"), index=normalized.index).to_numpy()

    reported = [
        (col, name) for col, name in _BREAKDOWN_COMPONENTS if col.removeprefix("norm_") in weights
    ]
    scores: dict[str, ChainScore] = {}
    for position, (_, row) in enumerate(normalized.iterrows()):
        row_indexer = row.get("indexer")
        if row_indexer is None or (isinstance(row_indexer, float) and pd.isna(row_indexer)):
            logger.debug("skipping row with missing indexer field")
            continue
        if str(row_indexer) in scores:
            logger.warning("indexer %s has more than 1 score row; using the first", row_indexer)
            continue

        score = weighted_scores[position]
        scores[str(row_indexer)] = ChainScore(
            weighted_score=float(score) if pd.notna(score) else None,
            components={
                name: float(row[col])
                for col, name in reported
                if col in row.index and pd.notna(row[col])
            },
        )
    return scores


def _network_scores(
    history: pd.DataFrame, indexer_id: Optional[str] = None
) -> dict[str, ChainScore]:
    """Each indexer scored against every loaded indexer, with prices and freshness left out."""
    from .indexer_selection import DEFAULT_WEIGHTS, _normalize_metrics

    weights = {k: v for k, v in DEFAULT_WEIGHTS.items() if k not in _CHAIN_WEIGHTS}
    return _scores_by_indexer(_normalize_metrics(history.copy()), weights, indexer_id)


def _priced_chains(history: pd.DataFrame) -> list[str]:
    """Every chain at least 1 indexer in ``history`` has set a price for, sorted."""
    if "dips_min_grt_per_30_days" not in history.columns:
        return []
    chains: set[str] = set()
    for prices_json in history["dips_min_grt_per_30_days"]:
        try:
            prices = json.loads(prices_json) if isinstance(prices_json, str) else {}
        except ValueError:
            logger.debug("skipping unreadable price map %r", prices_json)
            continue
        if isinstance(prices, dict):
            chains.update(prices)
    return sorted(chains)


def _chain_scores(
    history: pd.DataFrame, chains: list[str], indexer_id: Optional[str] = None
) -> dict[str, dict[str, ChainScore]]:
    """Scores by indexer, then by chain, each chain scored like a selection with no budget."""
    from .indexer_selection import DEFAULT_WEIGHTS, _normalize_metrics

    scores: dict[str, dict[str, ChainScore]] = {}
    for chain in chains:
        pool, _ = _filter_by_price(history, chain, None)
        # _filter_by_price skips its checks on data without DIPs columns, so drop indexers that
        # haven't priced this chain; their missing price would otherwise count as free.
        pool, _ = _keep_with_chain_price(pool, chain)
        if pool.empty:
            continue
        normalized = _normalize_metrics(_enrich_for_chain(pool, chain))
        for row_indexer, score in _scores_by_indexer(
            normalized, DEFAULT_WEIGHTS, indexer_id
        ).items():
            scores.setdefault(row_indexer, {})[chain] = score
    return scores


@app.get(
    "/scores/weighted", response_model=WeightedScoresResponse, responses=_documented_errors(401)
)
def scores_weighted(
    authorization: Optional[str] = Header(None),
) -> WeightedScoresResponse:
    """Bulk weighted-score snapshot — same shape as /get-score, every indexer.

    Single pass across the loaded snapshot — saves N round-trips to /get-score.
    Selection can pass `max_grt_per_30_days` as a `price_ceiling`; this endpoint
    cannot, so each chain's prices are normalised against its observed max instead.
    """
    _require_read_token(authorization)

    if _state.data_manager is None:
        return WeightedScoresResponse(computed_at=None, count=0, scores=[])

    snap = _state.data_manager.snapshot

    computed_at = _format_computed_at(snap.computed_at)

    if snap.data is None or snap.data.empty:
        return WeightedScoresResponse(computed_at=computed_at, count=0, scores=[])

    by_chain = _chain_scores(snap.data, _priced_chains(snap.data))
    entries = [
        WeightedScoreEntry(
            indexer=indexer_id,
            weighted_score=score.weighted_score,
            components=score.components,
            chains=by_chain.get(indexer_id, {}),
        )
        for indexer_id, score in _network_scores(snap.data).items()
    ]

    return WeightedScoresResponse(
        computed_at=computed_at,
        count=len(entries),
        scores=entries,
    )


@app.get("/dips-indexers", response_model=DipsIndexersResponse, responses=_documented_errors(401))
def dips_indexers(
    chain: str,
    max_grt_per_30_days: Optional[float] = Query(default=None, ge=0, allow_inf_nan=False),
    max_grt_per_billion_entities_per_30_days: Optional[float] = Query(
        default=None, ge=0, allow_inf_nan=False
    ),
    authorization: Optional[str] = Header(None),
) -> DipsIndexersResponse:
    """Indexers offering DIPs on ``chain`` (a 422 if omitted), by selection's price test: they
    answered their DIPs probe, support the chain and are priced within the optional
    ``max_grt_per_30_days`` and ``max_grt_per_billion_entities_per_30_days`` ceilings (omit
    either to skip it). Unless the chain is exempt from freshness, selection also leaves out
    indexers marked stale, which this list still includes. Example: ``?chain=arbitrum-one``.
    """
    _require_read_token(authorization)

    if _state.data_manager is None:
        return DipsIndexersResponse(computed_at=None, count=0, indexers=[])

    snap = _state.data_manager.snapshot

    computed_at = _format_computed_at(snap.computed_at)

    if snap.data is None or snap.data.empty or "dips_supported_networks" not in snap.data.columns:
        return DipsIndexersResponse(computed_at=computed_at, count=0, indexers=[])

    # Selection's own price test: an indexer offers DIPs on a chain only if it answered its
    # probe, supports the chain and priced it within the optional ceilings (None skips one).
    matched, _ = _filter_by_price(
        snap.data, chain, max_grt_per_30_days, max_grt_per_billion_entities_per_30_days
    )

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


@app.post(
    "/sync-status",
    response_model=SyncStatusAcceptedResponse,
    responses=_documented_errors(400, 401, 503),
)
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


@app.post(
    "/freshness",
    response_model=FreshnessAcceptedResponse,
    responses=_documented_errors(400, 401, 503),
)
def push_freshness(
    push: FreshnessPush,
    authorization: Optional[str] = Header(None),
) -> FreshnessAcceptedResponse:
    """Accept 1 minute of freshness outcomes from the prober and recompute every stale mark.

    Each outcome is checked on its own, and a bad or repeated one is refused and counted. A
    minute at or before the last one applied changes nothing, so a retry can't count twice.
    Sync def so the file write runs on the threadpool.
    """
    _require_push_token(authorization)

    if _state.settings is None:
        raise HTTPException(status_code=503, detail="IISA service not initialized")

    minute = minute_text(minute_index(push.minute))
    if push.refused:
        logger.warning(
            "Refused %d of %d freshness outcomes for minute %s, such as %s",
            push.refused,
            len(push.outcomes),
            minute,
            _log_safe(push.first_refusal or ""),
        )
    try:
        applied = _state.apply_freshness(push)
    except Exception:
        # A failed save leaves the minute applied in memory; the next saved push writes it out.
        logger.exception("Failed to apply or save pushed freshness for minute %s", minute)
        raise HTTPException(status_code=500, detail="Failed to apply or save freshness")

    if applied:
        logger.info("Applied freshness for minute %s: %d pairs", minute, len(push.accepted))
    else:
        logger.info("Ignored freshness for minute %s: at or before the last applied", minute)
    return FreshnessAcceptedResponse(
        status="success",
        minute=minute,
        applied=applied,
        pairs=len(push.accepted) if applied else 0,
        refused=push.refused,
    )


@app.get("/freshness", response_model=FreshnessResponse, responses=_documented_errors(401))
def freshness_report(
    authorization: Optional[str] = Header(None),
) -> FreshnessResponse:
    """Each indexer's stale mark, last 10 minutes, latest outcome and 28-day counts, by chain."""
    _require_read_token(authorization)
    return FreshnessResponse.model_validate(_state.freshness.snapshot(_enforcement()))


@app.post("/get-score", response_model=ScoreResponse, responses=_documented_errors(400, 401, 503))
async def get_score(
    request: ScoreRequest,
    authorization: Optional[str] = Header(None),
) -> ScoreResponse:
    """Return one indexer's weighted score plus the component scores feeding it.

    Useful for debugging selection decisions and monitoring per-indexer
    performance without running a full selection request.
    """
    _require_read_token(authorization)

    if not _state.is_ready or _state.history is None:
        raise HTTPException(status_code=503, detail="IISA data not loaded")

    # Normalisation is relative to the other indexers, so score this one against the whole
    # table like /scores/weighted (the two endpoints agree). Selection scores against only the
    # indexers its request could keep or pick, so its values can differ from these.
    indexer_id = request.indexer_id.lower()
    history = _state.history
    match = history[history["indexer"] == indexer_id]

    if match.empty:
        return ScoreResponse(indexer_id=request.indexer_id, found=False)

    network = _network_scores(history, indexer_id)[indexer_id]
    return ScoreResponse(
        indexer_id=request.indexer_id,
        weighted_score=network.weighted_score,
        components=network.components,
        chains=_chain_scores(history, _priced_chains(match), indexer_id).get(indexer_id, {}),
        found=True,
    )


@app.post(
    "/select-indexers",
    response_model=SelectionResponse,
    responses=_documented_errors(400, 413, 503),
)
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
        "existing=%d blocked=%d budget=%s entity_budget=%s",
        request.deployment_id,
        request.chain_id,
        request.num_candidates,
        len(request.existing_indexers or []),
        len(request.blocklist or []),
        f"{request.max_grt_per_30_days} GRT/30d"
        if request.max_grt_per_30_days is not None
        else "none",
        f"{request.max_grt_per_billion_entities_per_30_days} GRT per billion entities/30d"
        if request.max_grt_per_billion_entities_per_30_days is not None
        else "none",
    )

    try:
        response = await _select_checking_sync_claims(request)
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
    """Extract the price for a specific chain from the JSON price map.

    A price that is not a finite number >= 0 counts as no price, so the indexer is excluded on
    that chain. A NaN or negative price would otherwise be scored as the cheapest and win.
    """
    try:
        prices = json.loads(dips_min_grt_json) if isinstance(dips_min_grt_json, str) else {}
        if not isinstance(prices, dict):
            raise TypeError(f"price map is a JSON {type(prices).__name__}, not an object")
        val = prices.get(chain_id)
        if val is None:
            return None
        price = float(val)
    except (TypeError, ValueError, OverflowError, RecursionError):
        logger.debug(
            "Ignoring unreadable price for chain '%s' in %.200r", chain_id, dips_min_grt_json
        )
        return None
    if isinstance(val, bool) or not math.isfinite(price) or price < 0:
        logger.debug("Ignoring unusable price %r for chain '%s'", val, chain_id)
        return None
    return price


_ENTITY_PRICE_COLUMN = "dips_min_grt_per_billion_entities_per_30_days"


class _EntityPrice(NamedTuple):
    """An indexer's per-entity price, read the same way by every step that uses it.

    ``price`` is None when the indexer set none, meaning it accepts any per-entity price, and
    when ``invalid``: a value is set but is not a finite number >= 0.
    """

    price: Optional[float]
    invalid: bool = False


_INVALID_ENTITY_PRICE = _EntityPrice(None, invalid=True)


def _parse_entity_price(value: Any) -> _EntityPrice:
    """Read one indexer's per-entity price. None or NaN is missing; anything else must be a
    finite number >= 0, sent as a number or as text, or it is invalid.
    """
    if pd.api.types.is_scalar(value) and pd.isna(value):
        return _EntityPrice(None)
    if isinstance(value, bool) or not isinstance(value, (numbers.Real, str)):
        return _INVALID_ENTITY_PRICE
    try:
        price = float(value)
    except (ValueError, OverflowError):
        return _INVALID_ENTITY_PRICE
    return _EntityPrice(price) if math.isfinite(price) and price >= 0 else _INVALID_ENTITY_PRICE


def _keep_with_dips_info(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers that answered their DIP info probe."""
    initial_count = len(df)
    df = df[df["dips_info_available"] == True]  # noqa: E712
    logger.debug("price filter: %d/%d indexers have DIP info", len(df), initial_count)
    if df.empty:
        return df, f"all {initial_count} indexers lack DIP info (dips_info_available=False)"
    return df, ""


def _keep_supporting_chain(df: pd.DataFrame, chain_id: str) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers whose supported networks include the chain."""
    if "dips_supported_networks" not in df.columns:
        return df, ""
    pre_filter = len(df)
    df = df[df["dips_supported_networks"].apply(lambda v: _supports_chain(v, chain_id))]
    logger.debug("price filter: %d/%d indexers support chain '%s'", len(df), pre_filter, chain_id)
    if df.empty:
        return df, f"none of {pre_filter} indexers support chain '{chain_id}'"
    return df, ""


def _keep_with_chain_price(df: pd.DataFrame, chain_id: str) -> tuple[pd.DataFrame, str]:
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


def _keep_with_valid_entity_price(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Price filter step: drop indexers whose per-entity price is set but invalid.

    Runs whether or not the request sets a per-entity ceiling, since such a price sets no
    usable floor to score the indexer on or to offer it. A missing price passes.
    """
    if _ENTITY_PRICE_COLUMN not in df.columns:
        return df, ""
    invalid = df[_ENTITY_PRICE_COLUMN].apply(lambda v: _parse_entity_price(v).invalid).astype(bool)
    for indexer, value in zip(df.loc[invalid, "indexer"], df.loc[invalid, _ENTITY_PRICE_COLUMN]):
        logger.warning(
            "price filter: dropping indexer %s with invalid per-entity price %.200r",
            indexer,
            value,
        )
    pre_filter = len(df)
    df = df[~invalid]
    if df.empty:
        return df, f"all {pre_filter} indexers have an invalid per-entity price"
    return df, ""


def _keep_within_entity_budget(
    df: pd.DataFrame, max_grt_per_billion_entities_per_30_days: Optional[float]
) -> tuple[pd.DataFrame, str]:
    """Price filter step: keep indexers whose per-entity price is at or below the ceiling.

    An indexer with no per-entity price passes, since it is scored and offered as 0. An
    invalid price fails, since it sets no usable floor.
    """
    if max_grt_per_billion_entities_per_30_days is None or _ENTITY_PRICE_COLUMN not in df.columns:
        return df, ""
    ceiling = max_grt_per_billion_entities_per_30_days

    def within_budget(value: Any) -> bool:
        entity_price = _parse_entity_price(value)
        if entity_price.invalid:
            return False
        return entity_price.price is None or entity_price.price <= ceiling

    pre_filter = len(df)
    df = df[df[_ENTITY_PRICE_COLUMN].apply(within_budget)]
    logger.debug(
        "price filter: %d/%d indexers within budget of %s GRT per billion entities/30d",
        len(df),
        pre_filter,
        ceiling,
    )
    if df.empty:
        return (
            df,
            f"all {pre_filter} indexers exceed payment "
            f"ceiling of {ceiling} GRT per billion entities/30d",
        )
    return df, ""


def _filter_by_price(
    history: pd.DataFrame,
    chain_id: str,
    max_grt_per_30_days: Optional[float],
    max_grt_per_billion_entities_per_30_days: Optional[float] = None,
) -> tuple[pd.DataFrame, str]:
    """Drop indexers that lack DIP info, don't support the chain, lack a chain price, exceed
    max_grt_per_30_days for the chain, set an invalid per-entity price, or exceed the
    per-entity ceiling.

    Returns (filtered_df, reason). ``reason`` says why if no candidates are left, else is empty.
    """
    df = history.copy()

    # Only filter if we have the DIP info columns
    if "dips_info_available" not in df.columns:
        return df, ""

    steps: tuple[Callable[[pd.DataFrame], tuple[pd.DataFrame, str]], ...] = (
        _keep_with_dips_info,
        partial(_keep_supporting_chain, chain_id=chain_id),
        partial(_keep_with_chain_price, chain_id=chain_id),
        partial(_keep_within_budget, chain_id=chain_id, max_grt_per_30_days=max_grt_per_30_days),
        _keep_with_valid_entity_price,
        partial(
            _keep_within_entity_budget,
            max_grt_per_billion_entities_per_30_days=max_grt_per_billion_entities_per_30_days,
        ),
    )
    for step in steps:
        df, reason = step(df)
        if df.empty:
            return df, reason
    return df, ""


def _enrich_with_chain_prices(
    history: pd.DataFrame,
    chain_id: str,
) -> pd.DataFrame:
    """
    Add base_price_per_epoch and price_per_entity columns for scoring.

    Extracts the chain-specific price from the JSON fields.
    """
    df = history.copy()

    if "dips_min_grt_per_30_days" not in df.columns:
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

    def entity_price_for_scoring(value: Any) -> float:
        # A missing price scores as 0. An invalid one scores as NaN, which ranks worst.
        entity_price = _parse_entity_price(value)
        if entity_price.invalid:
            return math.nan
        return 0.0 if entity_price.price is None else entity_price.price

    if _ENTITY_PRICE_COLUMN in df.columns:
        df["price_per_entity"] = (
            df[_ENTITY_PRICE_COLUMN].map(entity_price_for_scoring).astype(float)
        )
    else:
        df["price_per_entity"] = 0.0

    return df


def _weighed_freshness(chain_id: str) -> Optional[Mapping[str, float]]:
    """Each indexer's freshness score on ``chain_id``, or None where freshness isn't weighed: on
    an exempt chain, whose checks are taken as untrustworthy or impossible to pass, on a chain
    whose checks don't yet cover 7 days, and on one where no check has passed or failed within
    IISA_FRESHNESS_MAX_AGE_MINUTES by this service's clock, which is when enforcement stops
    trusting its marks too. The scores are worked out at each push, so they go stale when the
    prober stops or no check goes through the gateway. Reads neither through the freshness lock.
    """
    scores = _state.freshness.scores_on(chain_id)
    enforcement = _enforcement()
    if scores is None or not enforcement.enforced(chain_id):
        return None
    if not enforcement.current(_state.freshness.last_answered_minute(chain_id)):
        return None
    return scores


def _with_chain_freshness(history: pd.DataFrame, chain_id: str) -> pd.DataFrame:
    """``history`` with each indexer's freshness score on ``chain_id`` in a freshness column,
    replacing any it held: 0 for an indexer with no record there, and NaN for everyone where
    _weighed_freshness gives no scores, so the weighted score leaves freshness out there rather
    than scoring everyone 0.
    """
    df = history.copy()
    scores = _weighed_freshness(chain_id)
    if scores is None or "indexer" not in df.columns:
        df["freshness"] = math.nan
        return df

    def score_of(indexer: Any) -> float:
        return scores.get(indexer.lower(), 0.0) if isinstance(indexer, str) else 0.0

    df["freshness"] = df["indexer"].map(score_of).astype(float)
    return df


# The {chain: share} maps the job pushes for uptime and success rate, the column selection reads
# each from, and how many times the share that column holds it.
_BY_CHAIN_COLUMNS = (
    ("uptime_by_chain", cols.SEL_UPTIME_PERCENT, 100.0),
    ("success_rate_by_chain", cols.SEL_AVERAGE_STATUS, 1.0),
)


def _is_share(value: Any) -> TypeGuard[float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value <= 1


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _chain_shares(by_chain: pd.Series, chain_id: str) -> tuple[pd.Series, list[Any]]:
    """Each row's share on ``chain_id`` from its {chain: share} map, NaN where it has none, and
    what couldn't be read, which also gives NaN: a value for ``chain_id`` that isn't a number
    from 0 to 1, or a map that isn't one. A row without a map, as from a job that sends none,
    is neither.
    """
    shares: list[float] = []
    unusable: list[Any] = []
    for value in by_chain:
        share = value.get(chain_id) if isinstance(value, dict) else None
        if _is_share(share):
            shares.append(float(share))
            continue
        if isinstance(value, dict) and chain_id in value:
            unusable.append(share)
        elif not isinstance(value, dict) and not _is_missing(value):
            unusable.append(value)
        shares.append(math.nan)
    return pd.Series(shares, index=by_chain.index, dtype=float), unusable


def _with_chain_quality(history: pd.DataFrame, chain_id: str) -> pd.DataFrame:
    """``history`` with each indexer's uptime and success rate on ``chain_id`` where the job
    pushed one, and its overall values elsewhere, as for scores from a job that pushes none.
    """
    df = history.copy()
    for by_chain, column, factor in _BY_CHAIN_COLUMNS:
        if by_chain not in df.columns:
            continue
        shares, unusable = _chain_shares(df[by_chain], chain_id)
        if unusable:
            logger.warning(
                "%d %s entr(ies) for chain %s can't be read as a share from 0 to 1, so those "
                "indexers are scored on their overall value, such as %.100r",
                len(unusable),
                by_chain,
                chain_id,
                unusable[0],
            )
        elif shares.isna().all():
            logger.debug(
                "No indexer has a %s value on chain %s, so every one is scored on its overall "
                "value",
                by_chain,
                chain_id,
            )
        overall = df[column] if column in df.columns else math.nan
        df[column] = (shares * factor).where(shares.notna(), overall)
    return df


def _enrich_for_chain(history: pd.DataFrame, chain_id: str) -> pd.DataFrame:
    """``history`` with everything that depends on ``chain_id``: its prices, each indexer's
    freshness there, and its uptime and success rate there where the job measured them.
    """
    priced = _enrich_with_chain_prices(history, chain_id)
    return _with_chain_quality(_with_chain_freshness(priced, chain_id), chain_id)


def _indexer_prices(
    history: pd.DataFrame, idx_id: str, chain_id: str
) -> tuple[Optional[float], Optional[float]]:
    """Return (min_grt_per_30_days for the chain, min_grt_per_billion_entities_per_30_days)
    from the indexer's first row in ``history``.

    Both are None without a row; either is None when its column is missing. The
    per-entity price is also None when the indexer set none or set an invalid one.
    """
    rows = history[history["indexer"] == idx_id]
    if rows.empty:
        return None, None
    row = rows.iloc[0]

    min_grt = None
    if "dips_min_grt_per_30_days" in row.index:
        min_grt = _extract_chain_price(row["dips_min_grt_per_30_days"], chain_id)

    min_entity = None
    if _ENTITY_PRICE_COLUMN in row.index:
        min_entity = _parse_entity_price(row[_ENTITY_PRICE_COLUMN]).price

    return min_grt, min_entity


def _build_selected_indexers(
    indexer_ids: list[str],
    history: pd.DataFrame,
    chain_id: str,
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
        request.max_grt_per_billion_entities_per_30_days,
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
    stale_indexers: list[str],
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
        stale_indexers=cast(list[EthAddressStr], stale_indexers),
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
    ("norm_freshness", "freshness"),
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


def _members_to_drop(history: pd.DataFrame, request: SelectionRequest) -> dict[str, str]:
    """Group members that must leave, each with the reason: the request blocklists it, or its
    own DIPs info fails a price check. A member with no score row or no DIPs info stays, since
    a failed /dips/info fetch looks the same, and dipper cancels the agreement of any member
    left out.
    """
    members = request.existing_indexers or []
    blocked = set(request.blocklist or [])
    reasons = {member: "blocklisted" for member in members if member in blocked}
    if "dips_info_available" not in history.columns:
        return reasons
    rows = history[history["indexer"].isin(members) & ~history["indexer"].isin(list(reasons))]
    with_info, _ = _keep_with_dips_info(rows)
    passing, _ = _filter_by_price(
        with_info,
        request.chain_id,
        request.max_grt_per_30_days,
        request.max_grt_per_billion_entities_per_30_days,
    )
    for member in set(with_info["indexer"]) - set(passing["indexer"]):
        reasons[member] = "its advertised prices fail the price checks"
    return reasons


def _without_failing_members(history: pd.DataFrame, request: SelectionRequest) -> SelectionRequest:
    """The request with any group member that must leave taken out of existing_indexers."""
    dropped = _members_to_drop(history, request)
    if not dropped:
        return request
    for member, reason in sorted(dropped.items()):
        logger.info(
            "deployment=%s dropping group member %s: %s", request.deployment_id, member, reason
        )
    kept = [member for member in request.existing_indexers or [] if member not in dropped]
    return request.model_copy(update={"existing_indexers": kept})


def _current_minute() -> int:
    """This service's clock, as a minute counted from the Unix epoch."""
    return minute_index(datetime.now(timezone.utc))


def _enforcement() -> Enforcement:
    settings = get_settings()
    return Enforcement(
        settings.freshness_exempt_chains, settings.freshness_max_age_minutes, _current_minute()
    )


def _checks_too_old(request: SelectionRequest, enforcement: Enforcement) -> bool:
    """True when no check on the request's chain passed or failed recently enough for its
    marks to be trusted. A no_evidence check doesn't count here, as the prober works it out
    without asking the gateway. A chain whose checks did pass or fail but not within the limit,
    as when the prober stops or the gateway is down, gets a warning at most once every
    LAPSE_WARNING_INTERVAL_MINUTES, saying how many were held back since the last. Once its
    checks are current again that count starts afresh, so its next lapse warns at once. A chain
    with no such check in the HISTORY_DAYS kept, such as one the prober doesn't check, only gets
    a debug line.
    """
    last_answered = _state.freshness.last_answered_minute(request.chain_id)
    if enforcement.current(last_answered):
        _state.lapse_warnings.reset(request.chain_id)
        return False
    if last_answered is None or enforcement.now - last_answered > HISTORY_DAYS * MINUTES_PER_DAY:
        logger.debug(
            "deployment=%s chain=%s: freshness isn't enforced there, since no freshness check "
            "on it has passed or failed in the last %d days",
            request.deployment_id,
            request.chain_id,
            HISTORY_DAYS,
        )
        return True
    held = _state.lapse_warnings.allow(request.chain_id, enforcement.now)
    if held is None:
        return True
    logger.warning(
        "deployment=%s chain=%s isn't exempt but freshness isn't enforced there, since no check "
        "on it has passed or failed for %d minutes, over the limit of %d set by "
        "IISA_FRESHNESS_MAX_AGE_MINUTES; candidates are picked as if the chain were exempt. "
        "Logged at most once every %d minutes per chain, with %d held back since the last",
        request.deployment_id,
        request.chain_id,
        enforcement.now - last_answered,
        enforcement.max_age_minutes,
        LAPSE_WARNING_INTERVAL_MINUTES,
        held,
    )
    return True


def _stale_candidates(request: SelectionRequest, candidates: pd.DataFrame) -> list[str]:
    """Candidates marked stale on the request's chain, or with no freshness record there,
    unless that chain is exempt or its checks aren't recent enough to trust. Group members are
    never listed: a stale member keeps its place and still counts towards each metric's scale,
    as members do on an exempt chain.
    """
    enforcement = _enforcement()
    if not enforcement.enforced(request.chain_id) or _checks_too_old(request, enforcement):
        return []
    members = {member.lower() for member in request.existing_indexers or []}
    ids = [ix for ix in candidates.get("indexer", pd.Series(dtype=object)) if isinstance(ix, str)]
    pickable = [ix for ix in ids if ix.lower() not in members]
    stale = sorted(_state.freshness.stale_indexers(request.chain_id, pickable))
    logger.info(
        "deployment=%s chain=%s left out %d of %d candidates as stale",
        request.deployment_id,
        request.chain_id,
        len(stale),
        len(pickable),
    )
    return stale


class _Selection(NamedTuple):
    """A selection ready to score: the request without its failing members, the candidates'
    history with this chain's prices, the indexers that say they have synced the deployment,
    and the candidates marked stale, which can only fill places no fresh candidate can.
    """

    request: SelectionRequest
    history: pd.DataFrame
    synced: set[str]
    stale: list[str]


def _prepare_selection(request: SelectionRequest) -> Union[_Selection, SelectionResponse]:
    """Everything a selection needs before scoring, or the answer itself when there is nothing
    to score: no scores loaded, or no candidate within the price limits.
    """
    if _state.history is None:
        return SelectionResponse(deployment_id=request.deployment_id, indexers=[])

    request = _without_failing_members(_state.history, request)

    # Filter by price constraints before scoring. With no candidate left, the members that
    # may stay are the answer, since an empty one makes dipper cancel every agreement.
    filtered_history = _price_filtered_candidates(_state.history, request)
    if filtered_history is None:
        return SelectionResponse(
            deployment_id=request.deployment_id,
            indexers=_build_selected_indexers(
                list(request.existing_indexers or []), _state.history, request.chain_id
            ),
        )

    # Enrich with the chain's prices, freshness, uptime and success rate for normalization
    enriched_history = _enrich_for_chain(filtered_history, request.chain_id)
    stale = _stale_candidates(request, enriched_history)
    return _Selection(request, enriched_history, _synced_indexers_for(request.deployment_id), stale)


def _selection_response(selection: _Selection, processor: IndexerSelector) -> SelectionResponse:
    """The answer for the group the selector settled on, with each member's pricing."""
    request = selection.request
    _log_selection_reasoning(processor, request.deployment_id)
    _warn_about_stale_picks(request, processor.current_group, selection.stale)
    selected = _build_selected_indexers(
        list(processor.current_group), selection.history, request.chain_id
    )
    return SelectionResponse(deployment_id=request.deployment_id, indexers=selected)


def _warn_about_stale_picks(
    request: SelectionRequest, group: Sequence[str], stale: list[str]
) -> None:
    """Warn when places went to candidates marked stale, as they do once no fresh candidate
    is left, naming the chain so an operator can check its freshness checks."""
    marked = {indexer.lower() for indexer in stale}
    picked = [indexer for indexer in group if indexer.lower() in marked]
    if not picked:
        return
    logger.warning(
        "deployment=%s chain=%s no fresh candidate was left, so %d of %d places went to "
        "indexers marked stale: %s",
        request.deployment_id,
        request.chain_id,
        len(picked),
        request.num_candidates,
        ", ".join(picked),
    )


def _select_with_processor(request: SelectionRequest) -> SelectionResponse:
    """Run IndexerSelector and return a SelectionResponse for the deployment.

    The selector weights stake-to-fees, base price, latency, uptime, freshness,
    success rate, and price-per-entity; lower is better for prices and
    latency, higher for the rest.
    """
    selection = _prepare_selection(request)
    if isinstance(selection, SelectionResponse):
        return selection
    processor = _build_selector(
        selection.request, selection.history, selection.synced, selection.stale
    )
    return _selection_response(selection, processor)


async def _select_checking_sync_claims(request: SelectionRequest) -> SelectionResponse:
    """_select_with_processor, with the synced claims that could win a candidate a place
    checked through the gateway first, when the check is switched on.
    """
    selection = _prepare_selection(request)
    if isinstance(selection, SelectionResponse):
        return selection
    processor = _build_selector(
        selection.request, selection.history, selection.synced, selection.stale
    )
    if _state.sync_checker is not None:
        processor = await _with_claims_checked(selection, processor, _state.sync_checker)
    return _selection_response(selection, processor)


async def _with_claims_checked(
    selection: _Selection, processor: IndexerSelector, checker: SyncClaimChecker
) -> IndexerSelector:
    """Check, through the gateway and all at once, the claim of every candidate the selection
    could have picked ahead of the rest because it says it has synced, and select again if any
    of those claims was not confirmed or was left unasked.

    IndexerSelector._note_claims_that_could_win says which candidates those are. Members of
    the starting group are never asked, and nothing is asked when the selection found no such
    candidate. Beyond MAX_CHECKS_PER_SELECTION the best scores are asked and the rest are left
    unasked.

    The second selection counts the starting members' claims as given and, among candidates,
    only the claims that were confirmed. An indexer whose claim is not counted still competes
    on its score.
    """
    request = selection.request
    claimants = list(dict.fromkeys(indexer.lower() for indexer in processor.preferred_on_a_claim()))
    asked = claimants[:MAX_CHECKS_PER_SELECTION]
    not_asked = claimants[MAX_CHECKS_PER_SELECTION:]
    if not asked:
        return processor
    unconfirmed = await checker.unconfirmed(request.deployment_id, asked)
    _log_claim_checks(request.deployment_id, asked, unconfirmed, not_asked)
    if not unconfirmed and not not_asked:
        return processor
    members = {indexer.lower() for indexer in processor.initial_group}
    claims = {indexer.lower() for indexer in selection.synced}
    confirmed = set(asked) - unconfirmed.keys()
    return _build_selector(
        request, selection.history, (claims & members) | confirmed, selection.stale
    )


def _log_claim_checks(
    deployment_id: str, asked: list[str], unconfirmed: dict[str, str], not_asked: list[str]
) -> None:
    """Log how the checks went, with a warning for each claim that was set aside."""
    logger.info(
        "deployment=%s checked %d synced claim(s) through the gateway: %d confirmed",
        deployment_id,
        len(asked),
        len(asked) - len(unconfirmed),
    )
    for indexer, reason in sorted(unconfirmed.items()):
        logger.warning(
            "deployment=%s indexer %s says it has synced this deployment, but the gateway "
            "check did not confirm it: %s. Its claim is set aside for this selection.",
            deployment_id,
            indexer,
            reason,
        )
    if not_asked:
        logger.warning(
            "deployment=%s %d synced claim(s) beyond the limit of %d checks for 1 selection "
            "were not checked and are set aside",
            deployment_id,
            len(not_asked),
            MAX_CHECKS_PER_SELECTION,
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
