"""The record of 1 selection, and the most recent records kept in memory.

A selection's reply carries only the picked indexers and their prices. Its record holds the
reasoning: the request, every candidate with the score it was judged on, who was left out and
why, each change made to the group, and the final group. Each record is logged as 1 JSON line,
the durable copy, and the most recent are kept here for GET /selections. They are kept in this
process's memory only, so they are lost when the service restarts, and they are only complete
while 1 process answers both the selections and the reads, as the service runs today.
"""

import logging
import threading
from typing import Literal, Optional, get_args

from pydantic import BaseModel, Field

__all__ = [
    "LOG_PREFIX",
    "MAX_RECORDS",
    "MAX_RECORDS_PER_DEPLOYMENT",
    "MAX_RECORD_BYTES",
    "PRICE_CHECKS",
    "RecordedCandidate",
    "RecordedDecentralisation",
    "RecordedFilteredOut",
    "RecordedRequest",
    "RecordedStep",
    "RecordedSyncCheck",
    "SelectionRecord",
    "SelectionRecordStore",
    "record_json",
]

logger = logging.getLogger(__name__)

# How many records are kept: the most recent of each deployment, and the most recent in all.
MAX_RECORDS_PER_DEPLOYMENT = 20
MAX_RECORDS = 500
# The largest a record may be as JSON, room for a few hundred candidates at about 600 bytes
# each. /select-indexers takes no token and a request can name thousands of indexers, so this
# is what keeps the records held to MAX_RECORDS times it, 128 MB, whatever is sent.
MAX_RECORD_BYTES = 256 * 1024
# What each record's log line starts with, so the lines can be found and the JSON cut out.
LOG_PREFIX = "selection_record "

# The price checks, in the order they run. RecordedFilteredOut.reason says what each means.
PriceCheck = Literal[
    "no_dips_info",
    "chain_not_supported",
    "no_price_for_chain",
    "over_budget",
    "invalid_entity_price",
    "over_entity_budget",
]
PRICE_CHECKS: tuple[str, ...] = get_args(PriceCheck)
# Why an indexer is not in the final group. RecordedCandidate.reasons says what each means.
LeftOutReason = Literal[
    "blocklisted",
    "pending_agreement",
    "declined",
    "stale",
    "decentralisation",
    "group_full",
    "over_target",
    "replaced",
    "no_dips_info",
    "chain_not_supported",
    "no_price_for_chain",
    "over_budget",
    "invalid_entity_price",
    "over_entity_budget",
]
Pool = Literal["synced", "unsynced", "all"]
SyncStatus = Literal["not_synced", "synced", "confirmed", "unconfirmed", "set_aside"]


class RecordedRequest(BaseModel):
    """What the request gave selection to work with."""

    deployment_id: str
    chain_id: str
    num_candidates: int
    max_grt_per_30_days: Optional[float]
    max_grt_per_billion_entities_per_30_days: Optional[float]
    existing_indexers: list[str] = Field(
        description="The group as the caller sent it, before any member was taken out."
    )
    blocklist: list[str]
    pending_indexers: list[str] = Field(
        description="Every indexer with a pending agreement on any deployment, sorted: the part "
        "of the request's pending agreements that selection reads."
    )
    declined_indexers: list[str] = Field(description="The indexers that declined this deployment.")
    optimistic_dips_fees: dict[str, float]


class RecordedCandidate(BaseModel):
    """1 indexer selection scored, or 1 member of the group the caller sent."""

    indexer: str
    member: bool = Field(description="Whether the caller sent it as a member of the group.")
    picked: bool = Field(description="Whether the reply names it.")
    weighted_score: Optional[float] = Field(
        description="The total selection ranked the indexer on. For an indexer picked in this "
        "selection it is the total it was picked on. Null for a member with no score."
    )
    factors: dict[str, float] = Field(
        description="The 4 factors whose product is the total, as GET /scores/weighted names "
        "them. Empty for a member with no score."
    )
    components: dict[str, float] = Field(
        description="The parts behind the factors, as GET /scores/weighted names them, leaving "
        "out any that was not scored. Empty for a member with no score."
    )
    org: Optional[str] = Field(
        description="The operator selection spreads a group over; null when the scores hold none."
    )
    location: Optional[str] = Field(
        description="The place selection spreads a group over; null when the scores hold none."
    )
    sync_status: Optional[SyncStatus] = Field(
        description="Whether the loaded sync status has the indexer synced on the deployment, "
        "and what selection made of that claim. not_synced: no claim. synced: counted without "
        "a check. confirmed: checked through the gateway and counted. unconfirmed: checked, "
        "not confirmed and not counted. set_aside: not checked and not counted, when selection "
        "decided again after a failed check. Null when no selection ran."
    )
    stale: bool = Field(
        description="Held back as stale on the request's chain, to be picked only if no fresh "
        "candidate is left."
    )
    pool: Optional[Pool] = Field(
        description="The pool the indexer stood in when selection looked for an indexer to add "
        "or swap in: synced candidates are drawn from before unsynced ones, and with no synced "
        "claim in play there is 1 pool, all. It is the pool in the search that picked the "
        "indexer or, for one not picked, in the search for the last indexer picked. Null when "
        "the indexer could not be picked or nobody was picked."
    )
    reasons: list[LeftOutReason] = Field(
        description="Why the indexer is not in the final group; empty when it is. A member "
        "taken out before scoring is blocklisted or fails a price check, named as in "
        "filtered_out. A candidate could not be picked when it is blocklisted, has a "
        "pending_agreement on any deployment, declined this deployment, or is stale while a "
        "fresh candidate could fill the group. One that could be picked was passed over to "
        "keep the group spread over 2 operators and 2 locations (decentralisation), or the "
        "group filled without it (group_full). A member that left was the worst of a group "
        "over its target size (over_target) or was swapped for a much better candidate "
        "(replaced)."
    )
    unscored_reason: Optional[Literal[PriceCheck, "no_score_row"]] = Field(
        description="Why a member has no score: the price check that kept it out of scoring, "
        "named as in filtered_out, or no_score_row when the loaded scores have no row for it. "
        "Null for every scored indexer."
    )


class RecordedFilteredOut(BaseModel):
    """1 loaded indexer the price checks left out before scoring, and the check it failed."""

    indexer: str
    reason: PriceCheck = Field(
        description="no_dips_info: it did not answer its DIPs info probe. chain_not_supported: "
        "it does not support the request's chain. no_price_for_chain: it has no price there. "
        "over_budget: its price is over max_grt_per_30_days. invalid_entity_price: its "
        "per-entity price is not a number of 0 or more. over_entity_budget: that price is over "
        "max_grt_per_billion_entities_per_30_days."
    )


class RecordedStep(BaseModel):
    """1 change selection made to the group, in the order it happened."""

    action: Literal["added", "replaced", "removed"]
    indexer: str = Field(description="The indexer added, swapped in or removed.")
    replaced: Optional[str] = Field(description="The member a swap took out.")
    pool: Optional[Pool] = Field(
        description="The pool the indexer was drawn from; null for a removal."
    )
    weighted_score: Optional[float] = Field(
        description="The total the indexer was picked on. For a removal, the total it was "
        "decided on, which is 0 for a member with no score."
    )
    decentralisation: Optional[Literal["met", "fallback"]] = Field(
        description="met: the pick left the group spread over 2 operators and 2 locations, or "
        "the group is too small to need it. fallback: no candidate could, so the best total "
        "was taken regardless. Null for a removal."
    )
    passed_over: list[str] = Field(
        description="Candidates ranked ahead of the pick that were skipped because they would "
        "not have left the group spread."
    )


class RecordedSyncCheck(BaseModel):
    """How the gateway checks of synced claims went, when any claim was checked."""

    asked: list[str]
    unconfirmed: dict[str, str] = Field(
        description="Each claim that was not confirmed, with the reason."
    )
    not_asked: list[str] = Field(
        description="Claims beyond the limit of checks for 1 selection, set aside unchecked."
    )
    decided_again: bool = Field(
        description="True when a claim was unconfirmed or not asked, so selection decided again "
        "counting only the members' claims and the confirmed ones. The record is of that "
        "second decision."
    )


class RecordedDecentralisation(BaseModel):
    """How spread the final group is, by selection's own count of its scored members."""

    met: Optional[bool] = Field(
        description="Whether it holds at least 2 operators and 2 locations; null for a group "
        "of fewer than 2, which has no such rule to meet."
    )
    fell_back: bool = Field(description="True when at least 1 pick was a fallback.")


class SelectionRecord(BaseModel):
    """Everything 1 selection considered and decided."""

    selection_id: str
    selected_at: str = Field(description="When the selection was made, in UTC.")
    outcome: Literal["selected", "no_candidates", "nothing_asked_for", "no_scores"] = Field(
        description="selected: candidates were scored and picked from. no_candidates: no loaded "
        "indexer passed the price checks, so the members that may stay are the answer. "
        "nothing_asked_for: the request asked for 0 indexers or fewer. no_scores: no scores "
        "were loaded."
    )
    request: RecordedRequest
    candidates: list[RecordedCandidate] = Field(
        description="Every scored indexer, best total first, then each member with no score. "
        "Empty when the loaded scores were never read: nothing_asked_for and no_scores."
    )
    filtered_out: list[RecordedFilteredOut] = Field(
        description="Every other loaded indexer, with the price check that left it out, in the "
        "order the checks run and then by address."
    )
    steps: list[RecordedStep]
    sync_check: Optional[RecordedSyncCheck] = Field(
        description="Null when the check is off or no claim needed checking."
    )
    stale_fill: bool = Field(
        description="True when no fresh candidate was left, so candidates marked stale could "
        "fill places and everyone still to be picked was scored again with them setting each "
        "metric's scale."
    )
    final_group: list[str] = Field(description="The indexers the reply names, in its order.")
    decentralisation: Optional[RecordedDecentralisation] = Field(
        description="Null when no selection ran."
    )
    truncated: bool = Field(
        default=False,
        description="True when the full record was over the size limit, and every list in it "
        "was then left out.",
    )

    def header(self) -> "SelectionRecord":
        """The record without its lists: what was asked, when, and how it ended."""
        request = self.request.model_copy(
            update={
                "existing_indexers": [],
                "blocklist": [],
                "pending_indexers": [],
                "declined_indexers": [],
                "optimistic_dips_fees": {},
            }
        )
        return self.model_copy(
            update={
                "request": request,
                "candidates": [],
                "filtered_out": [],
                "steps": [],
                "sync_check": None,
                "final_group": [],
                "truncated": True,
            }
        )


def record_json(record: SelectionRecord) -> str:
    """The record as 1 line of JSON, cut down to its header, with a warning, when the full
    record is over MAX_RECORD_BYTES.
    """
    text = record.model_dump_json()
    size = len(text.encode())
    if size <= MAX_RECORD_BYTES:
        return text
    logger.warning(
        "The record of selection %s for deployment %s is %s bytes, over the limit of %s, so it "
        "is logged and kept without its lists of indexers",
        record.selection_id,
        record.request.deployment_id,
        f"{size:,}",
        f"{MAX_RECORD_BYTES:,}",
    )
    return record.header().model_dump_json()


class SelectionRecordStore:
    """The most recent selection records, as JSON: at most ``per_deployment`` for any 1
    deployment and ``total`` in all, the oldest dropped first. It has a lock of its own, since
    selections add records on the event loop while reads run on worker threads.
    """

    def __init__(
        self, per_deployment: int = MAX_RECORDS_PER_DEPLOYMENT, total: int = MAX_RECORDS
    ) -> None:
        self._per_deployment = per_deployment
        self._total = total
        self._lock = threading.Lock()
        # (deployment id, record), oldest first.
        self._records: list[tuple[str, str]] = []

    def add(self, deployment_id: str, record: str) -> None:
        """Keep ``record``, dropping the deployment's oldest record if it already holds its
        share, and then the oldest record of all if the store is over its total.
        """
        with self._lock:
            held = [entry for entry in self._records if entry[0] == deployment_id]
            if len(held) >= self._per_deployment:
                self._records.remove(held[0])
            self._records.append((deployment_id, record))
            del self._records[: max(0, len(self._records) - self._total)]

    def for_deployment(self, deployment_id: str) -> list[str]:
        """The deployment's records, newest first."""
        with self._lock:
            return [record for held, record in reversed(self._records) if held == deployment_id]
