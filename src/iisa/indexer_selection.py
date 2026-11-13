"""
Indexer selection algorithm for IISA.

Selects the best indexers for subgraphs based on weighted scoring of multiple
metrics including latency, uptime, freshness, success rate, and economic security.
"""

import logging
from types import MappingProxyType
from typing import Any, Callable, NewType, Optional, TypedDict, Union, cast

import numpy as np
import pandas as pd

from .score_columns import NORM_LAT_LIN_REG_COEFFICIENT, SEL_LATENCY_CI
from .share_score import shaped_share

__all__ = [
    "IndexerSelector",
    "read_metric",
    "DeploymentId",
    "IndexerId",
    "IpfsHashStr",
    "EthAddressStr",
    "QueryIdStr",
]

# Type aliases for domain concepts
QueryIdStr = NewType("QueryIdStr", str)
IpfsHashStr = NewType("IpfsHashStr", str)
DeploymentId = IpfsHashStr
EthAddressStr = NewType("EthAddressStr", str)
IndexerId = EthAddressStr

# Module-level logger
logger = logging.getLogger(__name__)

# Indexers scoring below this threshold are candidates for replacement.
# They will only be replaced if a significantly better candidate is available.
MIN_INDEXER_SCORE = 0.15

# Minimum score improvement required to justify replacing an underperforming indexer.
# Candidate must score at least (current_score + REPLACEMENT_MARGIN) to replace.
REPLACEMENT_MARGIN = 0.50

# Minimum weighted score for a synced indexer to be preferred over unsynced
# candidates. Below this threshold, sync status is ignored and the indexer
# competes on merit in the unsynced pool.
MIN_SYNCED_THRESHOLD = 0.60


class WeightsDict(TypedDict, total=False):
    """
    A dictionary containing weights for each metric used in the weighted score calculation.
    """

    stake_to_fees: float
    base_price_per_epoch: float
    lat_lin_reg_coefficient: float
    uptime_score: float
    freshness: float
    success_rate: float
    price_per_entity: float


DEFAULT_WEIGHTS = cast(
    WeightsDict,
    MappingProxyType(
        {
            "stake_to_fees": 0.25,
            "base_price_per_epoch": 0.25,
            "lat_lin_reg_coefficient": 0.10,
            "uptime_score": 0.15,
            "freshness": 0.15,
            "success_rate": 0.05,
            "price_per_entity": 0.05,
        }
    ),
)


class IndexerSelector:
    """Score and pick the best indexers for one subgraph deployment.

    Takes the DataManager's metrics, normalises them, combines them into a
    weighted score, and assigns or replaces indexers, honouring the denylist
    and decentralisation floor.

    One job lifetime: built for a single add-or-replace then discarded; whether
    it adds or replaces depends on how many indexers already serve the subgraph
    and how their quality compares to the best alternative.
    """

    def __init__(
        self,
        history: pd.DataFrame,
        deployment_id: DeploymentId,
        existing_agreements: Optional[dict[DeploymentId, list[IndexerId]]] = None,
        pending_agreements: Optional[dict[DeploymentId, list[IndexerId]]] = None,
        declined_indexers: Optional[dict[DeploymentId, list[IndexerId]]] = None,
        indexer_denylist: Optional[list[IndexerId]] = None,
        weights: Optional[WeightsDict] = None,
        target_size: int = 3,
        optimistic_dips_fees: Optional[dict[str, float]] = None,
        price_ceiling: Optional[float] = None,
        synced_indexers: Optional[set[IndexerId]] = None,
    ):
        """Initialize the selector and run scoring for one deployment.
        Non-obvious args: optimistic_dips_fees lowers stake_to_fees for indexers
        that already hold agreements (load-balancing), price_ceiling caps pricing
        normalisation so one outlier can't compress the rest, and synced_indexers
        are preferred so queries serve right after acceptance.
        """
        self.data = pd.DataFrame(history)
        self.deployment_id = deployment_id
        self.existing_agreements = existing_agreements or {}
        self.pending_agreements = pending_agreements or {}
        self.declined_indexers = declined_indexers or {}
        self.indexer_denylist = indexer_denylist or []
        self.weights = {**DEFAULT_WEIGHTS, **(weights or {})}
        self.target_size = target_size
        self.optimistic_dips_fees = optimistic_dips_fees or {}
        self.price_ceiling = price_ceiling
        self.synced_indexers = {s.lower() for s in synced_indexers} if synced_indexers else set()
        self.current_group: list[IndexerId] = []
        self.initial_group: list[IndexerId] = []
        # Every candidate that could, at some pick, have been chosen ahead of the rest because
        # it says it has synced.
        self._preferred_on_a_claim: set[IndexerId] = set()

        self._process_data()

        logger.info(
            "IndexerSelector completed: deployment=%s target=%d initial=%d final=%d",
            self.deployment_id,
            self.target_size,
            len(self.initial_group),
            len(self.current_group),
        )

    def update_indexer_denylist_cancel_indexing_agreements(self, indexer_denylist):
        """Cancel outstanding agreements for indexers on the denylist.

        Does not reassign; the periodic process_subgraph loop picks up the
        resulting under-staffed subgraphs. Returns a map of denied indexer to
        the subgraphs it was removed from.
        """
        #
        self.indexer_denylist = indexer_denylist

        cancelled_agreements = {}

        for subgraph, indexers in self.existing_agreements.items():
            for indexer in indexers:
                if indexer in indexer_denylist:
                    # If indexer not already in cancelled_agreements, create new key-value
                    if indexer not in cancelled_agreements:
                        cancelled_agreements[indexer] = []
                    # Add subgraphs the blocked indexer loses.
                    cancelled_agreements[indexer].append(subgraph)

        return cancelled_agreements

    def get_indexer_selections(self):
        """Return (added, removed) indexers as the diff of initial vs current group.

        Each is a {deployment_id: [indexers]} dict, empty when nothing changed.
        """
        # Compare initial and current groups to determine changes
        added = set(self.current_group) - set(self.initial_group)
        cancelled = set(self.initial_group) - set(self.current_group)

        # Create dictionaries with subgraph_id as key and list of indexers as value
        added_dict = {self.deployment_id: list(added)} if added else {}
        cancelled_dict = {self.deployment_id: list(cancelled)} if cancelled else {}

        # Return two separate dictionaries
        return added_dict, cancelled_dict

    def _process_data(self):
        """
        Process data by normalizing metrics and calculating weighted scores.
        """
        # Add dipper's expected fees from accepted agreements to the query fees
        # before normalisation, so stake_to_fees can tell indexers apart even
        # before on-chain payment claims appear.
        if self.optimistic_dips_fees and "total_query_fees" in self.data.columns:
            dips_adjustment = self.data["indexer"].map(self.optimistic_dips_fees).fillna(0.0)
            effective_fees = self.data["total_query_fees"].fillna(0.0) + dips_adjustment

            if "last_known_slashable_stake" in self.data.columns:
                self.data["stake_to_fees"] = self.data[
                    "last_known_slashable_stake"
                ] / effective_fees.replace(0.0, float("nan"))
                # Drop pre-normalised column so _normalize_metrics recomputes it
                self.data = self.data.drop(columns=["norm_stake_to_fees"], errors="ignore")

            adjusted_count = (dips_adjustment > 0).sum()
            logger.info(
                "deployment=%s applied optimistic DIPs fees to %d/%d indexers",
                self.deployment_id,
                adjusted_count,
                len(self.data),
            )

        # Get the current group of indexers for the subgraph using '_get_current_group'
        self.current_group = self._get_current_group()
        self.initial_group = list(self.current_group)
        logger.debug(
            "deployment=%s current_group=%s (%d indexers)",
            self.deployment_id,
            [addr[:10] for addr in self.current_group],
            len(self.current_group),
        )

        # Normalize metrics and calculate scores
        self.data = self._normalize_and_score()

        if not self.data.empty and "weighted_score" in self.data.columns:
            top = self.data.nlargest(5, "weighted_score")[["indexer", "weighted_score"]]
            logger.debug(
                "deployment=%s top_5_scores: %s",
                self.deployment_id,
                [
                    (row["indexer"][:10], round(row["weighted_score"], 4))
                    for _, row in top.iterrows()
                ],
            )

        # Call _assign_indexers_to_subgraph to assign/replace/remove an indexer on the subgraph.
        self._assign_indexers_to_subgraph()

    def _get_current_group(self):
        """
        Get the current group of indexers assigned to a subgraph.

        :return: Indexers assigned to self.deployment_id, or []. A list of its own, so the
            picks added to it later never reach the caller's list.
        """
        # Check if the subgraph_id exists in the agreements and return the corresponding indexers
        return list(self.existing_agreements.get(self.deployment_id, []))

    def _normalize_and_score(self):
        """Normalise quality metrics and compute the weighted score per indexer.

        Returns the processed DataFrame; on a normalisation or scoring error it
        falls back to raw data / NaN scores rather than raising.
        """
        try:
            normalized_data = _normalize_metrics(
                self.data, price_ceiling=self.price_ceiling, scale_rows=self._scale_rows()
            )
            logger.debug(
                "deployment=%s normalized %d indexers",
                self.deployment_id,
                len(normalized_data),
            )
        except Exception as e:
            logger.error(
                "Unexpected error when trying normalize_metrics(self.data): %s",
                e,
            )
            normalized_data = self.data

        try:
            normalized_data["weighted_score"] = _calculate_weighted_scores(
                normalized_data, self.weights
            )
        except Exception as e:
            logger.error(f"Unexpected error when trying calculate_weighted_score: {e}")
            normalized_data["weighted_score"] = np.nan

        return normalized_data

    def _scale_rows(self) -> Optional[pd.Series]:
        """Rows that set each metric's scale: the group plus every indexer this request can pick.

        Blocked and declined indexers are left out even when in the group, so one with an
        extreme value can't stretch a metric's scale for every real candidate.
        """
        if "indexer" not in self.data.columns:
            return None
        blocked = set(self.indexer_denylist) | set(
            self.declined_indexers.get(self.deployment_id, [])
        )
        excluded = (self._unpickable_indexers() - set(self.current_group)) | blocked
        return ~self.data["indexer"].isin(excluded)

    def _assign_indexers_to_subgraph(self):
        """
        Assign indexers to subgraph based on weighted scores and decentralization requirements.

        Use the methods _add_indexers_to_group and _replace_underperforming_indexers to
        assign indexers to the subgraph in question.
        """
        if len(self.current_group) < self.target_size:
            action = "add"
        elif len(self.current_group) > self.target_size:
            action = "remove"
        else:
            action = "replace_check"
        logger.info(
            "deployment=%s assigning: current_size=%d target_size=%d action=%s",
            self.deployment_id,
            len(self.current_group),
            self.target_size,
            action,
        )
        # Under-staffed: add indexers
        if len(self.current_group) < self.target_size:
            self._add_indexers_to_group()

        # Over-staffed: remove worst indexers
        if len(self.current_group) > self.target_size:
            self._remove_indexers_from_group()

        # At target: check for underperforming replacements
        if len(self.current_group) == self.target_size:
            self._replace_underperforming_indexers()

    def _add_indexers_to_group(self):
        """
        Add indexers to the group to meet the required number of indexers.
        """
        # While the group has less than target_size indexers, select the best indexer to add
        while len(self.current_group) < self.target_size:
            next_indexer = self._find_best_replacement_or_select_best_indexer()

            # Add the best indexer to the group
            if next_indexer:
                self.current_group.append(next_indexer)
                logger.info(
                    "deployment=%s added indexer %s to group (%d/%d)",
                    self.deployment_id,
                    next_indexer[:10],
                    len(self.current_group),
                    self.target_size,
                )

            # If there are no indexers available, do nothing.
            else:
                logger.info(
                    "deployment=%s no more candidates available, group is %d/%d",
                    self.deployment_id,
                    len(self.current_group),
                    self.target_size,
                )
                break

    def _meets_decentralization_requirements(
        self, new_indexer: IndexerId, replacing_indexer: Optional[IndexerId] = None
    ) -> bool:
        """Check the resulting group keeps >=2 unique orgs and >=2 locations.

        Builds the group as current + new (or, when replacing_indexer is set,
        current - replacing + new). Groups of fewer than 2 indexers always pass.
        Returns True when the decentralisation floor is met.
        """
        # Build the resulting group
        if replacing_indexer:
            # Replacement scenario: remove old, add new
            new_group = [i for i in self.current_group if i != replacing_indexer] + [new_indexer]
        else:
            # Addition scenario: just add new
            new_group = self.current_group + [new_indexer]

        # If resulting group has fewer than 2 indexers, no decentralisation check needed
        if len(new_group) < 2:
            return True

        locations, orgs = self._count_locations_and_orgs(new_group)

        # Return True if decentralisation requirements are met
        meets = locations >= 2 and orgs >= 2
        if not meets:
            logger.debug(
                "deployment=%s decentralization check failed for %s: "
                "locations=%d orgs=%d (need 2 each)",
                self.deployment_id,
                new_indexer[:10],
                locations,
                orgs,
            )
        return meets

    def _count_locations_and_orgs(self, group: list[IndexerId]) -> tuple[int, int]:
        """Count the distinct locations and orgs among the group's rows. Values are compared
        as text, since a pushed list or object cannot be compared as it is.
        """
        rows = self.data[self.data["indexer"].isin(group)]
        return (
            rows["destination_loc"].astype(str).nunique(dropna=False),
            rows["org"].astype(str).nunique(dropna=False),
        )

    def _remove_indexers_from_group(self):
        """
        Remove worst indexers until group has target_size indexers.
        """
        while len(self.current_group) > self.target_size:
            indexer_scores = []
            for indexer in self.current_group:
                row = self.data[self.data["indexer"] == indexer]
                score = (
                    row["weighted_score"].iloc[0]
                    if not row.empty and "weighted_score" in row.columns
                    else 0.0
                )
                indexer_scores.append((indexer, score))

            # Sort by score, worst (lowest) first
            indexer_scores.sort(key=lambda x: x[1])

            for indexer, score in indexer_scores:
                temp_group = self.current_group.copy()
                temp_group.remove(indexer)

                if self._meets_decentralization_requirements_indexer_removal(temp_group):
                    self.current_group.remove(indexer)
                    logger.info(
                        "deployment=%s removed worst indexer %s (score=%.4f, group now %d/%d)",
                        self.deployment_id,
                        indexer[:10],
                        score,
                        len(self.current_group),
                        self.target_size,
                    )
                    break
            else:
                break

    def _meets_decentralization_requirements_indexer_removal(self, group):
        """
        Check if the group meets decentralisation requirements after removing an indexer.

        The group must have at least 2 unique organizations and 2 unique locations.
        """
        if len(group) < 2:
            return False

        locations, orgs = self._count_locations_and_orgs(group)

        # Return 'True' if decentralisation requirements are hit
        if locations >= 2 and orgs >= 2:
            return True

        # Otherwise 'False'
        return False

    def _replace_underperforming_indexers(self):
        """Replace indexers below MIN_INDEXER_SCORE with much better candidates.
        Only a poor performer (< MIN_INDEXER_SCORE) is swapped, and only for a
        candidate scoring at least REPLACEMENT_MARGIN higher, so adequate indexers
        stay put and marginal churn is avoided. A bad indexer with no good
        replacement is kept. Loops until no beneficial swap remains.
        """
        # Track indexers added in this call - don't replace them
        added_this_call: set[IndexerId] = set()

        while True:
            best_swap = self._find_best_swap(added_this_call)

            if best_swap:
                old_indexer, new_indexer, improvement = best_swap
                self.current_group.remove(old_indexer)
                self.current_group.append(new_indexer)
                added_this_call.add(new_indexer)
                logger.info(
                    "deployment=%s replaced indexer %s with %s (improvement=%.4f)",
                    self.deployment_id,
                    old_indexer[:10],
                    new_indexer[:10],
                    improvement,
                )
            else:
                logger.debug(
                    "deployment=%s no more beneficial replacements found",
                    self.deployment_id,
                )
                break  # No more beneficial replacements available

    def _score_of(self, indexer: IndexerId) -> Optional[float]:
        """Weighted score from the indexer's first row, or None when it has no row."""
        rows = self.data[self.data["indexer"] == indexer]
        if rows.empty:
            return None
        return cast(float, rows["weighted_score"].iloc[0])

    def _find_best_swap(
        self, added_this_call: set[IndexerId]
    ) -> Optional[tuple[IndexerId, IndexerId, float]]:
        """Return (existing, candidate, improvement) for the group member whose
        replacement gains the most, or None when no member has a beneficial swap.
        Members in added_this_call are not replaced. On equal improvement the
        member earlier in the group wins.
        """
        best_swap: Optional[tuple[IndexerId, IndexerId, float]] = None
        for existing_indexer in self.current_group:
            # Don't replace indexers we just added in this call
            if existing_indexer in added_this_call:
                continue
            swap = self._swap_for(existing_indexer)
            if swap is not None and (best_swap is None or swap[2] > best_swap[2]):
                best_swap = swap
        return best_swap

    def _swap_for(
        self, existing_indexer: IndexerId
    ) -> Optional[tuple[IndexerId, IndexerId, float]]:
        """Return (existing_indexer, candidate, improvement) when existing_indexer
        scores below MIN_INDEXER_SCORE and a candidate beats it by more than
        REPLACEMENT_MARGIN; None otherwise.

        A synced candidate is preferred. When that one gains too little, the best candidate
        whatever its sync status is tried, so a synced candidate that falls short does not
        end the search for this indexer's replacement.
        """
        current_score = self._score_of(existing_indexer)
        if current_score is None:
            return None

        # Only consider replacing indexers below the minimum threshold
        if current_score >= MIN_INDEXER_SCORE:
            logger.debug(
                "deployment=%s indexer %s score=%.4f >= threshold=%.2f, no replacement needed",
                self.deployment_id,
                existing_indexer[:10],
                current_score,
                MIN_INDEXER_SCORE,
            )
            return None

        # Find the best candidate for replacing this specific indexer, preferring a synced one
        # first and then, if that gains too little, picking as if nobody were synced.
        for prefer_synced in (True, False) if self.synced_indexers else (True,):
            candidate = self._find_best_replacement_or_select_best_indexer(
                replacing_indexer=existing_indexer, prefer_synced=prefer_synced
            )
            if not candidate:
                continue
            candidate_score = self._score_of(candidate)
            # Only replace if candidate is significantly better
            if candidate_score is not None and candidate_score > current_score + REPLACEMENT_MARGIN:
                return existing_indexer, candidate, candidate_score - current_score
        return None

    def _unpickable_indexers(self) -> set[IndexerId]:
        """Indexers this request can't pick: already in the group, on the denylist, with a
        pending (unaccepted) agreement on any subgraph, or that declined this subgraph.
        """
        return set(
            self.current_group
            + self.indexer_denylist
            + [indexer for pending in self.pending_agreements.values() for indexer in pending]
            + self.declined_indexers.get(self.deployment_id, [])
        )

    def preferred_on_a_claim(self) -> list[IndexerId]:
        """Every candidate that this selection could have picked ahead of the rest because it
        says it has synced the deployment, best score first, whether or not it was picked.
        Empty when the selection never looked for an indexer to add or swap in. See
        _note_claims_that_could_win for which candidates those are. Members of the starting
        group are never listed, even one that left the group and was weighed as a candidate.
        """
        members = {member.lower() for member in self.initial_group}
        listed = {i for i in self._preferred_on_a_claim if i.lower() not in members}
        rows = self.data[self.data["indexer"].isin(listed)]
        ranked = rows.sort_values(by="weighted_score", ascending=False)
        return cast(list[IndexerId], list(dict.fromkeys(ranked["indexer"])))

    def _note_claims_that_could_win(
        self, candidates: pd.DataFrame, replacing_indexer: Optional[IndexerId]
    ) -> None:
        """Record the candidates that this pick could choose because they say they have synced.

        They are the candidates drawn from first (see _synced_first) that would also leave the
        group meeting the decentralisation requirements and, when a member is being replaced,
        beat it by more than REPLACEMENT_MARGIN. Any other candidate can only be picked on its
        score, so whether its claim is true changes nothing here.
        """
        if not self.synced_indexers:
            return
        ahead = self._synced_first(candidates)
        if replacing_indexer is not None:
            replaced_score = self._score_of(replacing_indexer)
            if replaced_score is None:
                return
            ahead = ahead[ahead["weighted_score"] > replaced_score + REPLACEMENT_MARGIN]
        self._preferred_on_a_claim.update(
            indexer
            for indexer in ahead["indexer"]
            if self._meets_decentralization_requirements(indexer, replacing_indexer)
        )

    def _find_best_replacement_or_select_best_indexer(
        self, replacing_indexer: Optional[IndexerId] = None, prefer_synced: bool = True
    ) -> Optional[IndexerId]:
        """Pick the highest-scoring eligible indexer to add or to replace one.
        Skips indexers already in the group, on the denylist, with pending
        (unaccepted) agreements, or that previously declined this subgraph.
        replacing_indexer, when set, simulates the swap for the decentralisation
        check. prefer_synced, when False, ranks candidates on score alone, as if
        none were synced. Returns the best indexer ID, or None if none qualify.
        """
        unpickable_indexers = self._unpickable_indexers()
        logger.debug(
            "deployment=%s unpickable: %d in_group, %d denylisted, %d pending, %d declined",
            self.deployment_id,
            len(self.current_group),
            len(self.indexer_denylist),
            sum(len(v) for v in self.pending_agreements.values()),
            len(self.declined_indexers.get(self.deployment_id, [])),
        )

        # The candidates we could select are those that are not unpickable
        candidates = self.data[~self.data["indexer"].isin(unpickable_indexers)]

        # Sort the candidates by weighted score, highest score first
        candidates = candidates.sort_values(by="weighted_score", ascending=False)
        self._note_claims_that_could_win(candidates, replacing_indexer)
        logger.debug(
            "deployment=%s candidates: %d eligible out of %d total (excluded %d unpickable)",
            self.deployment_id,
            len(candidates),
            len(self.data),
            len(unpickable_indexers),
        )

        # Iterate pools in order, checking decentralisation
        pools = self._candidate_pools(candidates) if prefer_synced else [("all", candidates)]
        for pool_name, pool_df in pools:
            for indexer in pool_df["indexer"]:
                if self._meets_decentralization_requirements(
                    indexer, replacing_indexer=replacing_indexer
                ):
                    score = pool_df[pool_df["indexer"] == indexer]["weighted_score"].iloc[0]
                    logger.info(
                        "deployment=%s selected %s indexer %s (score=%.4f, meets decentralization)",
                        self.deployment_id,
                        pool_name,
                        indexer[:10],
                        score,
                    )
                    return cast(EthAddressStr, indexer)

        # Fallback: best scorer regardless of decentralisation or
        # sync status. Decentralisation is best-effort.
        if not candidates.empty:
            fallback = candidates["indexer"].iloc[0]
            fallback_score = candidates["weighted_score"].iloc[0]
            logger.debug(
                "deployment=%s no candidate meets decentralization, "
                "falling back to best scorer %s (score=%.4f)",
                self.deployment_id,
                fallback[:10],
                fallback_score,
            )
            return cast(EthAddressStr, fallback)

        logger.info(
            "deployment=%s zero candidates remain after filtering %d total indexers",
            self.deployment_id,
            len(self.data),
        )
        return None

    def _candidate_pools(self, candidates: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
        """Split score-sorted candidates into named pools, drawn from in order.

        Prefer already-synced candidates so queries serve right after
        acceptance: the first synced indexer gets in at any score, later ones
        must beat MIN_SYNCED_THRESHOLD or fall back to competing on merit.
        """
        if not self.synced_indexers:
            return [("all", candidates)]

        synced = self._synced_first(candidates)
        unsynced = candidates[~candidates["indexer"].isin(synced["indexer"])]
        logger.info(
            "deployment=%s candidates: %d synced eligible, %d unsynced (group_has_synced=%s)",
            self.deployment_id,
            len(synced),
            len(unsynced),
            self._group_has_synced(),
        )
        return [("synced", synced), ("unsynced", unsynced)]

    def _group_has_synced(self) -> bool:
        """Whether anyone in the group as it stands, earlier picks included, says it has synced."""
        return bool({i.lower() for i in self.current_group} & self.synced_indexers)

    def _synced_first(self, candidates: pd.DataFrame) -> pd.DataFrame:
        """The candidates drawn from first because they say they have synced the deployment."""
        all_synced_candidates = candidates[
            candidates["indexer"].str.lower().isin(self.synced_indexers)
        ]
        if self._group_has_synced():
            # Already have a synced indexer: threshold applies
            return all_synced_candidates[
                all_synced_candidates["weighted_score"] >= MIN_SYNCED_THRESHOLD
            ]
        # No synced indexer yet: first one gets in at any score
        return all_synced_candidates


# Norm columns the CronJob ships already normalised and the selector keeps: latency, and
# stake_to_fees in degraded mode (a neutral 0.5). Every other metric in _normalize_metrics is
# recomputed from its raw value, so a pre-scaled copy can't replace the selector's own scaling.
_PRECOMPUTED_NORM_COLUMNS = frozenset({NORM_LAT_LIN_REG_COEFFICIENT, "norm_stake_to_fees"})

# Norm columns left missing rather than scored 0 when there is nothing to score, so the weighted
# score leaves them out: freshness, on a chain whose checks don't yet cover 7 days.
_LEFT_OUT_WHEN_MISSING = frozenset({"norm_freshness"})

# A normaliser takes a metric's values and, optionally, a mask of the rows whose values set its
# scale (best, worst, cutoff). Rows outside the mask are scored on that scale, clipped to [0, 1].
_Normaliser = Callable[[pd.Series, Optional[pd.Series]], Union[pd.Series, float]]


def _normalize_metrics(
    merged: pd.DataFrame,
    price_ceiling: Optional[float] = None,
    scale_rows: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """Normalise each metric to [0, 1] where 1 is best (lower-is-better inverted).

    price_ceiling, when given, caps pricing instead of the observed max. scale_rows, when given,
    marks the rows that set each metric's best and worst; other rows are scored against them.
    Missing metrics score 0, except those in _LEFT_OUT_WHEN_MISSING, which stay missing, and a
    value that does not read as a number, such as text, scores 0. Input norm_ columns outside
    _PRECOMPUTED_NORM_COLUMNS are recomputed; those inside score 0 when outside [0, 1].
    """
    # (norm column, source column, normaliser), in the order the columns are added.
    metrics: list[tuple[str, str, _Normaliser]] = [
        (NORM_LAT_LIN_REG_COEFFICIENT, SEL_LATENCY_CI, _normalize_latency),
        # higher is better
        ("norm_uptime_score", "% up_x", _normalize_uptime),
        ("norm_stake_to_fees", "stake_to_fees", _normalize_stake_to_fees),
        # higher is better
        ("norm_success_rate", "average_status", _normalize_success_rate),
        (
            "norm_base_price_per_epoch",
            "base_price_per_epoch",
            lambda prices, rows: _normalize_base_price_per_epoch(prices, price_ceiling, rows),
        ),
        ("norm_price_per_entity", "price_per_entity", _normalize_price_per_entity),
        # higher is better
        ("norm_freshness", "freshness", _normalize_freshness),
    ]

    if merged.empty:
        for norm_col, _, _ in metrics:
            merged[norm_col] = pd.Series(dtype=float)
        return merged

    # With no row left to set the scale, scale against every row.
    if scale_rows is not None and not scale_rows.any():
        scale_rows = None

    for norm_col, source_col, normalize in metrics:
        if norm_col in _PRECOMPUTED_NORM_COLUMNS and norm_col in merged.columns:
            merged[norm_col] = _carried_scores(merged, norm_col)
        elif source_col in merged.columns:
            merged[norm_col] = _normalize_column(merged, source_col, normalize, scale_rows)
        else:
            merged[norm_col] = np.nan

    norm_columns = [
        col
        for col in merged.columns
        if col.startswith("norm_") and col not in _LEFT_OUT_WHEN_MISSING
    ]
    merged[norm_columns] = merged[norm_columns].fillna(0)

    return merged


def _normalize_column(
    merged: pd.DataFrame,
    column: str,
    normalize: _Normaliser,
    scale_rows: Optional[pd.Series] = None,
) -> pd.Series:
    """Normalise one metric column after reading it as numbers. An unreadable value scores 0
    and is left out of the normalisation, so it cannot move any other indexer's score.
    """
    numbers, unreadable = read_metric(merged, column)
    readable = ~unreadable.to_numpy()
    rows = None if scale_rows is None else scale_rows[readable]
    # With every row that sets the scale unreadable, scale against the readable rows.
    if rows is not None and not rows.any():
        rows = None
    scores = np.zeros(len(merged))
    scores[readable] = normalize(numbers[readable], rows)
    return pd.Series(scores, index=merged.index)


def _carried_scores(merged: pd.DataFrame, column: str) -> pd.Series:
    """Read a norm_ column the input already carries. A value that is not a finite number in
    [0, 1] scores 0, so a corrupt pushed score cannot outrank every other indexer.
    """
    numbers, unreadable = read_metric(merged, column)
    outside = numbers.notna() & ~numbers.between(0, 1)
    if outside.any():
        _warn_scored_as_zero(merged, column, outside, "fall outside [0, 1]")
    return numbers.mask(unreadable | outside, 0.0)


def read_metric(merged: pd.DataFrame, column: str) -> tuple[pd.Series, pd.Series]:
    """Return (numbers, unreadable) for a metric column read as floats.

    None and NaN stay NaN, as missing. Any other value that does not read as a
    number, such as text, also becomes NaN and is flagged in ``unreadable``.
    """
    values = merged[column]
    if pd.api.types.is_numeric_dtype(values):
        return values.astype(float), pd.Series(False, index=values.index)
    numbers = values.map(_float_or_nan).astype(float)
    unreadable = values.notna() & numbers.isna()
    if unreadable.any():
        _warn_scored_as_zero(merged, column, unreadable, "do not read as numbers")
    return numbers, unreadable


def _float_or_nan(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return float("nan")


def _warn_scored_as_zero(merged: pd.DataFrame, column: str, rows: pd.Series, why: str) -> None:
    """Log the indexers whose ``column`` value scores 0 because of ``why``."""
    indexers = merged.loc[rows, "indexer"].tolist() if "indexer" in merged.columns else []
    examples = merged.loc[rows, column].head(3).tolist()
    logger.warning(
        "scoring %d %r value(s) as 0 because they %s: indexers=%.300s examples=%.200r",
        int(rows.sum()),
        column,
        why,
        indexers,
        examples,
    )


def _scale_values(values: pd.Series, scale_rows: Optional[pd.Series]) -> pd.Series:
    """The values that set a metric's scale: only scale_rows when given, otherwise all."""
    return values if scale_rows is None else values[scale_rows]


def _normalize_latency(latency: pd.Series, scale_rows: Optional[pd.Series] = None) -> pd.Series:
    """Normalise the latency linear regression score. A missing latency scores 0, the worst."""
    # lower is better
    return (1 - _normalize_generic(latency, scale_rows)).mask(latency.isna(), 0.0)


def _normalize_stake_to_fees(
    stake_to_fees: pd.Series, scale_rows: Optional[pd.Series] = None
) -> pd.Series:
    """Normalise stake to fees ratio (higher = more capacity = better).

    Indexers with zero fees get NaN (infinite ratio = maximum capacity).
    Fill NaN above the max finite value so they normalise to 1.0.
    """
    stf = stake_to_fees.copy()
    finite_max = _scale_values(stf, scale_rows).max()
    fill_value = (finite_max + 1.0) if pd.notna(finite_max) else 1.0
    stf = stf.fillna(fill_value)
    # When every indexer on the scale has zero fees the scale is flat and min-max gives 0, so
    # set zero-fee rows to 1.0 directly.
    return _normalize_generic(stf, scale_rows).where(stake_to_fees.notna(), 1.0)


def _normalize_base_price_per_epoch(
    base_price_per_epoch: pd.Series,
    price_ceiling: Optional[float],
    scale_rows: Optional[pd.Series] = None,
) -> Union[pd.Series, float]:
    """Normalize base price per epoch (lower is better).

    When price_ceiling is provided, use it instead of the observed max
    so outlier prices cannot compress differentiation among others.
    """
    prices = pd.to_numeric(base_price_per_epoch, errors="coerce").fillna(0.0).clip(lower=0)
    scale = _scale_values(prices, scale_rows)
    ceiling = price_ceiling if price_ceiling is not None and price_ceiling > 0 else scale.max()
    if ceiling > 0 and ceiling > scale.min():
        return (1 - (prices / ceiling)).clip(lower=0)
    return 0.5


def _normalize_price_per_entity(
    price_per_entity: pd.Series, scale_rows: Optional[pd.Series] = None
) -> pd.Series:
    """Normalize price per entity (lower is better).

    price_ceiling applies to base epoch price; entity pricing uses observed max since there is
    no separate budget field for it. The service turns a missing price into 0 before this, so
    a value that is not a finite number >= 0 is unreadable and gets 0, the worst score.
    """
    prices = pd.to_numeric(price_per_entity, errors="coerce").astype(float)
    prices = prices.where(np.isfinite(prices) & (prices >= 0))
    scale = _scale_values(prices, scale_rows)
    ceiling = scale.max()
    if ceiling > 0 and ceiling > scale.min():
        scores = cast(pd.Series, (1 - (prices / ceiling)).clip(lower=0))
    else:
        scores = pd.Series(0.5, index=prices.index)
    return scores.where(prices.notna(), 0.0)


def _normalize_generic(series: pd.Series, scale_rows: Optional[pd.Series] = None) -> pd.Series:
    """Min-max normalise a Series to [0, 1].

    NaN and division-by-zero (a constant series, where max == min) both fill
    to 0.
    """
    scale = _scale_values(series, scale_rows)
    min_val = scale.min()
    max_val = scale.max()

    # Normalize to between 0 and 1 range, clipping rows that fall outside the scale.
    normalized = ((series - min_val) / (max_val - min_val)).clip(0, 1)

    # Handle any potential NaN values
    normalized = normalized.fillna(0)

    return cast(pd.Series, normalized)


def _normalize_uptime(
    uptime_percent: pd.Series, _scale_rows: Optional[pd.Series] = None
) -> pd.Series:
    """Score uptime on the indexer's own share of time up; ``% up_x`` holds it times 100."""
    return _normalize_share(uptime_percent / 100)


def _normalize_success_rate(
    success_rate: pd.Series, _scale_rows: Optional[pd.Series] = None
) -> pd.Series:
    """Score success rate, already a share from 0 to 1, on the indexer's own share."""
    return _normalize_share(success_rate)


def _normalize_share(share: pd.Series) -> pd.Series:
    """A share scored on its own, with no reference to the other candidates: clipped to [0, 1]
    and shaped by shaped_share, so 0.9 scores about 0.53 and 0.8 about 0.26. A missing value
    scores 0.
    """
    return shaped_share(share.clip(0, 1)).fillna(0.0)


def _normalize_freshness(
    freshness: pd.Series, _scale_rows: Optional[pd.Series] = None
) -> pd.Series:
    """Freshness arrives already scored on the indexer's own share of checks passed (see
    freshness.freshness_score), so it is only clipped to [0, 1]. NaN, for a chain where
    freshness isn't weighed yet, stays NaN.
    """
    return freshness.clip(0, 1)


def _calculate_weighted_scores(df: pd.DataFrame, weights: WeightsDict) -> pd.Series:
    """Weighted average of each row's present norm_* metrics by `weights`.
    NaN cells drop out of both the numerator and the per-row weight total, so each
    row renormalises over the metrics it has. Both sums add their terms in the same
    order, so scores within [0, 1] cannot round to an average above 1.
    Raises ValueError if any row has no usable column.
    """
    columns = []
    column_weights = []
    missing_columns = []
    for metric, weight in cast(dict[str, float], weights).items():
        column_name = f"norm_{metric}"
        if column_name in df.columns:
            columns.append(column_name)
            column_weights.append(weight)
        else:
            missing_columns.append(column_name)

    if missing_columns:
        logger.warning(f"Missing columns in input data: {', '.join(missing_columns)}")

    weight_vector = np.array(column_weights, dtype=float)
    matrix = df[columns].to_numpy(dtype=float)
    present = ~np.isnan(matrix)
    weighted_sum = (np.where(present, matrix, 0.0) * weight_vector).sum(axis=1)
    weight_total = (present * weight_vector).sum(axis=1)  # weights of the columns each row has

    if (weight_total == 0).any():
        logger.error("Total sum of weights is 0. Sum of weights should be non-zero, ideally 1.")
        raise ValueError("Total weight cannot be 0.")

    return pd.Series(weighted_sum / weight_total, index=df.index)
