"""
Indexer selection algorithm for IISA.

Selects the best indexers for subgraphs based on weighted scoring of multiple
metrics including latency, uptime, success rate, and economic security.
"""

import logging
from types import MappingProxyType
from typing import Callable, NewType, Optional, TypedDict, Union, cast

import numpy as np
import pandas as pd

__all__ = [
    "IndexerSelector",
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

NON_ZERO_UPTIME_SUCCESS_RATE_SCORE_THRESHOLD = 0.97

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
    success_rate: float
    price_per_entity: float


DEFAULT_WEIGHTS = cast(
    WeightsDict,
    MappingProxyType(
        {
            "stake_to_fees": 0.30,
            "base_price_per_epoch": 0.25,
            "lat_lin_reg_coefficient": 0.20,
            "uptime_score": 0.15,
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
                self.data.drop(
                    columns=["norm_stake_to_fees"],
                    errors="ignore",
                    inplace=True,
                )

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

        :return: Indexers assigned to self.deployment_id, or [].
        """
        # Check if the subgraph_id exists in the agreements and return the corresponding indexers
        return self.existing_agreements.get(self.deployment_id, [])

    def _normalize_and_score(self):
        """Normalise quality metrics and compute the weighted score per indexer.

        Returns the processed DataFrame; on a normalisation or scoring error it
        falls back to raw data / NaN scores rather than raising.
        """
        try:
            normalized_data = _normalize_metrics(self.data, price_ceiling=self.price_ceiling)
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

        # Get unique locations and organizations for the resulting group
        locations = self.data[self.data["indexer"].isin(new_group)]["destination_loc"].unique()
        orgs = self.data[self.data["indexer"].isin(new_group)]["org"].unique()

        # Return True if decentralisation requirements are met
        meets = len(locations) >= 2 and len(orgs) >= 2
        if not meets:
            logger.debug(
                "deployment=%s decentralization check failed for %s: "
                "locations=%d orgs=%d (need 2 each)",
                self.deployment_id,
                new_indexer[:10],
                len(locations),
                len(orgs),
            )
        return meets

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

        locations = self.data[self.data["indexer"].isin(group)]["destination_loc"].unique()
        orgs = self.data[self.data["indexer"].isin(group)]["org"].unique()

        # Return 'True' if decentralisation requirements are hit
        if len(locations) >= 2 and len(orgs) >= 2:
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
            best_swap: Optional[tuple[IndexerId, IndexerId, float]] = None

            for existing_indexer in self.current_group:
                # Don't replace indexers we just added in this call
                if existing_indexer in added_this_call:
                    continue

                # Get current indexer's score
                indexer_data = self.data[self.data["indexer"] == existing_indexer]
                if indexer_data.empty:
                    continue

                current_score = indexer_data["weighted_score"].iloc[0]

                # Only consider replacing indexers below the minimum threshold
                if current_score >= MIN_INDEXER_SCORE:
                    logger.debug(
                        "deployment=%s indexer %s score=%.4f "
                        ">= threshold=%.2f, no replacement needed",
                        self.deployment_id,
                        existing_indexer[:10],
                        current_score,
                        MIN_INDEXER_SCORE,
                    )
                    continue

                # Find best candidate for replacing this specific indexer
                candidate = self._find_best_replacement_or_select_best_indexer(
                    replacing_indexer=existing_indexer
                )

                if not candidate:
                    continue

                # Get candidate's score
                candidate_data = self.data[self.data["indexer"] == candidate]
                if candidate_data.empty:
                    continue

                candidate_score = candidate_data["weighted_score"].iloc[0]

                # Only replace if candidate is significantly better
                if candidate_score > current_score + REPLACEMENT_MARGIN:
                    improvement = candidate_score - current_score
                    if best_swap is None or improvement > best_swap[2]:
                        best_swap = (existing_indexer, candidate, improvement)

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

    def _find_best_replacement_or_select_best_indexer(
        self, replacing_indexer: Optional[IndexerId] = None
    ) -> Optional[IndexerId]:
        """Pick the highest-scoring eligible indexer to add or to replace one.
        Skips indexers already in the group, on the denylist, with pending
        (unaccepted) agreements, or that previously declined this subgraph.
        replacing_indexer, when set, simulates the swap for the decentralisation
        check. Returns the best indexer ID, or None if none qualify.
        """

        def flatten_list_of_lists(list_of_lists):
            flattened_list = []
            for sublist in list_of_lists:
                for item in sublist:
                    flattened_list.append(item)
            return flattened_list

        unpickable_indexers = set(
            self.current_group
            + self.indexer_denylist
            + flatten_list_of_lists(self.pending_agreements.values())
            + self.declined_indexers.get(self.deployment_id, [])
        )
        logger.debug(
            "deployment=%s unpickable: %d in_group, %d denylisted, %d pending, %d declined",
            self.deployment_id,
            len(self.current_group),
            len(self.indexer_denylist),
            sum(len(v) for v in self.pending_agreements.values()),
            len(self.declined_indexers.get(self.deployment_id, [])),
        )

        # The candidates we could select are those that are not unpickable
        candidates = self.data[~self.data["indexer"].isin(unpickable_indexers)].copy()

        # Sort the candidates by weighted score, highest score first
        candidates.sort_values(by="weighted_score", ascending=False, inplace=True)
        logger.debug(
            "deployment=%s candidates: %d eligible out of %d total (excluded %d unpickable)",
            self.deployment_id,
            len(candidates),
            len(self.data),
            len(unpickable_indexers),
        )

        # Prefer already-synced candidates so queries serve right after
        # acceptance: the first synced indexer gets in at any score, later ones
        # must beat MIN_SYNCED_THRESHOLD or fall back to competing on merit.
        if self.synced_indexers:
            group_lower = {i.lower() for i in self.current_group}
            group_has_synced = bool(group_lower & self.synced_indexers)
            all_synced_candidates = candidates[
                candidates["indexer"].str.lower().isin(self.synced_indexers)
            ]

            if group_has_synced:
                # Already have a synced indexer — threshold applies
                synced = all_synced_candidates[
                    all_synced_candidates["weighted_score"] >= MIN_SYNCED_THRESHOLD
                ]
            else:
                # No synced indexer yet — first one gets in at any score
                synced = all_synced_candidates

            unsynced = candidates[~candidates["indexer"].isin(synced["indexer"])]
            logger.info(
                "deployment=%s candidates: %d synced eligible, %d unsynced (group_has_synced=%s)",
                self.deployment_id,
                len(synced),
                len(unsynced),
                group_has_synced,
            )
            pools = [("synced", synced), ("unsynced", unsynced)]
        else:
            pools = [("all", candidates)]

        # Iterate pools in order, checking decentralisation
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


def _normalize_metrics(
    merged: pd.DataFrame,
    price_ceiling: Optional[float] = None,
) -> pd.DataFrame:
    """Normalise each metric to [0, 1] where 1 is best (lower-is-better inverted).

    price_ceiling, when given, caps pricing normalisation instead of using the
    observed max, so one outlier price can't compress the rest. Missing metrics
    normalise to NaN, then to 0.
    """
    # (norm column, source column, normaliser), in the order the columns are added.
    metrics: list[tuple[str, str, Callable[[pd.Series], Union[pd.Series, float]]]] = [
        (
            "norm_lat_lin_reg_coefficient",
            "Latency Coefficient + Error Confidence Interval",
            _normalize_latency,
        ),
        # higher is better
        ("norm_uptime_score", "% up_x", _normalize_uptime_and_success_rate),
        ("norm_stake_to_fees", "stake_to_fees", _normalize_stake_to_fees),
        # higher is better
        ("norm_success_rate", "average_status", _normalize_uptime_and_success_rate),
        (
            "norm_base_price_per_epoch",
            "base_price_per_epoch",
            lambda prices: _normalize_base_price_per_epoch(prices, price_ceiling),
        ),
        ("norm_price_per_entity", "price_per_entity", _normalize_price_per_entity),
    ]

    if merged.empty:
        for norm_col, _, _ in metrics:
            merged[norm_col] = pd.Series(dtype=float)
        return merged

    # Keep a norm_ column the input already carries (the CronJob ships latency normalised).
    for norm_col, source_col, normalize in metrics:
        if norm_col in merged.columns:
            continue
        if source_col in merged.columns:
            merged[norm_col] = normalize(merged[source_col])
        else:
            merged[norm_col] = np.nan

    # Fill NaN values with 0 for all norm_ columns
    norm_columns = [col for col in merged.columns if col.startswith("norm_")]
    merged[norm_columns] = merged[norm_columns].fillna(0)

    return merged


def _normalize_latency(latency: pd.Series) -> pd.Series:
    """Normalise the latency linear regression score."""
    return 1 - _normalize_generic(latency)  # lower is better


def _normalize_stake_to_fees(stake_to_fees: pd.Series) -> pd.Series:
    """Normalise stake to fees ratio (higher = more capacity = better).

    Indexers with zero fees get NaN (infinite ratio = maximum capacity).
    Fill NaN above the max finite value so they normalise to 1.0.
    """
    stf = stake_to_fees.copy()
    finite_max = stf.max()
    fill_value = (finite_max + 1.0) if pd.notna(finite_max) else 1.0
    stf = stf.fillna(fill_value)
    return _normalize_generic(stf)


def _normalize_base_price_per_epoch(
    base_price_per_epoch: pd.Series, price_ceiling: Optional[float]
) -> Union[pd.Series, float]:
    """Normalize base price per epoch (lower is better).

    When price_ceiling is provided, use it instead of the observed max
    so outlier prices cannot compress differentiation among others.
    """
    prices = pd.to_numeric(base_price_per_epoch, errors="coerce").fillna(0.0).clip(lower=0)
    ceiling = price_ceiling if price_ceiling is not None and price_ceiling > 0 else prices.max()
    if ceiling > 0 and ceiling > prices.min():
        return (1 - (prices / ceiling)).clip(lower=0)
    return 0.5


def _normalize_price_per_entity(price_per_entity: pd.Series) -> Union[pd.Series, float]:
    """Normalize price per entity (lower is better).

    price_ceiling applies to base epoch price; entity pricing uses
    observed max since there is no separate budget field for it.
    """
    prices = pd.to_numeric(price_per_entity, errors="coerce").fillna(0.0).clip(lower=0)
    ceiling = prices.max()
    if ceiling > 0 and ceiling > prices.min():
        return cast(pd.Series, 1 - (prices / ceiling))
    return 0.5


def _normalize_generic(series: pd.Series) -> pd.Series:
    """Min-max normalise a Series to [0, 1].

    NaN and division-by-zero (a constant series, where max == min) both fill
    to 0.
    """
    min_val = series.min()
    max_val = series.max()

    # Normalize to between 0 and 1 range
    normalized = (series - min_val) / (max_val - min_val)

    # Handle any potential NaN or inf values
    normalized = normalized.fillna(0)

    return cast(pd.Series, normalized)


def _normalize_uptime_and_success_rate(series: pd.Series) -> pd.Series:
    """Piecewise-scale uptime/success rate, rewarding only the top band.

    Values below NON_ZERO_UPTIME_SUCCESS_RATE_SCORE_THRESHOLD of the best
    indexer's score get 0; from there to the best they scale linearly to 1
    (e.g. 98.5% of the best maps to ~0.5).
    """
    # Find the best uptime/success rate score in the series first
    best = series.max()

    # Threshold whereby indexers that have less uptime/success rate than this get no score.
    threshold = best * NON_ZERO_UPTIME_SUCCESS_RATE_SCORE_THRESHOLD

    # Linear score between the threshold and the best.
    normalized = series.apply(
        lambda x: max(
            0,
            min(1, (x - threshold) / (best - threshold)),
        )
    )

    # Reindex and fill NaN's with 0.
    normalized = normalized.reindex(series.index).fillna(0)

    return cast(pd.Series, normalized)


def _calculate_weighted_scores(df: pd.DataFrame, weights: WeightsDict) -> pd.Series:
    """Weighted average of each row's present norm_* metrics by `weights`.
    One matrix multiply over the present norm_* columns: NaN cells drop out of
    both the numerator and the per-row weight total, so each row renormalises
    over the metrics it has. Raises ValueError if any row has no usable column.
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
    weighted_sum = np.nansum(matrix * weight_vector, axis=1)
    weight_total = present @ weight_vector  # weights of the columns each row has

    if (weight_total == 0).any():
        logger.error("Total sum of weights is 0. Sum of weights should be non-zero, ideally 1.")
        raise ValueError("Total weight cannot be 0.")

    return pd.Series(weighted_sum / weight_total, index=df.index)
