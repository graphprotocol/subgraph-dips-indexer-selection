"""Property-based tests for the price filters, the metric normalisers and indexer selection.

Hypothesis feeds each function prices and metrics that are NaN, infinite, negative, huge,
unreadable text or missing, and checks the rules selection relies on: scores stay finite and
within [0, 1], the filters only ever drop rows, and selection never returns more indexers than
asked, a blocklisted indexer, or one whose price is invalid or over the request's ceiling.
"""

import json
import math
from types import SimpleNamespace
from typing import Any, NamedTuple, Optional
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from hypothesis import example, given, note
from hypothesis import strategies as st

from iisa import iisa_http_endpoints as endpoints
from iisa.iisa_http_endpoints import (
    SelectionRequest,
    _extract_chain_price,
    _filter_by_price,
    _keep_supporting_chain,
    _keep_with_chain_price,
    _keep_with_dips_info,
    _keep_with_valid_entity_price,
    _keep_within_budget,
    _keep_within_entity_budget,
    _parse_entity_price,
    _select_with_processor,
    _supports_chain,
)
from iisa.indexer_selection import (
    DEFAULT_WEIGHTS,
    IndexerSelector,
    _calculate_weighted_scores,
    _normalize_base_price_per_epoch,
    _normalize_generic,
    _normalize_latency,
    _normalize_metrics,
    _normalize_price_per_entity,
    _normalize_stake_to_fees,
    _normalize_uptime_and_success_rate,
)
from iisa.score_columns import SEL_LATENCY_CI

CHAIN = "arbitrum-one"
OTHER_CHAIN = "mainnet"
DEPLOYMENT = "QmPropertyTestDeployment"
ENTITY_COLUMN = "dips_min_grt_per_billion_entities_per_30_days"
INDEXER_IDS = [f"0x{number:040x}" for number in range(1, 9)]
NORM_COLUMNS = [f"norm_{metric}" for metric in DEFAULT_WEIGHTS]
# Deep enough to exhaust the JSON decoder's recursion limit.
DEEPLY_NESTED = "[" * 2_000 + "]" * 2_000
# An integer longer than Python reads from text by default (4,300 digits).
TOO_MANY_DIGITS = "[" + "1" * 5_000 + "]"


def _reads_as_float(text: str) -> bool:
    try:
        float(text)
    except (ValueError, OverflowError):
        return False
    return True


any_floats = st.floats(allow_nan=True, allow_infinity=True)
# Usable prices are finite and >= 0, up to the float limit, sent as numbers or as text.
usable_prices = st.one_of(
    st.floats(min_value=0, max_value=1.7e308), st.integers(min_value=0, max_value=10**18)
)
usable_price_values = st.one_of(usable_prices, usable_prices.map(str))
unusable_numbers = st.one_of(
    st.sampled_from([math.nan, math.inf, -math.inf, True, False, 10**400]),
    st.floats(max_value=0, exclude_max=True),
    st.integers(max_value=-1),
)
unusable_text = st.sampled_from(["nan", "NaN", "inf", "-Infinity", "-5", "-0.5", "1e999"])
unreadable_text = st.one_of(
    st.sampled_from(["", "abc", "1,000", "12 GRT", "0x10", "None"]),
    st.text(max_size=6).filter(lambda text: not _reads_as_float(text)),
)
unreadable_values = st.one_of(
    unreadable_text,
    st.lists(st.integers(), max_size=2),
    st.dictionaries(st.text(max_size=3), st.integers(), max_size=2),
)
malformed_price_cells = st.sampled_from(
    ["not json", "", "{", "null", "5", "[1, 2]", '"450"', "true", DEEPLY_NESTED]
    + [None, math.nan, 5.0, {CHAIN: 5.0}]
)
malformed_network_cells = st.sampled_from(
    ["not json", "", "[", "null", "5", f'"{CHAIN}"', json.dumps({CHAIN: True})]
    + [DEEPLY_NESTED, TOO_MANY_DIGITS]
    + [None, math.nan, [CHAIN]]
)


def _maybe(draw: st.DrawFn, forced: Optional[bool]) -> bool:
    """forced when set, else a coin flip."""
    return forced if forced is not None else draw(st.booleans())


@st.composite
def chain_price_cells(
    draw: st.DrawFn, usable: Optional[bool] = None
) -> tuple[Any, Optional[float]]:
    """A dips_min_grt_per_30_days cell and the CHAIN price it should read as, None when none
    is usable. ``usable`` forces a usable price (True) or rules one out (False).
    """
    others = draw(st.dictionaries(st.just(OTHER_CHAIN), usable_price_values, max_size=1))
    if _maybe(draw, usable):
        value = draw(usable_price_values)
        return json.dumps({CHAIN: value, **others}), float(value)
    kind = draw(st.sampled_from(["unusable", "unreadable", "absent", "malformed"]))
    if kind == "absent":
        return json.dumps(others), None
    if kind == "malformed":
        return draw(malformed_price_cells), None
    if kind == "unusable":
        value = draw(st.one_of(unusable_numbers, unusable_text))
    else:
        value = draw(st.one_of(unreadable_values, st.none()))
    return json.dumps({CHAIN: value, **others}), None


class EntityTruth(NamedTuple):
    price: Optional[float]
    invalid: bool


@st.composite
def entity_price_cells(draw: st.DrawFn, valid: Optional[bool] = None) -> tuple[Any, EntityTruth]:
    """A per-entity price cell and how it should read. ``valid`` forces a missing or usable
    price (True) or an invalid one (False).
    """
    if _maybe(draw, valid):
        if draw(st.booleans()):
            return draw(st.sampled_from([None, math.nan])), EntityTruth(None, False)
        value = draw(usable_price_values)
        return value, EntityTruth(float(value), False)
    not_nan = unusable_numbers.filter(
        lambda value: not (isinstance(value, float) and value != value)
    )
    value = draw(st.one_of(not_nan, unusable_text, unreadable_values))
    return value, EntityTruth(None, True)


@st.composite
def network_cells(draw: st.DrawFn, supports: Optional[bool] = None) -> tuple[Any, bool]:
    """A dips_supported_networks cell and whether it lists CHAIN."""
    if _maybe(draw, supports):
        return json.dumps([CHAIN] + draw(st.lists(st.just(OTHER_CHAIN), max_size=1))), True
    if draw(st.booleans()):
        return json.dumps(draw(st.lists(st.just(OTHER_CHAIN), max_size=1))), False
    return draw(malformed_network_cells), False


class PriceTruth(NamedTuple):
    """What each price rule should conclude about one generated indexer row."""

    available: bool
    supports: bool
    price: Optional[float]
    entity: EntityTruth

    def within_budget(self, cap: Optional[float]) -> bool:
        return cap is None or (self.price is not None and self.price <= cap)

    def within_entity_budget(self, cap: Optional[float]) -> bool:
        price, invalid = self.entity
        return cap is None or (not invalid and (price is None or price <= cap))

    def passes(self, cap: Optional[float], entity_cap: Optional[float]) -> bool:
        return (
            self.available
            and self.supports
            and self.price is not None
            and self.within_budget(cap)
            and not self.entity.invalid
            and self.within_entity_budget(entity_cap)
        )


@st.composite
def priced_rows(draw: st.DrawFn) -> tuple[list[dict[str, Any]], dict[str, PriceTruth]]:
    """Rows carrying the DIPs price columns, each with what the filters should conclude.

    About half the rows are built to pass every rule, so filters and selection have work to
    do; the rest pick each field at random. Networks and per-entity price may be left out.
    """
    count = draw(st.integers(min_value=0, max_value=len(INDEXER_IDS)))
    with_networks, with_entity = draw(st.booleans()), draw(st.booleans())
    rows, truths = [], {}
    for indexer in INDEXER_IDS[:count]:
        valid: Optional[bool] = True if draw(st.booleans()) else None
        available = _maybe(draw, valid)
        price_cell, price = draw(chain_price_cells(usable=valid))
        row = {
            "indexer": indexer,
            "dips_info_available": True
            if available
            else draw(st.sampled_from([False, None, "true"])),
            "dips_min_grt_per_30_days": price_cell,
        }
        supports, entity = True, EntityTruth(None, False)
        if with_networks:
            row["dips_supported_networks"], supports = draw(network_cells(supports=valid))
        if with_entity:
            row[ENTITY_COLUMN], entity = draw(entity_price_cells(valid=valid))
        rows.append(row)
        truths[indexer] = PriceTruth(available, supports, price, entity)
    return rows, truths


def _table(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = list(dict.fromkeys(column for row in rows for column in row)) or [
        "indexer",
        "dips_info_available",
        "dips_min_grt_per_30_days",
    ]
    return pd.DataFrame(rows, columns=columns)


def _caps(draw: st.DrawFn, prices: list[float]) -> Optional[float]:
    """A ceiling a request may send: none, any finite value >= 0, or one of the prices."""
    choices = [st.none(), st.floats(min_value=0, max_value=1.7e308)]
    if prices:
        choices.append(st.sampled_from(prices))
    return draw(st.one_of(choices))


@st.composite
def priced_tables(draw: st.DrawFn):
    rows, truths = draw(priced_rows())
    cap = _caps(draw, [t.price for t in truths.values() if t.price is not None])
    entity_cap = _caps(
        draw, [t.entity.price for t in truths.values() if t.entity.price is not None]
    )
    return _table(rows), truths, cap, entity_cap


def _assert_unit_interval(values: Any, *, allow_nan: bool = False) -> None:
    """Every value is a finite number in [0, 1]; NaN is allowed only when allow_nan is set."""
    array = np.atleast_1d(np.asarray(values, dtype=float))
    if allow_nan:
        array = array[~np.isnan(array)]
    assert np.isfinite(array).all(), f"non-finite score in {array}"
    assert ((array >= 0) & (array <= 1)).all(), f"score outside [0, 1] in {array}"


# --- Price readers ---------------------------------------------------------------------------


@given(cell_and_price=chain_price_cells())
# Valid JSON that is not an object raised AttributeError, failing selection with a 500.
@example(cell_and_price=("null", None))
@example(cell_and_price=("[1, 2]", None))
# JSON nested deeper than the decoder can follow raised RecursionError, failing selection.
@example(cell_and_price=(DEEPLY_NESTED, None))
def test_chain_price_reads_as_a_usable_number_or_none(cell_and_price):
    cell, expected = cell_and_price
    price = _extract_chain_price(cell, CHAIN)
    assert price == expected
    assert price is None or (math.isfinite(price) and price >= 0)


@given(cell_and_truth=entity_price_cells())
def test_entity_price_reads_as_missing_usable_or_invalid(cell_and_truth):
    cell, expected = cell_and_truth
    assert tuple(_parse_entity_price(cell)) == tuple(expected)


@given(cell_and_supports=network_cells())
# The decoder's RecursionError and its ValueError for a very long integer failed selection.
@example(cell_and_supports=(DEEPLY_NESTED, False))
@example(cell_and_supports=(TOO_MANY_DIGITS, False))
def test_supported_networks_never_fail_to_read(cell_and_supports):
    cell, supports = cell_and_supports
    assert _supports_chain(cell, CHAIN) is supports


# --- Price filter --------------------------------------------------------------------------

STEPS = {
    "dips_info": (
        lambda df, cap, entity_cap: _keep_with_dips_info(df),
        lambda truth, cap, entity_cap: truth.available,
    ),
    "supports_chain": (
        lambda df, cap, entity_cap: _keep_supporting_chain(df, CHAIN),
        lambda truth, cap, entity_cap: truth.supports,
    ),
    "chain_price": (
        lambda df, cap, entity_cap: _keep_with_chain_price(df, CHAIN),
        lambda truth, cap, entity_cap: truth.price is not None,
    ),
    "budget": (
        lambda df, cap, entity_cap: _keep_within_budget(df, CHAIN, cap),
        lambda truth, cap, entity_cap: truth.within_budget(cap),
    ),
    "valid_entity_price": (
        lambda df, cap, entity_cap: _keep_with_valid_entity_price(df),
        lambda truth, cap, entity_cap: not truth.entity.invalid,
    ),
    "entity_budget": (
        lambda df, cap, entity_cap: _keep_within_entity_budget(df, entity_cap),
        lambda truth, cap, entity_cap: truth.within_entity_budget(entity_cap),
    ),
}


@pytest.mark.parametrize("step_name", list(STEPS))
@given(case=priced_tables())
def test_each_filter_step_keeps_exactly_the_rows_meeting_its_rule(step_name, case):
    table, truths, cap, entity_cap = case
    step, rule = STEPS[step_name]
    kept, reason = step(table, cap, entity_cap)
    expected = [i for i in table["indexer"] if rule(truths[i], cap, entity_cap)]
    assert len(kept) <= len(table)
    assert list(kept.get("indexer", [])) == expected
    if not kept.empty:
        pd.testing.assert_frame_equal(kept, table.loc[kept.index])
        assert reason == ""


@given(case=priced_tables())
def test_filter_keeps_only_indexers_with_a_valid_price_within_every_ceiling(case):
    table, truths, cap, entity_cap = case
    note(f"cap={cap!r} entity_cap={entity_cap!r}")
    kept, reason = _filter_by_price(table, CHAIN, cap, entity_cap)
    assert len(kept) <= len(table)
    assert list(kept.columns) == list(table.columns)
    for indexer in kept["indexer"]:
        truth = truths[indexer]
        assert truth.price is not None and not truth.entity.invalid, f"{indexer} has no valid price"
        assert truth.within_budget(cap) and truth.within_entity_budget(entity_cap)
    assert list(kept["indexer"]) == [
        i for i in table["indexer"] if truths[i].passes(cap, entity_cap)
    ]
    assert (reason == "") == (not kept.empty)


@given(case=priced_tables())
def test_filter_without_dips_info_column_returns_the_table_unchanged(case):
    table, _, cap, entity_cap = case
    table = table.drop(columns=["dips_info_available"])
    kept, reason = _filter_by_price(table, CHAIN, cap, entity_cap)
    pd.testing.assert_frame_equal(kept, table)
    assert reason == ""


# --- Normalisers and weighted scores --------------------------------------------------------

float_series = st.lists(any_floats, max_size=10).map(lambda values: pd.Series(values, dtype=float))


@pytest.mark.parametrize(
    "normalize",
    [
        _normalize_generic,
        _normalize_latency,
        _normalize_stake_to_fees,
        _normalize_uptime_and_success_rate,
    ],
)
@given(values=float_series)
def test_numeric_normalisers_return_finite_scores_within_unit_range(normalize, values):
    scores = normalize(values.copy())
    assert len(scores) == len(values)
    _assert_unit_interval(scores)


price_inputs = st.lists(
    st.one_of(any_floats, st.none(), unreadable_text, usable_prices.map(str)), max_size=10
)


@given(prices=price_inputs, ceiling=st.one_of(st.none(), any_floats))
def test_base_price_scores_stay_within_unit_range(prices, ceiling):
    # NaN marks a price it cannot score; _normalize_metrics turns that into 0.
    scores = _normalize_base_price_per_epoch(pd.Series(prices, dtype=object), ceiling)
    _assert_unit_interval(scores, allow_nan=True)


@given(prices=price_inputs)
def test_entity_price_scores_are_finite_and_within_unit_range(prices):
    scores = _normalize_price_per_entity(pd.Series(prices, dtype=object))
    assert len(scores) == len(prices)
    _assert_unit_interval(scores)


def metric_values(plausible: st.SearchStrategy) -> st.SearchStrategy:
    """Plausible values mixed with NaN, infinities, negatives, huge values, text and None."""
    return st.one_of(plausible, any_floats, st.none(), unreadable_text)


RAW_METRICS = {
    SEL_LATENCY_CI: st.floats(min_value=0, max_value=5),
    "% up_x": st.floats(min_value=0, max_value=100),
    "average_status": st.floats(min_value=0, max_value=1),
    "stake_to_fees": st.floats(min_value=0, max_value=1e6),
    "base_price_per_epoch": st.floats(min_value=0, max_value=1e6),
    "price_per_entity": st.floats(min_value=0, max_value=1e6),
}
# Scores the CronJob ships already normalised; the service keeps them as they are.
CARRIED_NORMS = ["norm_lat_lin_reg_coefficient", "norm_uptime_score", "norm_success_rate"]


@st.composite
def metric_columns(draw: st.DrawFn, count: int, raw: dict[str, st.SearchStrategy]):
    """Each metric column present or not, with count generated values."""
    columns = {}
    strategies = {**raw, **dict.fromkeys(CARRIED_NORMS, st.floats(min_value=0, max_value=1))}
    for column, plausible in strategies.items():
        if draw(st.booleans()):
            values = metric_values(plausible)
            columns[column] = draw(st.lists(values, min_size=count, max_size=count))
    return columns


@st.composite
def metric_tables(draw: st.DrawFn) -> pd.DataFrame:
    count = draw(st.integers(min_value=0, max_value=8))
    columns = draw(metric_columns(count, RAW_METRICS))
    return pd.DataFrame({"indexer": INDEXER_IDS[:count], **columns})


@given(table=metric_tables())
# Text, or a column holding only None, raised inside normalisation, so selection scored
# every indexer on whichever metrics were done before the error.
@example(table=pd.DataFrame({"indexer": INDEXER_IDS[:2], "stake_to_fees": [0.0, "abc"]}))
@example(table=pd.DataFrame({"indexer": INDEXER_IDS[:1], "average_status": [None]}))
def test_normalised_metrics_are_finite_and_within_unit_range(table):
    normalized = _normalize_metrics(table.copy())
    assert len(normalized) == len(table)
    for column in NORM_COLUMNS:
        _assert_unit_interval(normalized[column])


norm_values = st.one_of(st.floats(min_value=0, max_value=1), st.just(math.nan))
weights = st.one_of(
    st.just(dict(DEFAULT_WEIGHTS)),
    st.fixed_dictionaries(
        {metric: st.floats(min_value=0, max_value=1) for metric in DEFAULT_WEIGHTS}
    ),
)


@given(
    rows=st.lists(st.lists(norm_values, min_size=6, max_size=6), min_size=1, max_size=8),
    weights=weights,
)
def test_weighted_scores_are_finite_and_within_unit_range(rows, weights):
    norms = pd.DataFrame(rows, columns=NORM_COLUMNS)
    weight_vector = np.array([weights[metric] for metric in DEFAULT_WEIGHTS])
    if ((norms.notna().to_numpy() @ weight_vector) == 0).any():
        with pytest.raises(ValueError):
            _calculate_weighted_scores(norms, weights)
        return
    _assert_unit_interval(_calculate_weighted_scores(norms, weights))


# --- Selection end to end ------------------------------------------------------------------


class SelectionCase(NamedTuple):
    table: pd.DataFrame
    truths: dict[str, PriceTruth]
    request: SelectionRequest


SELECTION_METRICS = {
    **{column: RAW_METRICS[column] for column in [SEL_LATENCY_CI, "% up_x", "average_status"]},
    "stake_to_fees": RAW_METRICS["stake_to_fees"],
}
FEE_INPUTS = st.one_of(st.floats(min_value=0, max_value=1e6), any_floats, st.none())


def _some_of(draw: st.DrawFn, ids: list[str], max_size: int) -> list[str]:
    return draw(st.lists(st.sampled_from(ids), unique=True, max_size=max_size)) if ids else []


@st.composite
def selection_cases(
    draw: st.DrawFn, existing_meets_rules: bool = True, text_in_fees: bool = False
) -> SelectionCase:
    """A candidate table, what the price rules should conclude per indexer, and a request.

    With existing_meets_rules the request's existing indexers all pass the price rules, are
    not blocklisted and fit the target size; otherwise they are any indexers in the table.
    """
    rows, truths = draw(priced_rows())
    ids = [row["indexer"] for row in rows]
    metrics = draw(metric_columns(len(rows), SELECTION_METRICS))
    fee_values = metric_values(FEE_INPUTS) if text_in_fees else FEE_INPUTS
    for position, row in enumerate(rows):
        org = draw(st.sampled_from(["org-a", "org-b", "org-c"]))
        row.update({column: values[position] for column, values in metrics.items()})
        row.update(org=org, destination_loc=draw(st.sampled_from(["1,1", "2,2", "3,3"])))
        row["total_query_fees"] = draw(fee_values)
        row["last_known_slashable_stake"] = draw(fee_values)

    cap = _caps(draw, [t.price for t in truths.values() if t.price is not None])
    entity_cap = _caps(
        draw, [t.entity.price for t in truths.values() if t.entity.price is not None]
    )
    size = draw(st.integers(min_value=1, max_value=5))
    blocklist = _some_of(draw, ids, max_size=3)
    if existing_meets_rules:
        eligible = [i for i in ids if truths[i].passes(cap, entity_cap) and i not in blocklist]
        existing = _some_of(draw, eligible, max_size=size)
    else:
        existing = _some_of(draw, ids, max_size=len(ids))
    fee_amounts = draw(st.lists(st.floats(min_value=0, max_value=1e9), min_size=3, max_size=3))
    fees = dict(zip(_some_of(draw, ids, max_size=3), fee_amounts))
    request = SelectionRequest(
        deployment_id=DEPLOYMENT,
        chain_id=CHAIN,
        num_candidates=size,
        existing_indexers=existing or None,
        blocklist=blocklist or None,
        max_grt_per_30_days=cap,
        max_grt_per_billion_entities_per_30_days=entity_cap,
        optimistic_dips_fees=fees or None,
    )
    return SelectionCase(_table(rows), truths, request)


def _select(case: SelectionCase) -> tuple[list[str], Optional[IndexerSelector]]:
    """Run the /select-indexers selection on case.table; return the ids and the selector used."""
    built: list[IndexerSelector] = []

    def build(*args: Any, **kwargs: Any) -> IndexerSelector:
        built.append(IndexerSelector(*args, **kwargs))
        return built[-1]

    state = SimpleNamespace(history=case.table, sync_status=None)
    with (
        patch.object(endpoints, "_state", state),
        patch.object(endpoints, "IndexerSelector", side_effect=build),
    ):
        response = _select_with_processor(case.request)
    return [indexer.id for indexer in response.indexers], (built[0] if built else None)


EXAMPLE_PRICE = 100.0
FIRST, SECOND = INDEXER_IDS[:2]


def _example_row(indexer: str, org: str, **overrides: Any) -> dict[str, Any]:
    """A row priced at EXAMPLE_PRICE that passes every price rule, for hand-written examples."""
    row = {
        "indexer": indexer,
        "dips_info_available": True,
        "dips_min_grt_per_30_days": json.dumps({CHAIN: EXAMPLE_PRICE}),
        "org": org,
        "destination_loc": f"{org},0",
        SEL_LATENCY_CI: 1.0,
        "% up_x": 99.0,
        "average_status": 0.99,
        "stake_to_fees": 10.0,
        "total_query_fees": 1.0,
        "last_known_slashable_stake": 10.0,
    }
    return {**row, **overrides}


def _example_case(
    rows: list[dict[str, Any]], unpriced: tuple[str, ...] = (), **request: Any
) -> SelectionCase:
    """A SelectionCase for hand-written examples. Rows named in unpriced have no usable price."""
    truths = {
        row["indexer"]: PriceTruth(
            True,
            True,
            None if row["indexer"] in unpriced else EXAMPLE_PRICE,
            EntityTruth(None, False),
        )
        for row in rows
    }
    selection = SelectionRequest(deployment_id=DEPLOYMENT, chain_id=CHAIN, **request)
    return SelectionCase(_table(rows), truths, selection)


def _assert_selection_scores(selector: Optional[IndexerSelector]) -> None:
    if selector is None:
        return
    for column in [*NORM_COLUMNS, "weighted_score"]:
        _assert_unit_interval(selector.data[column])


@given(case=selection_cases())
@example(case=_example_case([_example_row(FIRST, "org-a", stake_to_fees=None)], num_candidates=1))
def test_selection_respects_size_blocklist_and_price_rules(case):
    request = case.request
    selected, selector = _select(case)
    note(f"selected={selected}")
    assert len(selected) <= request.num_candidates
    assert len(set(selected)) == len(selected)
    assert not set(selected) & set(request.blocklist or [])
    for indexer in selected:
        assert case.truths[indexer].passes(
            request.max_grt_per_30_days, request.max_grt_per_billion_entities_per_30_days
        ), f"selected {indexer} despite an invalid or over-ceiling price"
    _assert_selection_scores(selector)


# Properties that do not hold yet: each needs a decision, so each is a strict xfail with a
# hand-written example that fails today. Remove the xfail once the behaviour is settled.
@pytest.mark.xfail(
    strict=True,
    reason="A group already bigger than num_candidates is trimmed only while it keeps 2 orgs "
    "and 2 locations, so it can stay bigger than asked. Trimming anyway cancels a live "
    "agreement, which needs a decision.",
)
@given(case=selection_cases(existing_meets_rules=False))
@example(
    case=_example_case(
        [_example_row(FIRST, "org-a"), _example_row(SECOND, "org-b")],
        num_candidates=1,
        existing_indexers=[FIRST, SECOND],
    )
)
def test_selection_never_returns_more_than_asked_when_the_group_is_too_big(case):
    selected, _ = _select(case)
    assert len(selected) <= case.request.num_candidates


@pytest.mark.xfail(
    strict=True,
    reason="An indexer already in the group stays selected when the request blocklists it. "
    "Dropping it makes dipper cancel its live agreement, which needs a decision.",
)
@given(case=selection_cases(existing_meets_rules=False))
@example(
    case=_example_case(
        [_example_row(FIRST, "org-a"), _example_row(SECOND, "org-b")],
        num_candidates=1,
        existing_indexers=[FIRST],
        blocklist=[FIRST],
    )
)
def test_selection_never_returns_a_blocklisted_indexer_already_in_the_group(case):
    selected, _ = _select(case)
    assert not set(selected) & set(case.request.blocklist or [])


@pytest.mark.xfail(
    strict=True,
    reason="An indexer already in the group stays selected when the price filter drops it, "
    "even for an invalid price. Dropping it makes dipper cancel its live agreement, which "
    "needs a decision.",
)
@given(case=selection_cases(existing_meets_rules=False))
@example(
    case=_example_case(
        [
            _example_row(FIRST, "org-a", dips_min_grt_per_30_days=json.dumps({CHAIN: "abc"})),
            _example_row(SECOND, "org-b"),
        ],
        unpriced=(FIRST,),
        num_candidates=1,
        existing_indexers=[FIRST],
    )
)
def test_selection_never_returns_an_invalid_price_already_in_the_group(case):
    request = case.request
    selected, _ = _select(case)
    for indexer in selected:
        assert case.truths[indexer].passes(
            request.max_grt_per_30_days, request.max_grt_per_billion_entities_per_30_days
        ), f"selected {indexer} despite an invalid or over-ceiling price"


@pytest.mark.xfail(
    strict=True,
    reason="With optimistic DIPs fees the selector recomputes stake_to_fees from stake and "
    "query fees, and text in either raises. A missing stake already scores as the best "
    "ratio, so how to score an unreadable one needs a decision.",
)
@given(case=selection_cases(text_in_fees=True))
@example(
    case=_example_case(
        [_example_row(FIRST, "org-a", total_query_fees="abc")],
        num_candidates=1,
        optimistic_dips_fees={FIRST: 1.0},
    )
)
def test_selection_survives_unreadable_stake_or_query_fees(case):
    selected, selector = _select(case)
    assert len(selected) <= case.request.num_candidates
    _assert_selection_scores(selector)
