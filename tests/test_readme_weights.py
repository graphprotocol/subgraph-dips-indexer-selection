"""Guard the README's score table against the code.

The README documents, for human readers, how each signal feeds the total: uptime and freshness
are factors on their own, and quality and value are weighted averages of their signals. Those
weights are tuning knobs that live in ``QUALITY_WEIGHTS`` and ``VALUE_WEIGHTS`` in
``src/iisa/indexer_selection.py``. This test parses the table out of the README and asserts it
matches the code, so the documentation cannot silently go stale when a weight is retuned.
"""

import math
import re
from pathlib import Path

from iisa.indexer_selection import QUALITY_WEIGHTS, VALUE_WEIGHTS

README = Path(__file__).resolve().parents[1] / "README.md"

# Maps each human label in the README table to its metric key. Update this if the table's
# signal labels are reworded.
LABEL_TO_KEY = {
    "Uptime": "uptime_score",
    "Freshness": "freshness",
    "Success rate": "success_rate",
    "Latency": "lat_lin_reg_coefficient",
    "Economic security (stake-to-fees)": "stake_to_fees",
    "Price (per 30 days)": "base_price_per_epoch",
    "Price (per billion entities)": "price_per_entity",
}

# What the code says, by factor: uptime and freshness are whole factors, a weight of 1.
EXPECTED = {
    "Uptime": {"uptime_score": 1.0},
    "Freshness": {"freshness": 1.0},
    "Quality": dict(QUALITY_WEIGHTS),
    "Value": dict(VALUE_WEIGHTS),
}


def _cells(row: str) -> list[str]:
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def _parse_score_table(markdown: str) -> dict[str, dict[str, float]]:
    """Return {factor: {signal label: weight}} from the first table with Factor, Signal and
    Weight columns."""
    rows = [line for line in markdown.splitlines() if line.strip().startswith("|")]
    header_idx = next(
        i
        for i, row in enumerate(rows)
        if {"factor", "signal", "weight"} <= set(_cells(row.lower()))
    )
    header = _cells(rows[header_idx].lower())
    factor_col, signal_col, weight_col = (header.index(n) for n in ("factor", "signal", "weight"))

    table: dict[str, dict[str, float]] = {}
    # Skip the header row and the '---' separator row that follows it.
    for row in rows[header_idx + 2 :]:
        values = _cells(row)
        if len(values) <= max(factor_col, signal_col, weight_col):
            break
        if not re.fullmatch(r"[0-9]*\.?[0-9]+", values[weight_col]):
            break
        table.setdefault(values[factor_col], {})[values[signal_col]] = float(values[weight_col])
    return table


def test_readme_score_table_matches_code() -> None:
    table = _parse_score_table(README.read_text())
    assert table, "Could not find a score table in README.md"

    labels = {label for signals in table.values() for label in signals}
    unknown = labels - set(LABEL_TO_KEY)
    assert not unknown, (
        f"README score table has unmapped labels: {sorted(unknown)}. "
        "Update LABEL_TO_KEY in this test if the table was reworded."
    )

    documented = {
        factor: {LABEL_TO_KEY[label]: weight for label, weight in signals.items()}
        for factor, signals in table.items()
    }
    shape = {factor: set(weights) for factor, weights in documented.items()}
    assert shape == {factor: set(weights) for factor, weights in EXPECTED.items()}, (
        "README score table's factors or signals differ from the code "
        "(QUALITY_WEIGHTS and VALUE_WEIGHTS in src/iisa/indexer_selection.py)."
    )
    for factor, weights in EXPECTED.items():
        for key, want in weights.items():
            assert math.isclose(documented[factor][key], want, abs_tol=1e-9), (
                f"README weight for '{key}' in {factor} is {documented[factor][key]} but the "
                f"code says {want} (src/iisa/indexer_selection.py). Keep the README in sync."
            )
