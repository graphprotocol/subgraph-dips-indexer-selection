"""Guard the README's selection-weights table against the code.

The README documents the seven selection weights for human readers, but those
values are tuning knobs that live in ``DEFAULT_WEIGHTS`` in
``src/iisa/indexer_selection.py``. This test parses the table out of the README
and asserts it matches the code, so the documentation cannot silently go stale
when a weight is retuned (in either direction).
"""

import math
import re
from pathlib import Path

from iisa.indexer_selection import DEFAULT_WEIGHTS

README = Path(__file__).resolve().parents[1] / "README.md"

# Maps each human label in the README weights table to its DEFAULT_WEIGHTS key.
# Update this if the table's signal labels are reworded.
LABEL_TO_KEY = {
    "Economic security (stake-to-fees)": "stake_to_fees",
    "Price (per 30 days)": "base_price_per_epoch",
    "Latency": "lat_lin_reg_coefficient",
    "Uptime": "uptime_score",
    "Freshness": "freshness",
    "Success rate": "success_rate",
    "Price (per billion entities)": "price_per_entity",
}


def _parse_weights_table(markdown: str) -> dict[str, float]:
    """Return {signal label: weight} from the first table that has a Weight column."""
    rows = [line for line in markdown.splitlines() if line.strip().startswith("|")]
    header_idx = next(
        i for i, row in enumerate(rows) if "weight" in row.lower() and "signal" in row.lower()
    )
    header = [cell.strip().lower() for cell in rows[header_idx].strip("|").split("|")]
    signal_col = header.index("signal")
    weight_col = header.index("weight")

    weights: dict[str, float] = {}
    # Skip the header row and the '---' separator row that follows it.
    for row in rows[header_idx + 2 :]:
        cells = [cell.strip() for cell in row.strip("|").split("|")]
        if len(cells) <= max(signal_col, weight_col):
            break
        value = cells[weight_col]
        if not re.fullmatch(r"[0-9]*\.?[0-9]+", value):
            break
        weights[cells[signal_col]] = float(value)
    return weights


def test_readme_weights_match_code() -> None:
    table = _parse_weights_table(README.read_text())
    assert table, "Could not find a weights table in README.md"

    unknown = set(table) - set(LABEL_TO_KEY)
    assert not unknown, (
        f"README weights table has unmapped labels: {sorted(unknown)}. "
        "Update LABEL_TO_KEY in this test if the table was reworded."
    )

    documented = {LABEL_TO_KEY[label]: weight for label, weight in table.items()}
    expected = {key: float(value) for key, value in DEFAULT_WEIGHTS.items()}

    assert set(documented) == set(expected), (
        "README weights table is missing or has extra signals versus DEFAULT_WEIGHTS "
        f"(src/iisa/indexer_selection.py).\nREADME keys: {sorted(documented)}\n"
        f"Code keys:   {sorted(expected)}"
    )
    for key, want in expected.items():
        assert math.isclose(documented[key], want, abs_tol=1e-9), (
            f"README weight for '{key}' is {documented[key]} but DEFAULT_WEIGHTS says "
            f"{want} (src/iisa/indexer_selection.py). Keep the README table in sync."
        )
