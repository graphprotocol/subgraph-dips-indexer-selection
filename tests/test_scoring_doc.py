"""Keep docs/scoring.md from drifting away from the code.

The page explains the score for readers who would rather check the maths than the code. Its
constants table names the code constant behind every number, and this test checks each one the
service owns; the score job's are checked by test_scoring_doc_job_constants.py among the
cronjob tests. It also
recomputes the page's worked example and latency table with the code.
"""

import importlib
import math
import re
from pathlib import Path

import pandas as pd
import pytest

from iisa.freshness import freshness_score
from iisa.indexer_selection import _score_factors, _score_latency_ms
from iisa.share_score import shaped_share

PAGE = Path(__file__).resolve().parents[1] / "docs" / "scoring.md"
# Modules of the score job, which tests here may not load.
JOB_MODULES = {"chain_quality", "processing"}


def _tables(markdown: str) -> list[list[list[str]]]:
    """Every table on the page, as rows of cells, without the header's separator row."""
    tables: list[list[list[str]]] = []
    current: list[list[str]] = []
    for line in [*markdown.splitlines(), ""]:
        if line.strip().startswith("|"):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if not all(re.fullmatch(r"-+", cell) for cell in cells):
                current.append(cells)
        elif current:
            tables.append(current)
            current = []
    return tables


def _table(header_start: str) -> list[list[str]]:
    """The body rows of the table whose header's first cell is ``header_start``."""
    for table in _tables(PAGE.read_text()):
        if table[0][0] == header_start:
            return table[1:]
    raise AssertionError(f"docs/scoring.md has no table headed {header_start!r}")


def _number(text: str) -> float:
    return float(text.replace(",", "").removesuffix("%").removesuffix(" ms"))


CONSTANTS = [(row[1].strip("`"), row[2]) for row in _table("Constant")]


def test_every_constant_names_code_a_test_checks():
    unchecked = [
        code
        for code, _ in CONSTANTS
        if not code.startswith("iisa.") and code.split(".")[0] not in JOB_MODULES
    ]
    assert CONSTANTS and unchecked == []


@pytest.mark.parametrize(
    ("code", "value"), [(code, value) for code, value in CONSTANTS if code.startswith("iisa.")]
)
def test_each_constant_matches_the_code(code, value):
    module, name = code.rsplit(".", 1)
    actual = getattr(importlib.import_module(module), name)

    assert math.isclose(float(actual), _number(value)), f"{code} is {actual}, the page says {value}"


def test_the_latency_table_matches_the_curve():
    rows = _table("Latency")
    latencies = pd.Series([_number(latency) for latency, _ in rows])

    scores = _score_latency_ms(latencies).round(3).tolist()

    assert scores == [float(score) for _, score in rows]


def test_the_worked_example_adds_up():
    """The page gives each indexer's raw readings and K, P and E; the code works out the rest."""
    rows = {row[0]: row[1:] for row in _table("")}

    def readings(column: int) -> dict[str, float]:
        def read(label: str) -> float:
            return _number(rows[label][column])

        return {
            "norm_uptime_score": shaped_share(read("Uptime") / 100),
            "norm_freshness": freshness_score(round(read("Freshness checks passed") * 200), 20_000),
            "norm_success_rate": shaped_share(read("Success rate") / 100),
            "norm_lat_lin_reg_coefficient": float(
                _score_latency_ms(pd.Series([read("Latency")])).iloc[0]
            ),
            "norm_stake_to_fees": read("K, economic security"),
            "norm_base_price_per_epoch": read("P, price per 30 days"),
            "norm_price_per_entity": read("E, price per billion entities"),
        }

    factors = _score_factors(pd.DataFrame([readings(0), readings(1)]))
    factors["total"] = factors.prod(axis=1)

    labels = {
        "uptime": "U, uptime",
        "freshness": "F, freshness",
        "quality": "Q, quality",
        "value": "V, value",
        "total": "Total",
    }
    for column, label in labels.items():
        stated = [float(value) for value in rows[label]]
        assert factors[column].round(3).tolist() == stated, f"the page's {label} row is wrong"
