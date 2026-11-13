"""Keep docs/scoring.md's constants table in line with the score job's code.

The page's constants table names the code constant behind every number. The service's own
constants are checked by tests/test_scoring_doc.py; this checks the score job's.
"""

import importlib
import math
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "cronjobs" / "compute_scores"))

PAGE = REPO / "docs" / "scoring.md"
JOB_MODULES = {"chain_quality", "processing"}


def _constants() -> list[tuple[str, str]]:
    """(code, value) for each row of the page's constants table."""
    rows = []
    in_table = False
    for line in PAGE.read_text().splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if line.strip().startswith("|") and cells[0] == "Constant":
            in_table = True
        elif in_table and line.strip().startswith("|"):
            if not all(re.fullmatch(r"-+", cell) for cell in cells):
                rows.append((cells[1].strip("`"), cells[2]))
        elif in_table:
            break
    return rows


JOB_CONSTANTS = [(code, value) for code, value in _constants() if code.split(".")[0] in JOB_MODULES]


def test_the_page_lists_the_job_constants():
    assert {code.split(".")[0] for code, _ in JOB_CONSTANTS} == JOB_MODULES


@pytest.mark.parametrize(("code", "value"), JOB_CONSTANTS)
def test_each_job_constant_matches_the_code(code, value):
    module, name = code.rsplit(".", 1)
    actual = getattr(importlib.import_module(module), name)

    assert math.isclose(float(actual), float(value.replace(",", ""))), (
        f"{code} is {actual}, the page says {value}"
    )
