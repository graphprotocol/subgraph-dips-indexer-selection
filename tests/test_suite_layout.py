"""CI tests the service against uv.lock and the daily job against its own requirements.txt.

Each job collects tests by folder, so a test in the wrong folder silently runs against the
other part's package versions, and a test outside tests/ never runs at all. These checks
keep the folders honest.
"""

import os
import re
from pathlib import Path

TESTS = Path(__file__).resolve().parent
CRONJOB_TESTS = TESTS / "cronjob"
REPO = TESTS.parent


def _is_test_file(name: str) -> bool:
    return name.endswith(".py") and (name.startswith("test_") or name.endswith("_test.py"))


def test_tests_that_load_the_daily_job_live_in_the_cronjob_folder():
    misplaced = [
        path.name
        for path in sorted(TESTS.glob("test_*.py"))
        if path.name != Path(__file__).name and "compute_scores" in path.read_text()
    ]
    assert misplaced == []


def test_cronjob_folder_holds_no_service_tests():
    service_import = re.compile(r"^\s*(from|import)\s+iisa\b", re.MULTILINE)
    misplaced = [
        path.name
        for path in sorted(CRONJOB_TESTS.glob("test_*.py"))
        if service_import.search(path.read_text())
    ]
    assert misplaced == []


def test_no_test_files_outside_the_tests_folder():
    stray = []
    for root, dirs, files in os.walk(REPO):
        here = Path(root)
        # Skip hidden folders such as .git and .venv, any other virtualenv, and tests/ itself.
        dirs[:] = [
            name
            for name in dirs
            if not name.startswith(".")
            and here / name != TESTS
            and not (here / name / "pyvenv.cfg").exists()
        ]
        stray += [str((here / name).relative_to(REPO)) for name in files if _is_test_file(name)]
    assert sorted(stray) == []
