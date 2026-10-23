"""CI tests the service against uv.lock and the daily job against its own requirements.txt.

Each job collects tests by folder, so a test in the wrong folder silently runs against the
other part's package versions. These checks keep the 2 folders honest.
"""

import re
from pathlib import Path

TESTS = Path(__file__).resolve().parent
CRONJOB_TESTS = TESTS / "cronjob"


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
