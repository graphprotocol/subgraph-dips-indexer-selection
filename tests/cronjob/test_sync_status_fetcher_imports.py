"""The sync-status fetcher is a long-running pod, so it should load only what it uses."""

import subprocess
import sys
from pathlib import Path

JOBS_PATH = Path(__file__).parents[2] / "cronjobs" / "compute_scores"

# Libraries the scoring job needs and the fetcher never touches.
SCORING_ONLY_MODULES = ("processing", "pandas", "numpy", "scipy", "sklearn", "geoip2")


def test_fetcher_does_not_load_the_scoring_libraries():
    # A fresh interpreter, so modules other tests imported don't hide what the fetcher loads.
    script = (
        "import sys; "
        f"sys.path.insert(0, {str(JOBS_PATH)!r}); "
        "import sync_status_fetcher; "
        f"print(','.join(m for m in {SCORING_ONLY_MODULES!r} if m in sys.modules))"
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == ""
