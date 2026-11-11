"""What the score job's fallback sends to iisa.

When the main pipeline cannot run, the job pushes fallback scores that treat every indexer the
same. It never read stake or query fees for them, so it must say so, and label the scores, so
that iisa does not weigh dipper's expected fees against numbers nobody measured.
"""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

import processing  # noqa: E402

A, B, C, D = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4"))
URLS = {indexer: f"https://{indexer[2:8]}.example/" for indexer in (A, B, C, D)}


@pytest.fixture
def pushed(monkeypatch) -> list[dict]:
    """The fallback's scores for 4 indexers as the push to iisa carries them, a list of JSON
    records, with no indexer or network asked for anything.
    """
    monkeypatch.setattr(processing, "fetch_dips_info", lambda _: pd.DataFrame())
    monkeypatch.setattr(processing, "fetch_and_filter_graph_node_versions", lambda df, _: df)
    scores = processing.compute_degraded_scores(URLS)
    return json.loads(scores.to_json(orient="records", date_format="iso", date_unit="s"))


def test_every_row_is_labelled_degraded(pushed):
    assert [row["scoring_mode"] for row in pushed] == ["degraded"] * 4


def test_stake_and_query_fees_are_sent_as_unknown_not_0(pushed):
    assert [row["last_known_slashable_stake"] for row in pushed] == [None] * 4
    assert [row["total_query_fees"] for row in pushed] == [None] * 4


def test_the_neutral_stake_to_fees_score_is_still_sent(pushed):
    assert [row["norm_stake_to_fees"] for row in pushed] == [0.5] * 4
