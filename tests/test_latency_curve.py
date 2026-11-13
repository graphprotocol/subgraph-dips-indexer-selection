"""Latency is scored on a fixed curve of each indexer's latency in milliseconds.

The score job pushes latency_ms, each indexer's latency in milliseconds. The service scores it
on the gateway's latency curve made steeper, so an indexer's score depends only on its own
latency, not on how fast the other candidates are. Scores from an older job have no latency_ms,
and for those the service keeps scaling latency against the candidates, with a warning.
"""

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from iisa import indexer_selection
from iisa.indexer_selection import (
    LATENCY_FLOOR_SCORE,
    LATENCY_MIDPOINT_MS,
    LATENCY_WIDTH_MS,
    _normalize_metrics,
)
from iisa.log_limit import LogLimit
from iisa.score_columns import NORM_LAT_LIN_REG_COEFFICIENT, SEL_LATENCY_CI
from iisa.score_loader import DataManager, FileScoreLoader


@pytest.fixture(autouse=True)
def fresh_fallback_warnings(monkeypatch):
    """Each test starts with no fallback warning logged yet, at a fixed minute."""
    monkeypatch.setattr(indexer_selection, "_LATENCY_FALLBACK_WARNINGS", LogLimit(10))
    monkeypatch.setattr(indexer_selection, "_current_minute", lambda: 1_000)


def _latency_scores(**columns) -> list[float]:
    return _normalize_metrics(pd.DataFrame(columns))[NORM_LAT_LIN_REG_COEFFICIENT].tolist()


class TestCurve:
    def test_the_constants_are_the_agreed_ones(self):
        assert (LATENCY_MIDPOINT_MS, LATENCY_WIDTH_MS, LATENCY_FLOOR_SCORE) == (250, 80, 0.001)

    def test_scores_fall_along_the_curve(self):
        scores = _latency_scores(latency_ms=[0.0, 50.0, 250.0, 500.0])

        assert scores == pytest.approx([1.0, 0.96, 0.52, 0.04], abs=0.01)

    def test_a_very_slow_indexer_scores_the_floor(self):
        assert _latency_scores(latency_ms=[2_000.0, 1e9]) == [LATENCY_FLOOR_SCORE] * 2

    def test_an_indexer_without_a_latency_scores_0(self):
        """Indexers have to prove themselves, so no latency is the worst score, below the floor
        a measured latency can reach."""
        scores = _latency_scores(latency_ms=[100.0, np.nan, None, "slow", np.inf])

        assert scores[0] == pytest.approx(0.91, abs=0.01)
        assert scores[1:] == [0.0, 0.0, 0.0, 0.0]

    def test_a_score_does_not_depend_on_the_other_candidates(self):
        alone = _latency_scores(latency_ms=[120.0])
        beside_a_faster_one = _latency_scores(latency_ms=[120.0, 10.0])

        assert beside_a_faster_one[0] == alone[0]

    def test_the_old_latency_scores_are_ignored_when_latency_ms_is_there(self, caplog):
        """The job still pushes its pool-scaled score and the regression's upper bound."""
        with caplog.at_level(logging.WARNING, logger="iisa.indexer_selection"):
            scores = _latency_scores(
                latency_ms=[50.0, 500.0],
                **{NORM_LAT_LIN_REG_COEFFICIENT: [0.0, 1.0], SEL_LATENCY_CI: [900.0, -900.0]},
            )

        assert scores == pytest.approx([0.96, 0.04], abs=0.01)
        assert caplog.records == []


class TestOlderJob:
    """Scores from a job that doesn't push latency_ms keep today's latency score."""

    def test_the_pushed_pool_scaled_score_is_kept_with_a_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger="iisa.indexer_selection"):
            scores = _latency_scores(**{NORM_LAT_LIN_REG_COEFFICIENT: [0.2, 0.9]})

        assert scores == [0.2, 0.9]
        (warning,) = [r.getMessage() for r in caplog.records]
        assert "latency_ms" in warning
        assert "0 held back" in warning

    def test_without_a_pushed_score_latency_is_scaled_against_the_candidates(self):
        assert _latency_scores(**{SEL_LATENCY_CI: [10.0, 20.0, 30.0]}) == [1.0, 0.5, 0.0]

    def test_the_warning_is_logged_at_most_once_every_10_minutes(self, monkeypatch, caplog):
        with caplog.at_level(logging.WARNING, logger="iisa.indexer_selection"):
            for minute in (1_000, 1_000, 1_009, 1_010):
                monkeypatch.setattr(indexer_selection, "_current_minute", lambda m=minute: m)
                _latency_scores(**{NORM_LAT_LIN_REG_COEFFICIENT: [0.5]})

        messages = [r.getMessage() for r in caplog.records]
        assert len(messages) == 2
        assert "2 held back" in messages[1]


class TestLoadedScores:
    @staticmethod
    def _loaded(tmp_path, latencies: list) -> pd.DataFrame:
        rows = pd.DataFrame(
            {
                "indexer": [f"0x{i:040x}" for i in range(len(latencies))],
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "lat_normalized_score": 0.5,
                "uptime_score": 0.99,
                "success_rate": 0.98,
                "stake_to_fees": 100.0,
                "dst_lat": 40.0,
                "dst_lon": -74.0,
                "org": "Org",
                "latency_ms": latencies,
            }
        )
        manager = DataManager(FileScoreLoader(str(tmp_path / "scores.json")))
        return manager.transform_scores_df(rows)

    def test_a_row_without_latency_ms_still_loads_and_scores_0(self, tmp_path):
        loaded = self._loaded(tmp_path, [120.0, None])

        scores = _normalize_metrics(loaded)[NORM_LAT_LIN_REG_COEFFICIENT].tolist()

        assert scores[0] == pytest.approx(0.87, abs=0.01)
        assert scores[1] == 0.0

    def test_scores_where_no_row_has_latency_ms_fall_back(self, tmp_path, caplog):
        """A job that pushes latency_ms as null for every row, as without GeoIP, measured none."""
        loaded = self._loaded(tmp_path, [None, None])

        with caplog.at_level(logging.WARNING, logger="iisa.indexer_selection"):
            scores = _normalize_metrics(loaded)[NORM_LAT_LIN_REG_COEFFICIENT].tolist()

        assert "latency_ms" not in loaded.columns
        assert scores == [0.5, 0.5]
        assert len(caplog.records) == 1
