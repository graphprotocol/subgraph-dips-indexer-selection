"""Tests for the IISA HTTP API endpoints."""

import json
import logging
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient


class TestSettings:
    """Tests for Settings class and get_settings()."""

    def test_settings_loads_from_env(self):
        """Verify IISA_ prefix environment variables load correctly."""
        # Arrange
        env_vars = {
            "IISA_HOST": "127.0.0.1",
            "IISA_PORT": "9000",
            "IISA_LOG_LEVEL": "DEBUG",
        }

        # Act
        with patch.dict(os.environ, env_vars, clear=False):
            from iisa.iisa_http_endpoints import Settings

            settings = Settings()

        # Assert
        assert settings.host == "127.0.0.1"
        assert settings.port == 9000
        assert settings.log_level == "DEBUG"

    def test_settings_default_values(self):
        """Verify defaults: host="0.0.0.0", port=8080, log_level="INFO"."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import Settings

        settings = Settings()

        # Assert
        assert settings.host == "0.0.0.0"
        assert settings.port == 8080
        assert settings.log_level == "INFO"

    def test_get_settings_cached(self):
        """Verify @lru_cache returns same instance."""
        # Arrange
        env_vars = {}

        with patch.dict(os.environ, env_vars, clear=False):
            # Need to reimport to get fresh cache
            from iisa import iisa_http_endpoints

            # Clear the cache first
            iisa_http_endpoints.get_settings.cache_clear()

            # Act
            settings1 = iisa_http_endpoints.get_settings()
            settings2 = iisa_http_endpoints.get_settings()

            # Assert
            assert settings1 is settings2


class TestPydanticModels:
    """Tests for request/response model validation."""

    def test_selection_request_requires_num_candidates(self):
        """num_candidates is required along with deployment_id."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import SelectionRequest

        request = SelectionRequest(deployment_id="Qm123", num_candidates=3)

        # Assert
        assert request.deployment_id == "Qm123"
        assert request.num_candidates == 3
        assert request.existing_indexers is None
        assert request.pending_agreements is None
        assert request.blocklist is None
        assert request.declined_indexers is None

    def test_selection_request_with_all_fields(self):
        """Verify all optional fields serialize correctly."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import SelectionRequest

        request = SelectionRequest(
            deployment_id="Qm123",
            existing_indexers=["0x111"],
            num_candidates=2,
            blocklist=["0xBAD"],
            pending_agreements={"Qm123": ["0xPEND"]},
            declined_indexers={"Qm123": ["0xDEC"]},
        )

        # Assert
        assert request.existing_indexers == ["0x111"]
        assert request.num_candidates == 2
        assert request.blocklist == ["0xBAD"]
        assert request.pending_agreements == {"Qm123": ["0xPEND"]}
        assert request.declined_indexers == {"Qm123": ["0xDEC"]}

    def test_selection_request_missing_num_candidates_raises(self):
        """Verify ValidationError when num_candidates missing."""
        # Arrange & Act & Assert
        from pydantic import ValidationError as PydanticValidationError

        from iisa.iisa_http_endpoints import SelectionRequest

        with pytest.raises(PydanticValidationError) as exc_info:
            SelectionRequest(deployment_id="Qm123")

        assert "num_candidates" in str(exc_info.value)

    def test_selection_response(self):
        """deployment_id and indexers fields."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import SelectedIndexer, SelectionResponse

        response = SelectionResponse(
            deployment_id="Qm123",
            indexers=[
                SelectedIndexer(id="0xABC", min_grt_per_30_days=450.0),
                SelectedIndexer(id="0xXYZ"),
                SelectedIndexer(id="0x123"),
            ],
        )

        # Assert
        assert response.deployment_id == "Qm123"
        assert len(response.indexers) == 3
        assert response.indexers[0].id == "0xABC"
        assert response.indexers[0].min_grt_per_30_days == 450.0

    def test_selection_response_empty_indexers(self):
        """Verify empty indexers list is valid."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import SelectionResponse

        response = SelectionResponse(deployment_id="Qm123", indexers=[])

        # Assert
        assert response.deployment_id == "Qm123"
        assert response.indexers == []

    def test_health_response(self):
        """status and data_loaded fields."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import HealthResponse

        response = HealthResponse(status="healthy", data_loaded=True)

        # Assert
        assert response.status == "healthy"
        assert response.data_loaded is True


class TestIISAState:
    """Tests for IISAState lifecycle management."""

    def test_init_default_state(self):
        """Verify initial state has all fields set to None/False."""
        # Arrange & Act
        from iisa.iisa_http_endpoints import IISAState

        state = IISAState()

        # Assert
        assert state.settings is None
        assert state.data_manager is None
        assert state._history is None
        assert state._initialized is False

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_initialize_success(self, mock_loader_class, mock_dm_class):
        """Mock FileScoreLoader/DataManager, verify _initialized=True."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState, Settings

        mock_loader_instance = MagicMock()
        mock_loader_class.return_value = mock_loader_instance

        mock_dm_instance = MagicMock()
        mock_dm_class.return_value = mock_dm_instance

        state = IISAState()
        settings = Settings()

        # Act
        result = state.initialize(settings)

        # Assert
        assert result is True
        assert state._initialized is True
        assert state.settings is settings
        assert state.data_manager is mock_dm_instance
        mock_loader_class.assert_called_once()
        mock_dm_class.assert_called_once_with(mock_loader_instance)

    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_initialize_failure(self, mock_loader_class):
        """Mock FileScoreLoader to raise, verify returns False and logs warning."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState, Settings

        mock_loader_class.side_effect = Exception("Connection failed")

        state = IISAState()
        settings = Settings()

        # Act
        result = state.initialize(settings)

        # Assert
        assert result is False
        assert state._initialized is False

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_refresh_data_success(self, mock_loader_class, mock_dm_class):
        """Mock load_scores()=True, verify _history populated."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState, Settings

        mock_history_df = pd.DataFrame({"indexer": ["0xABC", "0xXYZ"]})

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.return_value = True
        mock_dm_instance.get_data.return_value = mock_history_df
        mock_dm_class.return_value = mock_dm_instance

        state = IISAState()
        settings = Settings()
        state.initialize(settings)

        # Act
        result = state.refresh_data()

        # Assert
        assert result is True
        assert state._history is not None
        assert len(state._history) == 2
        mock_dm_instance.load_scores.assert_called_once()

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_refresh_data_failure(self, mock_loader_class, mock_dm_class):
        """Mock load_scores()=False, verify returns False."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState, Settings

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.return_value = False
        mock_dm_class.return_value = mock_dm_instance

        state = IISAState()
        settings = Settings()
        state.initialize(settings)

        # Act
        result = state.refresh_data()

        # Assert
        assert result is False

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_refresh_data_exception(self, mock_loader_class, mock_dm_class):
        """Mock load_scores() to raise exception, verify returns False."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState, Settings

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.side_effect = Exception("Connection failed")
        mock_dm_class.return_value = mock_dm_instance

        state = IISAState()
        settings = Settings()
        state.initialize(settings)

        # Act
        result = state.refresh_data()

        # Assert
        assert result is False

    def test_refresh_data_not_initialized(self):
        """Call refresh before initialize, verify returns False."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState

        state = IISAState()

        # Act
        result = state.refresh_data()

        # Assert
        assert result is False

    def test_is_ready_with_data(self):
        """Set _history to non-empty DataFrame, verify is_ready=True."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState

        state = IISAState()
        state._history = pd.DataFrame({"indexer": ["0xABC"]})

        # Act & Assert
        assert state.is_ready is True

    def test_is_ready_without_data(self):
        """Verify is_ready=False when _history is None or empty."""
        # Arrange
        from iisa.iisa_http_endpoints import IISAState

        state = IISAState()

        # Act & Assert - None case
        assert state.is_ready is False

        # Arrange - empty DataFrame case
        state._history = pd.DataFrame()

        # Act & Assert
        assert state.is_ready is False


@pytest.fixture(autouse=True)
def reset_state(monkeypatch):
    """Reset global state and clean push-token env vars before each test.

    The module-level _state singleton and the lru_cache on get_settings
    otherwise leak between tests. Env vars set via monkeypatch.setenv in
    one test (e.g. IISA_PUSH_TOKEN) must not be visible to the next.
    """
    from iisa import iisa_http_endpoints

    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    iisa_http_endpoints.get_settings.cache_clear()
    iisa_http_endpoints._state = iisa_http_endpoints.IISAState()

    yield

    iisa_http_endpoints._state = iisa_http_endpoints.IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


@pytest.fixture
def mock_history_df():
    """Create a mock DataFrame simulating loaded history data."""
    return pd.DataFrame(
        {
            "indexer": ["0xabc", "0xxyz", "0x123"],  # lowercase for case-insensitive matching
            "url": ["https://a.com/", "https://b.com/", "https://c.com/"],
            "norm_lat_lin_reg_coefficient": [0.8, 0.9, 0.6],
            "norm_uptime_score": [0.9, 0.7, 0.95],
            "norm_success_rate": [0.85, 0.6, 0.9],
            "norm_stake_to_fees": [0.5, 0.8, 0.65],
            "norm_base_price_per_epoch": [0.7, 0.8, 0.9],
            "norm_price_per_entity": [0.6, 0.7, 0.8],
            "dips_info_available": [True, True, False],
            "dips_min_grt_per_30_days": [
                '{"arbitrum-one": "450"}',
                '{"arbitrum-one": "500"}',
                "{}",
            ],
            "dips_min_grt_per_billion_entities_per_30_days": ["200", "300", None],
            "dips_supported_networks": ['["arbitrum-one"]', '["arbitrum-one"]', "[]"],
        }
    )


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""

    def test_health_with_data_loaded(self, mock_history_df):
        """is_ready=True returns data_loaded=True."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints._state._history = mock_history_df
        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.get("/health")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["data_loaded"] is True

    def test_health_without_data(self):
        """is_ready=False returns data_loaded=False."""
        # Arrange
        from iisa.iisa_http_endpoints import app

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.get("/health")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["data_loaded"] is False

    def test_health_reports_computed_at_and_age(self):
        """Loaded scores expose computed_at and age; fresh scores stay healthy."""
        # Arrange
        from datetime import datetime, timedelta, timezone
        from unittest.mock import MagicMock

        import pandas as pd

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app
        from iisa.score_loader import DataManager, ScoresSnapshot

        state = iisa_http_endpoints._state
        orig_dm, orig_hist = state.data_manager, state._history
        try:
            dm = DataManager(MagicMock())
            ts = datetime.now(timezone.utc) - timedelta(hours=3)
            dm._snapshot = ScoresSnapshot(data=pd.DataFrame({"indexer": ["0xABC"]}), computed_at=ts)
            state.data_manager = dm
            state._history = pd.DataFrame({"indexer": ["0xABC"]})
            client = TestClient(app, raise_server_exceptions=False)

            # Act
            response = client.get("/health")

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["computed_at"] is not None
            assert 2.5 <= data["scores_age_hours"] <= 3.5
        finally:
            state.data_manager, state._history = orig_dm, orig_hist

    def test_health_degraded_when_scores_critically_stale(self):
        """Scores older than the critical threshold flip status to degraded."""
        # Arrange
        from datetime import datetime, timedelta, timezone
        from unittest.mock import MagicMock

        import pandas as pd

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app
        from iisa.score_loader import (
            STALE_SCORES_CRITICAL_HOURS,
            DataManager,
            ScoresSnapshot,
        )

        state = iisa_http_endpoints._state
        orig_dm, orig_hist = state.data_manager, state._history
        try:
            dm = DataManager(MagicMock())
            ts = datetime.now(timezone.utc) - timedelta(hours=STALE_SCORES_CRITICAL_HOURS + 1)
            dm._snapshot = ScoresSnapshot(data=pd.DataFrame({"indexer": ["0xABC"]}), computed_at=ts)
            state.data_manager = dm
            state._history = pd.DataFrame({"indexer": ["0xABC"]})
            client = TestClient(app, raise_server_exceptions=False)

            # Act
            response = client.get("/health")

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "degraded"
            assert data["scores_age_hours"] > STALE_SCORES_CRITICAL_HOURS
        finally:
            state.data_manager, state._history = orig_dm, orig_hist

    def test_health_no_computed_at_when_scores_absent(self):
        """With no loaded snapshot, computed_at and age are null and status healthy."""
        # Arrange
        from unittest.mock import MagicMock

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app
        from iisa.score_loader import DataManager

        state = iisa_http_endpoints._state
        orig_dm, orig_hist = state.data_manager, state._history
        try:
            dm = DataManager(MagicMock())  # snapshot defaults to (None, None)
            state.data_manager = dm
            state._history = None
            client = TestClient(app, raise_server_exceptions=False)

            # Act
            response = client.get("/health")

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["computed_at"] is None
            assert data["scores_age_hours"] is None
        finally:
            state.data_manager, state._history = orig_dm, orig_hist


class TestPushScoresEndpoint:
    """Tests for POST /scores endpoint (cronjob → iisa push)."""

    @staticmethod
    def _sample_payload():
        return [
            {
                "indexer": "0xABC",
                "computed_at": "2026-04-14T09:00:00+00:00",
                "lat_normalized_score": 0.8,
                "uptime_score": 0.95,
                "success_rate": 0.99,
                "stake_to_fees": 1000.0,
            }
        ]

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_push_scores_success(self, mock_loader_class, mock_dm_class, tmp_path, monkeypatch):
        """Valid body + no auth required (unset token) → 200 with row count."""
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = str(tmp_path / "scores.json")
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", scores_path)

        mock_dm_instance = MagicMock()
        mock_dm_instance.scores_file_path = scores_path
        mock_dm_instance.load_scores_from_df.return_value = True
        mock_dm_instance.get_data.return_value = pd.DataFrame({"indexer": ["0xABC"]})
        mock_dm_class.return_value = mock_dm_instance

        settings = Settings()  # push_token unset in test env
        iisa_http_endpoints._state.initialize(settings)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.post("/scores", json=self._sample_payload())

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["rows"] == 1
        # Disk was written atomically
        assert (tmp_path / "scores.json").exists()

    def test_push_scores_empty_body_rejected(self):
        """Empty payload → 422."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post("/scores", json=[])
        assert response.status_code == 422

    def test_push_scores_rejects_missing_token_when_required(self, monkeypatch):
        """IISA_PUSH_TOKEN set + no header → 401."""
        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post("/scores", json=self._sample_payload())
        assert response.status_code == 401
        assert response.json()["detail"] == "Missing or malformed Authorization header"

    def test_push_scores_rejects_wrong_token(self, monkeypatch):
        """IISA_PUSH_TOKEN set + wrong bearer → 401."""
        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/scores",
            json=self._sample_payload(),
            headers={"Authorization": "Bearer wrong"},
        )
        assert response.status_code == 401
        assert "Invalid" in response.json()["detail"]

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_push_scores_transform_failure_does_not_touch_disk(
        self, mock_loader_class, mock_dm_class, tmp_path, monkeypatch
    ):
        """
        Dry-run invariant: if transform_scores_df raises, the cache file
        must NOT be written. A restart should still load the previous
        valid payload, not a poisoned one.
        """
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = tmp_path / "scores.json"
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(scores_path))

        # Transform raises — simulates a schema-invalid payload.
        mock_dm_instance = MagicMock()
        mock_dm_instance.scores_file_path = str(scores_path)
        mock_dm_instance.transform_scores_df.side_effect = KeyError("missing column")
        mock_dm_class.return_value = mock_dm_instance

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post("/scores", json=self._sample_payload())

        # Handler 500s on the transform failure.
        assert response.status_code == 500
        # Critical invariant: disk was never touched. commit_scores was also
        # never called — memory and disk both untouched by the failed push.
        assert not scores_path.exists()
        mock_dm_instance.commit_scores.assert_not_called()

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_push_scores_accepts_valid_token(
        self, mock_loader_class, mock_dm_class, tmp_path, monkeypatch
    ):
        """IISA_PUSH_TOKEN set + matching bearer → 200."""
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = str(tmp_path / "scores.json")
        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", scores_path)
        iisa_http_endpoints.get_settings.cache_clear()

        mock_dm_instance = MagicMock()
        mock_dm_instance.scores_file_path = scores_path
        mock_dm_instance.load_scores_from_df.return_value = True
        mock_dm_instance.get_data.return_value = pd.DataFrame({"indexer": ["0xABC"]})
        mock_dm_class.return_value = mock_dm_instance

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/scores",
            json=self._sample_payload(),
            headers={"Authorization": "Bearer secret"},
        )
        assert response.status_code == 200

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_push_scores_accepts_lowercase_bearer(
        self, mock_loader_class, mock_dm_class, tmp_path, monkeypatch
    ):
        """RFC 6750 §2.1: the scheme prefix is case-insensitive."""
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = str(tmp_path / "scores.json")
        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", scores_path)
        iisa_http_endpoints.get_settings.cache_clear()

        mock_dm_instance = MagicMock()
        mock_dm_instance.scores_file_path = scores_path
        mock_dm_instance.get_data.return_value = pd.DataFrame({"indexer": ["0xABC"]})
        mock_dm_class.return_value = mock_dm_instance

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/scores",
            json=self._sample_payload(),
            headers={"Authorization": "bearer secret"},
        )
        assert response.status_code == 200

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_push_scores_unparseable_computed_at_coerces_to_nat(
        self, mock_loader_class, mock_dm_class, tmp_path, monkeypatch
    ):
        """
        A payload whose computed_at is not a valid timestamp should still
        succeed: the push path parses with errors="coerce", which turns
        bad values into NaT, which _extract_computed_at normalises to None.
        The computed_at on /scores/status should then be null.
        """
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = str(tmp_path / "scores.json")
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", scores_path)

        from iisa.score_loader import ScoresSnapshot

        mock_dm_instance = MagicMock()
        mock_dm_instance.scores_file_path = scores_path
        mock_dm_instance.snapshot = ScoresSnapshot(data=None, computed_at=None)
        mock_dm_instance.get_data.return_value = pd.DataFrame({"indexer": ["0xABC"]})

        def _commit(transformed_df, computed_at):
            mock_dm_instance.snapshot = ScoresSnapshot(data=transformed_df, computed_at=computed_at)

        mock_dm_instance.commit_scores.side_effect = _commit
        mock_dm_class.return_value = mock_dm_instance

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        payload = [
            {
                "indexer": "0xABC",
                "computed_at": "not a real timestamp",
                "lat_normalized_score": 0.8,
                "uptime_score": 0.95,
                "success_rate": 0.99,
            }
        ]

        response = client.post("/scores", json=payload)
        assert response.status_code == 200
        body = response.json()
        assert body["rows"] == 1

        # commit_scores was called with computed_at=None because the
        # malformed timestamp was coerced to NaT and normalised to None.
        commit_call = mock_dm_instance.commit_scores.call_args
        assert commit_call is not None
        assert commit_call.args[1] is None

        # /scores/status reflects the None computed_at.
        status_response = client.get("/scores/status")
        assert status_response.status_code == 200
        assert status_response.json()["computed_at"] is None

    def test_push_scores_missing_required_column_rejected(self, tmp_path, monkeypatch):
        """A payload missing a required column is rejected 422 without touching disk."""
        # Arrange — real DataManager so the boundary validation actually runs.
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = tmp_path / "scores.json"
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(scores_path))
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        bad = self._sample_payload()
        del bad[0]["uptime_score"]

        # Act
        response = client.post("/scores", json=bad)

        # Assert
        assert response.status_code == 422
        assert "uptime_score" in response.json()["detail"]
        assert not scores_path.exists()

    def test_push_scores_missing_indexer_rejected(self, tmp_path, monkeypatch):
        """A payload without the indexer identity column is rejected 422."""
        # Arrange
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = tmp_path / "scores.json"
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(scores_path))
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        bad = self._sample_payload()
        del bad[0]["indexer"]

        # Act
        response = client.post("/scores", json=bad)

        # Assert
        assert response.status_code == 422
        assert "indexer" in response.json()["detail"]
        assert not scores_path.exists()

    def test_push_scores_full_payload_accepted_end_to_end(self, tmp_path, monkeypatch):
        """A complete payload passes validation, writes disk, and returns the row count."""
        # Arrange — real DataManager, real transform, no mocks.
        from iisa import iisa_http_endpoints, score_loader
        from iisa.iisa_http_endpoints import Settings, app

        scores_path = tmp_path / "scores.json"
        monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(scores_path))
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post("/scores", json=self._sample_payload())

        # Assert
        assert response.status_code == 200
        assert response.json()["rows"] == 1
        assert scores_path.exists()


class TestScoresStatusEndpoint:
    """Tests for GET /scores/status endpoint (cronjob idempotency check)."""

    def test_returns_computed_at_when_loaded(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        # Seed state with a fake DataManager whose snapshot has both data
        # and computed_at — the endpoint reads them as a single pair.
        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame({"indexer": ["0xABC"]}),
            computed_at=datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/scores/status")

        assert response.status_code == 200
        body = response.json()
        assert body["computed_at"] is not None
        assert "2026-04-14" in body["computed_at"]
        assert body["rows"] == 1

    def test_returns_null_when_no_scores_loaded(self):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/scores/status")
        assert response.status_code == 200
        body = response.json()
        assert body["computed_at"] is None
        assert body["rows"] == 0


class TestScoresSnapshotEndpoint:
    """Tests for GET /scores (full snapshot read-back)."""

    def test_returns_records_with_computed_at_when_loaded(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        # Mix float, NaN, and string columns so the to_json round-trip hits the
        # NaN → null path. The cronjob pushes NaN for missing optional fields,
        # so the read-back must not 500 or emit invalid JSON.
        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame(
                {
                    "indexer": ["0xAAA", "0xBBB"],
                    "url": ["https://a.example/", "https://b.example/"],
                    "scoring_mode": ["full", "full"],
                    "lat_normalized_score": [0.9, float("nan")],
                }
            ),
            computed_at=datetime(2026, 4, 21, 11, 19, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/scores")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 2
        assert "2026-04-21" in body["computed_at"]
        assert len(body["scores"]) == 2
        indexers = {row["indexer"] for row in body["scores"]}
        assert indexers == {"0xAAA", "0xBBB"}
        # NaN must surface as JSON null, not NaN (invalid JSON) or skipped.
        nan_row = next(r for r in body["scores"] if r["indexer"] == "0xBBB")
        assert nan_row["lat_normalized_score"] is None

    def test_returns_empty_snapshot_when_no_scores_loaded(self):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/scores")
        assert response.status_code == 200
        body = response.json()
        assert body["computed_at"] is None
        assert body["count"] == 0
        assert body["scores"] == []

    def test_requires_bearer_when_push_token_set(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        # No Authorization header → 401.
        response = client.get("/scores")
        assert response.status_code == 401

        # Wrong token → 401.
        response = client.get("/scores", headers={"Authorization": "Bearer wrong"})
        assert response.status_code == 401

        # Correct token → 200 (even with no history loaded).
        response = client.get("/scores", headers={"Authorization": "Bearer secret"})
        assert response.status_code == 200


class TestScoresWeightedEndpoint:
    """Tests for GET /scores/weighted (bulk weighted scores)."""

    def test_returns_weighted_scores_for_each_indexer(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame(
                {
                    "indexer": ["0xabc", "0xxyz"],
                    "url": ["https://a.example/", "https://b.example/"],
                    "norm_lat_lin_reg_coefficient": [0.8, 0.6],
                    "norm_uptime_score": [0.9, 0.95],
                    "norm_success_rate": [0.85, 0.9],
                    "norm_stake_to_fees": [0.5, 0.65],
                    "norm_base_price_per_epoch": [0.7, 0.9],
                    "norm_price_per_entity": [0.6, 0.8],
                }
            ),
            computed_at=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/scores/weighted")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 2
        assert "2026-05-28" in body["computed_at"]
        assert len(body["scores"]) == 2

        entries_by_id = {e["indexer"]: e for e in body["scores"]}
        assert set(entries_by_id) == {"0xabc", "0xxyz"}

        abc = entries_by_id["0xabc"]
        assert isinstance(abc["weighted_score"], float)
        assert 0.0 <= abc["weighted_score"] <= 1.0
        assert abc["components"]["latency"] == 0.8
        assert abc["components"]["uptime"] == 0.9
        assert abc["components"]["success_rate"] == 0.85
        assert abc["components"]["stake_to_fees"] == 0.5
        assert abc["components"]["base_price"] == 0.7
        assert abc["components"]["price_per_entity"] == 0.6

        xyz = entries_by_id["0xxyz"]
        assert xyz["components"]["latency"] == 0.6
        assert xyz["components"]["uptime"] == 0.95
        assert xyz["components"]["success_rate"] == 0.9
        assert xyz["components"]["stake_to_fees"] == 0.65
        assert xyz["components"]["base_price"] == 0.9
        assert xyz["components"]["price_per_entity"] == 0.8

    def test_nan_component_normalised_to_zero(self):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame(
                {
                    "indexer": ["0xabc"],
                    "url": ["https://a.example/"],
                    "norm_lat_lin_reg_coefficient": [float("nan")],
                    "norm_uptime_score": [0.9],
                    "norm_success_rate": [0.85],
                    "norm_stake_to_fees": [0.5],
                    "norm_base_price_per_epoch": [0.7],
                    "norm_price_per_entity": [0.6],
                }
            ),
            computed_at=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/scores/weighted")
        assert response.status_code == 200
        body = response.json()
        components = body["scores"][0]["components"]
        # _normalize_metrics fills NaN with 0 across every norm_* column, so a
        # NaN latency input lands as a 0.0 latency in the response — not skipped.
        assert components["latency"] == 0.0
        assert components["uptime"] == 0.9

    def test_weighted_score_none_when_calculation_raises(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame(
                {
                    "indexer": ["0xabc"],
                    "url": ["https://a.example/"],
                    "norm_uptime_score": [0.9],
                }
            ),
            computed_at=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

        from iisa import indexer_selection

        def raises(df, weights):
            raise ValueError("forced failure")

        monkeypatch.setattr(indexer_selection, "_calculate_weighted_scores", raises)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/scores/weighted")
        assert response.status_code == 200
        body = response.json()
        assert body["scores"][0]["indexer"] == "0xabc"
        assert body["scores"][0]["weighted_score"] is None
        assert body["scores"][0]["components"]["uptime"] == 0.9

    def test_returns_empty_snapshot_when_no_scores_loaded(self):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/scores/weighted")
        assert response.status_code == 200
        body = response.json()
        assert body["computed_at"] is None
        assert body["count"] == 0
        assert body["scores"] == []

    def test_requires_bearer_when_push_token_set(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/scores/weighted")
        assert response.status_code == 401

        response = client.get("/scores/weighted", headers={"Authorization": "Bearer wrong"})
        assert response.status_code == 401

        response = client.get("/scores/weighted", headers={"Authorization": "Bearer secret"})
        assert response.status_code == 200


class TestDipsIndexersEndpoint:
    """Tests for GET /dips-indexers (the set of DIPs-accepting indexers)."""

    @staticmethod
    def _seed_snapshot(monkeypatch):
        """Snapshot covering each DIPs eligibility case: 0xaaa/0xbbb answered their
        probe and priced their chains; 0xccc (empty) and 0xddd (None) do not accept
        DIPs; 0xeee lists arbitrum-one in its supported networks but never priced it,
        so it is not eligible for any chain.
        """
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame(
                {
                    "indexer": ["0xaaa", "0xbbb", "0xccc", "0xddd", "0xeee"],
                    "url": [
                        "https://a.example/",
                        "https://b.example/",
                        "https://c.example/",
                        "https://d.example/",
                        "https://e.example/",
                    ],
                    "dips_info_available": [True, True, False, False, True],
                    "dips_supported_networks": [
                        '["arbitrum-one", "mainnet"]',
                        '["mainnet"]',
                        "[]",
                        None,
                        '["arbitrum-one"]',
                    ],
                    "dips_min_grt_per_30_days": [
                        '{"arbitrum-one": "100", "mainnet": "200"}',
                        '{"mainnet": "150"}',
                        "{}",
                        None,
                        "{}",
                    ],
                }
            ),
            computed_at=datetime(2026, 6, 2, 8, 30, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

    def test_missing_chain_is_rejected(self, monkeypatch):
        """``chain`` is required: omitting it is a 422, not an all-chains dump."""
        from iisa.iisa_http_endpoints import app

        self._seed_snapshot(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)
        assert client.get("/dips-indexers").status_code == 422

    def test_chain_filter_narrows_to_supporting_indexers(self, monkeypatch):
        from iisa.iisa_http_endpoints import app

        self._seed_snapshot(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)

        # arbitrum-one: 0xaaa supports and priced it; 0xeee supports but priced
        # nothing, so selection would drop it and so does this endpoint.
        arb = client.get("/dips-indexers", params={"chain": "arbitrum-one"})
        assert arb.status_code == 200
        assert arb.json()["count"] == 1
        assert arb.json()["indexers"] == ["0xaaa"]

        # mainnet is supported and priced by both 0xaaa and 0xbbb.
        mainnet = client.get("/dips-indexers", params={"chain": "mainnet"})
        assert mainnet.status_code == 200
        assert mainnet.json()["count"] == 2
        assert set(mainnet.json()["indexers"]) == {"0xaaa", "0xbbb"}

        # A chain no indexer supports yields an empty set, not an error.
        none = client.get("/dips-indexers", params={"chain": "optimism"})
        assert none.status_code == 200
        assert none.json()["count"] == 0
        assert none.json()["indexers"] == []

    def test_price_ceiling_drops_over_ceiling_indexers(self, monkeypatch):
        """``max_grt_per_30_days`` drops indexers priced above the ceiling, so the
        endpoint returns the pool selection would offer at that budget.
        """
        from iisa.iisa_http_endpoints import app

        self._seed_snapshot(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)

        # Without a ceiling, mainnet has 0xaaa (priced 200) and 0xbbb (priced 150).
        no_ceiling = client.get("/dips-indexers", params={"chain": "mainnet"})
        assert no_ceiling.json()["count"] == 2

        # A 175 ceiling drops 0xaaa (200) and keeps 0xbbb (150).
        capped = client.get(
            "/dips-indexers", params={"chain": "mainnet", "max_grt_per_30_days": 175}
        )
        assert capped.status_code == 200
        assert capped.json()["indexers"] == ["0xbbb"]

        # A ceiling above both prices keeps both.
        high = client.get("/dips-indexers", params={"chain": "mainnet", "max_grt_per_30_days": 250})
        assert set(high.json()["indexers"]) == {"0xaaa", "0xbbb"}

    def test_chain_filter_excludes_advertised_but_unpriced_indexer(self, monkeypatch):
        """An indexer that lists a chain but never priced it is excluded for that
        chain, matching what the selection path would actually pick.
        """
        from iisa.iisa_http_endpoints import app

        self._seed_snapshot(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)

        # 0xeee lists arbitrum-one in its supported networks but never priced it, so
        # (like the selection path) it is not eligible for that chain.
        arb = client.get("/dips-indexers", params={"chain": "arbitrum-one"}).json()
        assert "0xeee" not in arb["indexers"]
        assert arb["indexers"] == ["0xaaa"]

    def test_returns_empty_when_no_scores_loaded(self):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/dips-indexers", params={"chain": "arbitrum-one"})
        assert response.status_code == 200
        body = response.json()
        assert body["computed_at"] is None
        assert body["count"] == 0
        assert body["indexers"] == []

    def test_returns_empty_when_column_absent(self, monkeypatch):
        """A snapshot without the supported-networks column yields no indexers."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=pd.DataFrame({"indexer": ["0xaaa"]}),
            computed_at=datetime(2026, 6, 2, 8, 30, tzinfo=timezone.utc),
        )
        iisa_http_endpoints._state.data_manager = mock_dm

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/dips-indexers", params={"chain": "arbitrum-one"})
        assert response.status_code == 200
        body = response.json()
        assert "2026-06-02" in body["computed_at"]
        assert body["count"] == 0
        assert body["indexers"] == []

    def test_requires_bearer_when_push_token_set(self, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        client = TestClient(app, raise_server_exceptions=False)

        assert client.get("/dips-indexers", params={"chain": "arbitrum-one"}).status_code == 401
        assert (
            client.get(
                "/dips-indexers",
                params={"chain": "arbitrum-one"},
                headers={"Authorization": "Bearer wrong"},
            ).status_code
            == 401
        )
        assert (
            client.get(
                "/dips-indexers",
                params={"chain": "arbitrum-one"},
                headers={"Authorization": "Bearer secret"},
            ).status_code
            == 200
        )


class TestGetScoreEndpoint:
    """Tests for POST /get-score endpoint."""

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_get_score_found(self, mock_loader_class, mock_dm_class, mock_history_df):
        """Verify returns score and components for existing indexer."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.return_value = True
        mock_dm_instance.get_data.return_value = mock_history_df
        mock_dm_class.return_value = mock_dm_instance

        settings = Settings()
        iisa_http_endpoints._state.initialize(settings)
        iisa_http_endpoints._state.refresh_data()

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post("/get-score", json={"indexer_id": "0xABC"})

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["indexer_id"] == "0xABC"
        assert data["found"] is True
        assert data["weighted_score"] is not None
        assert isinstance(data["components"], dict)

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_get_score_not_found(self, mock_loader_class, mock_dm_class, mock_history_df):
        """Verify returns found=false for non-existent indexer."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.return_value = True
        mock_dm_instance.get_data.return_value = mock_history_df
        mock_dm_class.return_value = mock_dm_instance

        settings = Settings()
        iisa_http_endpoints._state.initialize(settings)
        iisa_http_endpoints._state.refresh_data()

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post("/get-score", json={"indexer_id": "0xNONEXISTENT"})

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["indexer_id"] == "0xNONEXISTENT"
        assert data["found"] is False
        assert data["weighted_score"] is None
        assert data["components"] is None

    def test_get_score_no_data(self):
        """Verify 503 when data not loaded."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints._state._history = None

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post("/get-score", json={"indexer_id": "A"})

        # Assert
        assert response.status_code == 503

    def test_requires_bearer_when_push_token_set(self, monkeypatch, mock_history_df):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())
        iisa_http_endpoints._state._history = mock_history_df
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post("/get-score", json={"indexer_id": "0xabc"})
        assert response.status_code == 401

        response = client.post(
            "/get-score",
            json={"indexer_id": "0xabc"},
            headers={"Authorization": "Bearer wrong"},
        )
        assert response.status_code == 401

        response = client.post(
            "/get-score",
            json={"indexer_id": "0xabc"},
            headers={"Authorization": "Bearer secret"},
        )
        assert response.status_code == 200

    def test_get_score_matches_scores_weighted_for_same_indexer(self):
        """/get-score and /scores/weighted agree, both normalising the full table.

        Regression test: /get-score used to normalise one indexer's row alone.
        Min-max normalisation of a single row is degenerate, so every indexer
        scored the same and disagreed with the bulk endpoint.
        """
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app
        from iisa.score_loader import ScoresSnapshot

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        # Raw metric columns (not pre-normalised), so _normalize_metrics scales
        # each relative to the set — the case single-row normalisation got wrong.
        data = pd.DataFrame(
            {
                "indexer": ["0xaaa", "0xbbb", "0xccc"],
                "url": ["https://a.example/", "https://b.example/", "https://c.example/"],
                "Latency Coefficient + Error Confidence Interval": [0.2, 0.5, 0.9],
                "% up_x": [99.5, 98.0, 96.0],
                "average_status": [0.99, 0.95, 0.90],
                "stake_to_fees": [3.0, 2.0, 1.0],
                "base_price_per_epoch": [100.0, 300.0, 500.0],
                "price_per_entity": [0.1, 0.3, 0.5],
            }
        )

        mock_dm = MagicMock()
        mock_dm.snapshot = ScoresSnapshot(
            data=data, computed_at=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc)
        )
        iisa_http_endpoints._state.data_manager = mock_dm
        iisa_http_endpoints._state._history = data

        client = TestClient(app, raise_server_exceptions=False)

        bulk = client.get("/scores/weighted")
        assert bulk.status_code == 200
        bulk_by_id = {e["indexer"]: e for e in bulk.json()["scores"]}

        single_scores = {}
        for indexer_id in ("0xaaa", "0xbbb", "0xccc"):
            single = client.post("/get-score", json={"indexer_id": indexer_id})
            assert single.status_code == 200
            body = single.json()
            assert body["weighted_score"] == pytest.approx(bulk_by_id[indexer_id]["weighted_score"])
            assert body["components"] == pytest.approx(bulk_by_id[indexer_id]["components"])
            single_scores[indexer_id] = body["weighted_score"]

        # Scores differ across indexers — not the degenerate constant the
        # single-row normalisation produced for every indexer.
        assert len(set(single_scores.values())) == 3


class TestSelectIndexersEndpoint:
    """Tests for POST /select-indexers endpoint."""

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_indexers_returns_deployment_id_and_indexers(
        self, mock_processor_class, mock_history_df
    ):
        """With data loaded, mock IndexerSelector, verify response structure."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        mock_processor = MagicMock()
        mock_processor.current_group = ["0xABC", "0xXYZ", "0x123"]
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "Qm123",
                "num_candidates": 3,
            },
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["deployment_id"] == "Qm123"
        assert len(data["indexers"]) == 3
        indexer_ids = [i["id"] for i in data["indexers"]]
        assert indexer_ids == ["0xABC", "0xXYZ", "0x123"]

    def test_select_indexers_no_data_returns_503(self):
        """Without data loaded, verify 503 returned."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints._state._history = None
        iisa_http_endpoints._state._initialized = False

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "Qm123",
                "num_candidates": 3,
            },
        )

        # Assert
        assert response.status_code == 503
        assert "IISA data not loaded" in response.json()["detail"]

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_indexers_processor_exception_returns_500(
        self, mock_processor_class, mock_history_df
    ):
        """IndexerSelector raises, verify 500 returned."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        mock_processor_class.side_effect = Exception("Processing failed")

        iisa_http_endpoints._state._history = mock_history_df
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "Qm123",
                "num_candidates": 3,
            },
        )

        # Assert
        assert response.status_code == 500
        assert "Selection failed: Processing failed" in response.json()["detail"]

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_indexers_zero_num_candidates(self, mock_processor_class, mock_history_df):
        """Verify empty list returned when num_candidates is 0."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints._state._history = mock_history_df
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "Qm123",
                "num_candidates": 0,
            },
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["deployment_id"] == "Qm123"
        assert len(data["indexers"]) == 0
        mock_processor_class.assert_not_called()

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_indexers_empty_result(self, mock_processor_class, mock_history_df):
        """IndexerSelector returns no selection, verify empty indexers list."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "Qm123",
                "num_candidates": 3,
            },
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["deployment_id"] == "Qm123"
        assert len(data["indexers"]) == 0

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_indexers_passes_target_size(self, mock_processor_class, mock_history_df):
        """Verify num_candidates passed as target_size to IndexerSelector."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        mock_processor = MagicMock()
        mock_processor.current_group = ["0xabc"]
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        # Act
        client.post(
            "/select-indexers",
            json={
                "deployment_id": "Qm123",
                "num_candidates": 5,
            },
        )

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["target_size"] == 5


class TestSelectWithProcessor:
    """Tests for _select_with_processor helper."""

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_returns_selection_response(
        self, mock_processor_class, mock_history_df
    ):
        """Mock IndexerSelector.current_group, verify SelectionResponse returned."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import (
            SelectionRequest,
            SelectionResponse,
            _select_with_processor,
        )

        mock_processor = MagicMock()
        mock_processor.current_group = ["0xABC", "0xXYZ", "0x123"]
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        request = SelectionRequest(
            deployment_id="Qm123",
            existing_indexers=["0xEXIST"],
            num_candidates=3,
        )

        # Act
        result = _select_with_processor(request)

        # Assert
        assert isinstance(result, SelectionResponse)
        assert result.deployment_id == "Qm123"
        assert len(result.indexers) == 3
        assert [i.id for i in result.indexers] == ["0xABC", "0xXYZ", "0x123"]
        mock_processor_class.assert_called_once()

    def test_select_with_processor_no_history(self):
        """_state.history=None, verify empty SelectionResponse."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        iisa_http_endpoints._state._history = None

        request = SelectionRequest(deployment_id="Qm123", num_candidates=3)

        # Act
        result = _select_with_processor(request)

        # Assert
        assert result.deployment_id == "Qm123"
        assert result.indexers == []

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_maps_blocklist(self, mock_processor_class, mock_history_df):
        """Verify blocklist mapped to indexer_denylist param."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        request = SelectionRequest(
            deployment_id="Qm123",
            blocklist=["0xBAD1", "0xBAD2"],
            num_candidates=3,
        )

        # Act
        _select_with_processor(request)

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["indexer_denylist"] == ["0xBAD1", "0xBAD2"]

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_builds_existing_agreements(
        self, mock_processor_class, mock_history_df
    ):
        """Verify existing_indexers mapped to existing_agreements dict."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        request = SelectionRequest(
            deployment_id="Qm123",
            existing_indexers=["0xEXIST1", "0xEXIST2"],
            num_candidates=3,
        )

        # Act
        _select_with_processor(request)

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["existing_agreements"] == {"Qm123": ["0xEXIST1", "0xEXIST2"]}

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_passes_pending_agreements(
        self, mock_processor_class, mock_history_df
    ):
        """Verify pending_agreements passed through."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        pending = {"Qm123": ["0xPEND1"], "Qm456": ["0xPEND2"]}
        request = SelectionRequest(
            deployment_id="Qm123",
            pending_agreements=pending,
            num_candidates=3,
        )

        # Act
        _select_with_processor(request)

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["pending_agreements"] == pending

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_passes_declined_indexers(
        self, mock_processor_class, mock_history_df
    ):
        """Verify declined_indexers passed through."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        declined = {"Qm123": ["0xDEC1", "0xDEC2"]}
        request = SelectionRequest(
            deployment_id="Qm123",
            declined_indexers=declined,
            num_candidates=3,
        )

        # Act
        _select_with_processor(request)

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["declined_indexers"] == declined

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_passes_target_size(self, mock_processor_class, mock_history_df):
        """Verify num_candidates passed as target_size to IndexerSelector."""
        # Arrange
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        request = SelectionRequest(
            deployment_id="Qm123",
            num_candidates=5,
        )

        # Act
        _select_with_processor(request)

        # Assert - target_size
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["target_size"] == 5

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_passes_synced_indexers(
        self, mock_processor_class, mock_history_df
    ):
        """Verify synced indexers from sync status threaded to IndexerSelector."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df

        # Set up sync status with known synced indexers
        mock_sync = MagicMock()
        mock_sync.synced_indexers_for.return_value = {"0xaaa", "0xbbb"}
        iisa_http_endpoints._state._sync_status = mock_sync

        request = SelectionRequest(
            deployment_id="Qm123",
            num_candidates=3,
        )

        # Act
        _select_with_processor(request)

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["synced_indexers"] == {"0xaaa", "0xbbb"}

        # Cleanup
        iisa_http_endpoints._state._sync_status = None

    @patch("iisa.iisa_http_endpoints.IndexerSelector")
    def test_select_with_processor_no_sync_status(self, mock_processor_class, mock_history_df):
        """Without sync status, synced_indexers is empty set."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import SelectionRequest, _select_with_processor

        mock_processor = MagicMock()
        mock_processor.current_group = []
        mock_processor_class.return_value = mock_processor

        iisa_http_endpoints._state._history = mock_history_df
        iisa_http_endpoints._state._sync_status = None

        request = SelectionRequest(
            deployment_id="Qm123",
            num_candidates=3,
        )

        # Act
        _select_with_processor(request)

        # Assert
        call_kwargs = mock_processor_class.call_args[1]
        assert call_kwargs["synced_indexers"] == set()


class TestPushSyncStatusEndpoint:
    """Tests for POST /sync-status endpoint (fetcher → iisa push)."""

    @staticmethod
    def _sample_payload():
        return {
            "0xAAA": {
                "deployments": ["QmDeploy1"],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        }

    def test_push_sync_status_success(self, tmp_path, monkeypatch):
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints.get_settings.cache_clear()
        settings = Settings()
        # Redirect the sync-status cache path into tmp_path
        settings.sync_status_file_path = str(tmp_path / "sync_status.json")
        iisa_http_endpoints._state.initialize(settings)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.post("/sync-status", json=self._sample_payload())

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["indexers"] == 1
        assert (tmp_path / "sync_status.json").exists()

    def test_push_sync_status_empty_object_accepted(self, tmp_path, monkeypatch):
        """Empty dict payload (no indexers synced) should be accepted."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints.get_settings.cache_clear()
        settings = Settings()
        settings.sync_status_file_path = str(tmp_path / "sync_status.json")
        iisa_http_endpoints._state.initialize(settings)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.post("/sync-status", json={})
        assert response.status_code == 200
        assert response.json()["indexers"] == 0

    def test_push_sync_status_rejects_wrong_token(self, monkeypatch):
        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import Settings, app

        iisa_http_endpoints.get_settings.cache_clear()
        iisa_http_endpoints._state.initialize(Settings())

        client = TestClient(app, raise_server_exceptions=False)
        response = client.post(
            "/sync-status",
            json=self._sample_payload(),
            headers={"Authorization": "Bearer wrong"},
        )
        assert response.status_code == 401


class TestRequirePushTokenStartup:
    """Tests for the IISA_REQUIRE_PUSH_TOKEN hard-fail gate on lifespan."""

    def test_startup_fails_when_required_and_token_missing(self, monkeypatch):
        """IISA_REQUIRE_PUSH_TOKEN=true + unset token → RuntimeError at startup."""
        monkeypatch.setenv("IISA_REQUIRE_PUSH_TOKEN", "true")
        monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints.get_settings.cache_clear()

        # TestClient's context manager runs lifespan on __enter__. The
        # RuntimeError from the gate surfaces there.
        with pytest.raises(RuntimeError, match="IISA_PUSH_TOKEN is required"):
            with TestClient(app) as _client:
                pass

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_startup_succeeds_when_required_and_token_set(
        self, mock_loader_class, mock_dm_class, monkeypatch
    ):
        """IISA_REQUIRE_PUSH_TOKEN=true + token set → service starts normally."""
        monkeypatch.setenv("IISA_REQUIRE_PUSH_TOKEN", "true")
        monkeypatch.setenv("IISA_PUSH_TOKEN", "secret")

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints.get_settings.cache_clear()

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.return_value = False  # empty cache is fine
        mock_dm_class.return_value = mock_dm_instance

        with TestClient(app) as client:
            response = client.get("/health")
            assert response.status_code == 200

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_startup_warns_when_not_required_and_token_missing(
        self, mock_loader_class, mock_dm_class, monkeypatch, caplog
    ):
        """No require flag, no token → startup WARNING, service accepts requests."""
        import logging

        monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
        monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)

        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        iisa_http_endpoints.get_settings.cache_clear()

        mock_dm_instance = MagicMock()
        mock_dm_instance.load_scores.return_value = False
        mock_dm_class.return_value = mock_dm_instance

        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            with TestClient(app) as client:
                response = client.get("/health")
                assert response.status_code == 200

        assert any("IISA_PUSH_TOKEN is not set" in rec.message for rec in caplog.records)


class TestExtractChainPrice:
    """Tests for _extract_chain_price -- parses per-chain price from JSON blob."""

    def test_extracts_price_for_matching_chain(self):
        from iisa.iisa_http_endpoints import _extract_chain_price

        prices_json = json.dumps({"arbitrum-one": 450.0, "mainnet": 200.0})
        assert _extract_chain_price(prices_json, "arbitrum-one") == 450.0
        assert _extract_chain_price(prices_json, "mainnet") == 200.0

    def test_returns_none_for_missing_chain(self):
        from iisa.iisa_http_endpoints import _extract_chain_price

        prices_json = json.dumps({"arbitrum-one": 450.0})
        assert _extract_chain_price(prices_json, "optimism") is None

    def test_returns_none_for_empty_json(self):
        from iisa.iisa_http_endpoints import _extract_chain_price

        assert _extract_chain_price("{}", "arbitrum-one") is None

    def test_returns_none_for_malformed_json(self):
        from iisa.iisa_http_endpoints import _extract_chain_price

        assert _extract_chain_price("not json", "arbitrum-one") is None

    def test_returns_none_for_none_input(self):
        from iisa.iisa_http_endpoints import _extract_chain_price

        assert _extract_chain_price(None, "arbitrum-one") is None

    def test_converts_string_price_to_float(self):
        from iisa.iisa_http_endpoints import _extract_chain_price

        prices_json = json.dumps({"arbitrum-one": "450.5"})
        assert _extract_chain_price(prices_json, "arbitrum-one") == 450.5


class TestFilterByPrice:
    """Tests for _filter_by_price -- excludes indexers by DIP info, chain, and budget."""

    def _make_history(self, rows):
        return pd.DataFrame(rows)

    def test_no_filtering_when_chain_id_is_none(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history([{"indexer": "0xA", "dips_info_available": False}])
        result, reason = _filter_by_price(df, None, None)
        assert len(result) == 1

    def test_excludes_indexers_without_dips_info(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                {
                    "indexer": "0xA",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 100}),
                },
                {
                    "indexer": "0xB",
                    "dips_info_available": False,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 100}),
                },
            ]
        )
        result, reason = _filter_by_price(df, "arb", None)
        assert len(result) == 1
        assert result.iloc[0]["indexer"] == "0xA"

    def test_all_lacking_dips_info_returns_empty_with_reason(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                {"indexer": "0xA", "dips_info_available": False},
                {"indexer": "0xB", "dips_info_available": False},
            ]
        )
        result, reason = _filter_by_price(df, "arb", None)
        assert result.empty
        assert "lack DIP info" in reason

    def test_excludes_indexers_not_supporting_chain(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                {
                    "indexer": "0xA",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arbitrum-one"]),
                    "dips_min_grt_per_30_days": json.dumps({"arbitrum-one": 100}),
                },
                {
                    "indexer": "0xB",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["mainnet"]),
                    "dips_min_grt_per_30_days": json.dumps({"mainnet": 200}),
                },
            ]
        )
        result, reason = _filter_by_price(df, "arbitrum-one", None)
        assert len(result) == 1
        assert result.iloc[0]["indexer"] == "0xA"

    def test_excludes_indexers_over_budget(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                {
                    "indexer": "0xA",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 100}),
                },
                {
                    "indexer": "0xB",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 500}),
                },
            ]
        )
        result, reason = _filter_by_price(df, "arb", 200.0)
        assert len(result) == 1
        assert result.iloc[0]["indexer"] == "0xA"

    def test_all_over_budget_returns_empty_with_reason(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                {
                    "indexer": "0xA",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 500}),
                },
            ]
        )
        result, reason = _filter_by_price(df, "arb", 200.0)
        assert result.empty
        assert "exceed payment ceiling" in reason

    def test_indexer_without_chain_pricing_excluded(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                {
                    "indexer": "0xA",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"mainnet": 100}),
                },
            ]
        )
        result, reason = _filter_by_price(df, "arb", None)
        assert result.empty

    def _priced(self, indexer, networks, prices, dips_info=True):
        return {
            "indexer": indexer,
            "dips_info_available": dips_info,
            "dips_supported_networks": json.dumps(networks),
            "dips_min_grt_per_30_days": json.dumps(prices),
        }

    def test_returns_every_row_when_dips_info_column_missing(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history([{"indexer": "0xA"}, {"indexer": "0xB"}])
        result, reason = _filter_by_price(df, "arb", 1.0)
        assert list(result["indexer"]) == ["0xA", "0xB"]
        assert reason == ""

    def test_reason_text_when_all_lack_dips_info(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                self._priced("0xA", ["arb"], {"arb": 100}, dips_info=False),
                self._priced("0xB", ["arb"], {"arb": 100}, dips_info=False),
            ]
        )
        _, reason = _filter_by_price(df, "arb", None)
        assert reason == "all 2 indexers lack DIP info (dips_info_available=False)"

    def test_reason_text_when_none_support_chain(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                self._priced("0xA", ["mainnet"], {"arb": 100}),
                self._priced("0xB", ["mainnet"], {"arb": 100}),
            ]
        )
        result, reason = _filter_by_price(df, "arb", None)
        assert result.empty
        assert reason == "none of 2 indexers support chain 'arb'"

    def test_reason_text_when_none_priced_for_chain(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history([self._priced("0xA", ["arb"], {"mainnet": 100})])
        _, reason = _filter_by_price(df, "arb", None)
        assert reason == "none of 1 indexers have pricing configured for chain 'arb'"

    def test_reason_text_when_all_over_budget(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history([self._priced("0xA", ["arb"], {"arb": 500})])
        _, reason = _filter_by_price(df, "arb", 200.0)
        assert reason == "all 1 indexers exceed payment ceiling of 200.0 GRT/30d for chain 'arb'"

    def test_unparsable_chain_price_counts_as_unpriced(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history([self._priced("0xA", ["arb"], {"arb": "abc"})])
        result, reason = _filter_by_price(df, "arb", 200.0)
        assert result.empty
        assert reason == "none of 1 indexers have pricing configured for chain 'arb'"

    def test_price_equal_to_budget_is_kept(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history([self._priced("0xA", ["arb"], {"arb": 200})])
        result, reason = _filter_by_price(df, "arb", 200.0)
        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""

    def test_skips_chain_step_when_networks_column_missing(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        row = self._priced("0xA", [], {"arb": 100})
        del row["dips_supported_networks"]
        result, reason = _filter_by_price(self._make_history([row]), "arb", 200.0)
        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""

    def test_skips_price_and_budget_steps_when_price_column_missing(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        row = self._priced("0xA", ["arb"], {})
        del row["dips_min_grt_per_30_days"]
        result, reason = _filter_by_price(self._make_history([row]), "arb", 1.0)
        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""

    def test_does_not_modify_the_input_frame(self):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                self._priced("0xA", ["arb"], {"arb": 100}),
                self._priced("0xB", ["arb"], {"arb": 500}),
            ]
        )
        _filter_by_price(df, "arb", 200.0)
        assert list(df["indexer"]) == ["0xA", "0xB"]

    def test_logs_each_step_at_debug(self, caplog):
        from iisa.iisa_http_endpoints import _filter_by_price

        df = self._make_history(
            [
                self._priced("0xA", ["arb"], {"arb": 100}),
                self._priced("0xB", ["arb"], {"arb": 500}),
                self._priced("0xC", ["arb"], {"arb": 100}, dips_info=False),
                self._priced("0xD", ["mainnet"], {"mainnet": 100}),
            ]
        )
        with caplog.at_level(logging.DEBUG, logger="iisa-service"):
            result, reason = _filter_by_price(df, "arb", 200.0)

        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""
        assert [r.getMessage() for r in caplog.records if r.name == "iisa-service"] == [
            "price filter: 3/4 indexers have DIP info",
            "price filter: 2/3 indexers support chain 'arb'",
            "price filter: 1/2 indexers within budget of 200.0 GRT/30d for chain 'arb'",
        ]


class TestPriceFilterSteps:
    """Tests for the single price filter steps that _filter_by_price runs in order."""

    @pytest.mark.parametrize(
        "step_name", ["_keep_supporting_chain", "_keep_with_chain_price", "_keep_within_budget"]
    )
    def test_passes_rows_through_when_its_column_is_missing(self, step_name):
        from iisa import iisa_http_endpoints

        step = getattr(iisa_http_endpoints, step_name)
        result, reason = step(pd.DataFrame([{"indexer": "0xA"}]), "arb", 1.0)
        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""

    def test_keep_within_budget_passes_rows_through_without_a_ceiling(self):
        from iisa.iisa_http_endpoints import _keep_within_budget

        df = pd.DataFrame(
            [{"indexer": "0xA", "dips_min_grt_per_30_days": json.dumps({"arb": 500})}]
        )
        result, reason = _keep_within_budget(df, "arb", None)
        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""

    def test_keep_within_budget_drops_unpriced_rows(self):
        from iisa.iisa_http_endpoints import _keep_within_budget

        df = pd.DataFrame(
            [
                {"indexer": "0xA", "dips_min_grt_per_30_days": json.dumps({"arb": 100})},
                {"indexer": "0xB", "dips_min_grt_per_30_days": json.dumps({"mainnet": 1})},
            ]
        )
        result, reason = _keep_within_budget(df, "arb", 200.0)
        assert list(result["indexer"]) == ["0xA"]
        assert reason == ""


class TestBuildSelectedIndexers:
    """Tests for _build_selected_indexers -- extracts chain-specific price into response."""

    def test_returns_chain_specific_price(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps(
                        {"arbitrum-one": 450.0, "mainnet": 200.0}
                    ),
                    "dips_min_grt_per_billion_entities_per_30_days": 2000.0,
                }
            ]
        )
        result = _build_selected_indexers(["0xa"], history, "arbitrum-one")
        assert len(result) == 1
        assert result[0].min_grt_per_30_days == 450.0
        assert result[0].min_grt_per_billion_entities_per_30_days == 2000.0

    def test_returns_none_when_chain_not_in_pricing(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps({"mainnet": 200.0}),
                    "dips_min_grt_per_billion_entities_per_30_days": None,
                }
            ]
        )
        result = _build_selected_indexers(["0xa"], history, "arbitrum-one")
        assert result[0].min_grt_per_30_days is None

    def test_returns_none_when_no_chain_id(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps({"arbitrum-one": 450.0}),
                }
            ]
        )
        result = _build_selected_indexers(["0xa"], history, None)
        assert result[0].min_grt_per_30_days is None

    def test_indexer_not_in_history_returns_none_pricing(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame([{"indexer": "0xother"}])
        result = _build_selected_indexers(["0xa"], history, "arbitrum-one")
        assert result[0].min_grt_per_30_days is None

    def test_returns_none_prices_when_price_columns_missing(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame([{"indexer": "0xa"}])
        result = _build_selected_indexers(["0xa"], history, "arbitrum-one")
        assert result[0].min_grt_per_30_days is None
        assert result[0].min_grt_per_billion_entities_per_30_days is None

    @pytest.mark.parametrize(
        "entity_value, expected",
        [(float("nan"), None), ("2000", 2000.0), ("abc", None), (0, 0.0)],
    )
    def test_entity_price_parsing(self, entity_value, expected):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps({"arbitrum-one": 450.0}),
                    "dips_min_grt_per_billion_entities_per_30_days": entity_value,
                }
            ]
        )
        result = _build_selected_indexers(["0xa"], history, "arbitrum-one")
        assert result[0].min_grt_per_billion_entities_per_30_days == expected

    def test_entity_price_is_none_when_no_chain_id(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame(
            [{"indexer": "0xa", "dips_min_grt_per_billion_entities_per_30_days": 2000.0}]
        )
        result = _build_selected_indexers(["0xa"], history, None)
        assert result[0].min_grt_per_billion_entities_per_30_days is None

    def test_uses_first_row_and_keeps_requested_order(self):
        from iisa.iisa_http_endpoints import _build_selected_indexers

        history = pd.DataFrame(
            [
                {"indexer": "0xa", "dips_min_grt_per_30_days": json.dumps({"arb": 1.0})},
                {"indexer": "0xb", "dips_min_grt_per_30_days": json.dumps({"arb": 2.0})},
                {"indexer": "0xa", "dips_min_grt_per_30_days": json.dumps({"arb": 3.0})},
            ]
        )
        result = _build_selected_indexers(["0xb", "0xa"], history, "arb")
        assert [(r.id, r.min_grt_per_30_days) for r in result] == [("0xb", 2.0), ("0xa", 1.0)]


class TestIndexerPrices:
    """Tests for _indexer_prices -- one indexer's chain price and entity price."""

    HISTORY = pd.DataFrame(
        [
            {
                "indexer": "0xa",
                "dips_min_grt_per_30_days": json.dumps({"arb": 450.0}),
                "dips_min_grt_per_billion_entities_per_30_days": 2000.0,
            }
        ]
    )

    @pytest.mark.parametrize(
        "history, idx_id, chain_id, expected",
        [
            (HISTORY, "0xa", "arb", (450.0, 2000.0)),
            (HISTORY, "0xa", "mainnet", (None, 2000.0)),
            (HISTORY, "0xa", None, (None, None)),
            (HISTORY, "0xother", "arb", (None, None)),
            (HISTORY[["indexer"]], "0xa", "arb", (None, None)),
            (HISTORY.drop(columns=["dips_min_grt_per_30_days"]), "0xa", "arb", (None, 2000.0)),
        ],
    )
    def test_returns_chain_and_entity_price(self, history, idx_id, chain_id, expected):
        from iisa.iisa_http_endpoints import _indexer_prices

        assert _indexer_prices(history, idx_id, chain_id) == expected


class TestLogSelectionReasoning:
    """Tests for _log_selection_reasoning -- one score breakdown line per selected indexer."""

    WEIGHTS = {
        "stake_to_fees": 0.5,
        "base_price_per_epoch": 0.25,
        "lat_lin_reg_coefficient": 0.25,
        "success_rate": 0.5,
        "price_per_entity": 0.1,
    }

    def _processor(self, data, current_group, weights=None):
        from types import SimpleNamespace

        return SimpleNamespace(
            data=data,
            current_group=current_group,
            weights=self.WEIGHTS if weights is None else weights,
        )

    def _log_lines(self, processor, caplog):
        from iisa.iisa_http_endpoints import _log_selection_reasoning

        with caplog.at_level(logging.INFO, logger="iisa-service"):
            _log_selection_reasoning(processor, "QmDeployment")
        return [r.getMessage() for r in caplog.records if r.name == "iisa-service"]

    def test_logs_breakdown_for_selected_indexers_only(self, caplog):
        nan = float("nan")
        data = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "norm_stake_to_fees": 0.5,
                    "norm_base_price_per_epoch": 1.0,
                    "norm_lat_lin_reg_coefficient": 0.25,
                    "norm_uptime_score": 1.0,
                    "norm_success_rate": 0.0,
                    "norm_price_per_entity": nan,
                    "weighted_score": 0.123456,
                },
                {
                    "indexer": "0xb",
                    "norm_stake_to_fees": nan,
                    "norm_base_price_per_epoch": nan,
                    "norm_lat_lin_reg_coefficient": nan,
                    "norm_uptime_score": nan,
                    "norm_success_rate": nan,
                    "norm_price_per_entity": nan,
                    "weighted_score": nan,
                },
                {
                    "indexer": "0xc",
                    "norm_stake_to_fees": 1.0,
                    "norm_base_price_per_epoch": 1.0,
                    "norm_lat_lin_reg_coefficient": 1.0,
                    "norm_uptime_score": 1.0,
                    "norm_success_rate": 1.0,
                    "norm_price_per_entity": 1.0,
                    "weighted_score": 1.0,
                },
            ]
        )

        lines = self._log_lines(self._processor(data, ["0xb", "0xa"]), caplog)

        # Uptime has no weight and price_per_entity is NaN, so both are left out.
        assert lines == [
            "selected indexer=0xa score=0.1235 "
            "components={'stake_to_fees': 0.5, 'base_price': 1.0, 'latency': 0.25, "
            "'success_rate': 0.0} "
            "weights={'stake_to_fees': 0.5, 'base_price': 0.25, 'latency': 0.25, "
            "'success_rate': 0.5} "
            "contributions={'stake_to_fees': 0.1667, 'base_price': 0.1667, "
            "'latency': 0.0417, 'success_rate': 0.0} "
            "deployment=QmDeployment",
            "selected indexer=0xb score=0.0000 components={} weights={} contributions={} "
            "deployment=QmDeployment",
        ]

    def test_zero_total_weight_logs_no_contributions(self, caplog):
        data = pd.DataFrame([{"indexer": "0xa", "norm_stake_to_fees": 0.5}])

        lines = self._log_lines(self._processor(data, ["0xa"], {"stake_to_fees": 0.0}), caplog)

        assert lines == [
            "selected indexer=0xa score=0.0000 components={'stake_to_fees': 0.5} "
            "weights={'stake_to_fees': 0.0} contributions={} deployment=QmDeployment"
        ]

    @pytest.mark.parametrize(
        "data, current_group",
        [
            (None, ["0xa"]),
            (pd.DataFrame(), ["0xa"]),
            (pd.DataFrame([{"indexer": "0xa"}]), []),
        ],
    )
    def test_logs_nothing_without_data_or_selection(self, data, current_group, caplog):
        assert self._log_lines(self._processor(data, current_group), caplog) == []


class TestScoreBreakdown:
    """Tests for _score_breakdown -- one indexer's rounded score parts for the selection log."""

    def test_splits_score_over_weighted_metrics(self):
        from iisa.iisa_http_endpoints import _score_breakdown

        row = pd.Series(
            {
                "norm_stake_to_fees": 1.0,
                "norm_uptime_score": 0.5,
                "norm_success_rate": float("nan"),
                "weighted_score": 0.75,
            }
        )
        weights = {"stake_to_fees": 0.25, "uptime_score": 0.25, "success_rate": 0.5}

        assert _score_breakdown(row, weights) == (
            0.75,
            {"stake_to_fees": 1.0, "uptime": 0.5},
            {"stake_to_fees": 0.25, "uptime": 0.25},
            {"stake_to_fees": 0.5, "uptime": 0.25},
        )

    def test_returns_no_score_and_no_contributions_when_unweighted(self):
        from iisa.iisa_http_endpoints import _score_breakdown

        row = pd.Series({"norm_stake_to_fees": 1.0})

        assert _score_breakdown(row, {"stake_to_fees": 0.0}) == (
            None,
            {"stake_to_fees": 1.0},
            {"stake_to_fees": 0.0},
            {},
        )


class TestEnrichWithChainPrices:
    """Tests for _enrich_with_chain_prices -- adds price columns for scoring."""

    def test_adds_chain_specific_base_price(self):
        from iisa.iisa_http_endpoints import _enrich_with_chain_prices

        df = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps({"arb": 450.0}),
                    "dips_min_grt_per_billion_entities_per_30_days": 2000.0,
                }
            ]
        )
        result = _enrich_with_chain_prices(df, "arb")
        assert result.iloc[0]["base_price_per_epoch"] == 450.0
        assert result.iloc[0]["price_per_entity"] == 2000.0

    def test_zero_price_when_chain_not_found(self):
        from iisa.iisa_http_endpoints import _enrich_with_chain_prices

        df = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps({"mainnet": 200.0}),
                }
            ]
        )
        result = _enrich_with_chain_prices(df, "arb")
        assert result.iloc[0]["base_price_per_epoch"] == 0.0

    def test_zero_price_when_no_chain_id(self):
        from iisa.iisa_http_endpoints import _enrich_with_chain_prices

        df = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_min_grt_per_30_days": json.dumps({"arb": 450.0}),
                }
            ]
        )
        result = _enrich_with_chain_prices(df, None)
        assert result.iloc[0]["base_price_per_epoch"] == 0.0


class TestSelectIndexersEndToEndPricing:
    """Integration test: /select-indexers must not return indexers with None pricing."""

    def test_excludes_indexers_without_dips_info_from_response(self):
        """Indexers without DIP info or without pricing for the requested chain
        must not appear in the /select-indexers response. If they did, dipper
        would fall back to its static pricing_table (potentially 10x the market
        rate) with no signal."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        # 3 indexers: 0xa has DIP info + "arb" pricing (eligible); 0xb has DIP
        # info but only "mainnet" pricing (wrong chain); 0xc has no DIP info.
        history = pd.DataFrame(
            [
                {
                    "indexer": "0xa",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 450.0}),
                    "dips_min_grt_per_billion_entities_per_30_days": 2000.0,
                },
                {
                    "indexer": "0xb",
                    "dips_info_available": True,
                    "dips_supported_networks": json.dumps(["mainnet"]),
                    "dips_min_grt_per_30_days": json.dumps({"mainnet": 200.0}),
                    "dips_min_grt_per_billion_entities_per_30_days": 1000.0,
                },
                {
                    "indexer": "0xc",
                    "dips_info_available": False,
                    "dips_supported_networks": json.dumps(["arb"]),
                    "dips_min_grt_per_30_days": json.dumps({"arb": 100.0}),
                    "dips_min_grt_per_billion_entities_per_30_days": 500.0,
                },
            ]
        )

        iisa_http_endpoints._state._history = history
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "QmTest123",
                "chain_id": "arb",
                "num_candidates": 3,
            },
        )

        assert response.status_code == 200
        data = response.json()
        indexers = data["indexers"]

        # Only 0xa should be returned
        indexer_ids = [i["id"] for i in indexers]
        assert "0xa" in indexer_ids
        assert "0xb" not in indexer_ids, (
            "indexer without pricing for requested chain should be excluded"
        )
        assert "0xc" not in indexer_ids, "indexer without DIP info should be excluded"

        # The returned indexer must have a non-None price
        for indexer in indexers:
            assert indexer["min_grt_per_30_days"] is not None, (
                f"indexer {indexer['id']} returned with None pricing -- "
                "dipper would fall back to static pricing_table"
            )

    @staticmethod
    def _row(address, chain, price, loc="US", org="org1"):
        """Build a minimal indexer row with all columns IndexerSelector needs."""
        return {
            "indexer": address,
            "dips_info_available": True,
            "dips_supported_networks": json.dumps([chain]),
            "dips_min_grt_per_30_days": json.dumps({chain: price}),
            "dips_min_grt_per_billion_entities_per_30_days": 500.0,
            "destination_loc": loc,
            "org": org,
        }

    def test_budget_enforcement_excludes_expensive_indexers(self):
        """Indexers whose price exceeds max_grt_per_30_days must be excluded."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        history = pd.DataFrame(
            [
                self._row("0xcheap", "arb", 100.0),
                self._row("0xmid", "arb", 300.0),
                self._row("0xexpensive", "arb", 5000.0),
            ]
        )

        iisa_http_endpoints._state._history = history
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "QmBudgetTest",
                "chain_id": "arb",
                "num_candidates": 3,
                "max_grt_per_30_days": 400.0,
            },
        )

        assert response.status_code == 200
        indexer_ids = [i["id"] for i in response.json()["indexers"]]
        assert "0xcheap" in indexer_ids
        assert "0xmid" in indexer_ids
        assert "0xexpensive" not in indexer_ids, (
            "indexer priced at 5000 GRT/30d should be excluded with budget of 400"
        )

    def test_blocklist_excludes_indexers(self):
        """Blocklisted indexers must not appear in the response."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        history = pd.DataFrame(
            [
                self._row("0xgood", "arb", 100.0),
                self._row("0xblocked", "arb", 100.0),
            ]
        )

        iisa_http_endpoints._state._history = history
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "QmBlocklistTest",
                "chain_id": "arb",
                "num_candidates": 2,
                "blocklist": ["0xblocked"],
            },
        )

        assert response.status_code == 200
        indexer_ids = [i["id"] for i in response.json()["indexers"]]
        assert "0xgood" in indexer_ids
        assert "0xblocked" not in indexer_ids, "blocklisted indexer should be excluded"

    def test_num_candidates_caps_response_size(self):
        """Response must not contain more indexers than num_candidates."""
        from iisa import iisa_http_endpoints
        from iisa.iisa_http_endpoints import app

        history = pd.DataFrame(
            [
                self._row(f"0x{i:040x}", "arb", 100.0 + i, loc=f"loc{i}", org=f"org{i}")
                for i in range(5)
            ]
        )

        iisa_http_endpoints._state._history = history
        iisa_http_endpoints._state._initialized = True

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/select-indexers",
            json={
                "deployment_id": "QmCapTest",
                "chain_id": "arb",
                "num_candidates": 2,
            },
        )

        assert response.status_code == 200
        indexers = response.json()["indexers"]
        assert len(indexers) <= 2, f"requested 2 candidates but got {len(indexers)}"
