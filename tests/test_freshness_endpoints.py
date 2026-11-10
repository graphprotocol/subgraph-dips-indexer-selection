"""Tests for POST /freshness and GET /freshness."""

import json
import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints
from iisa.iisa_http_endpoints import IISAState, Settings, app

INDEXER = "0x" + "a1" * 20
OTHER = "0x" + "b2" * 20
CHAIN = "arbitrum-one"
TOKEN = "fake-freshness-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def _minute(minutes_ago: int = 2) -> str:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return (now - timedelta(minutes=minutes_ago)).isoformat().replace("+00:00", "Z")


def _outcome(**overrides: Any) -> dict[str, Any]:
    outcome: dict[str, Any] = {
        "indexer": INDEXER,
        "chain": CHAIN,
        "outcome": "pass",
        "reason": None,
        "detail": None,
        "deployment": "QmFakeDeployment",
        "age_seconds": 0.8,
    }
    outcome.update(overrides)
    return outcome


def _push(minute: str, *outcomes: dict[str, Any]) -> dict[str, Any]:
    return {"minute": minute, "outcomes": list(outcomes)}


@pytest.fixture
def freshness_file(tmp_path, monkeypatch) -> Iterator[Any]:
    """A fresh, initialised service state with its freshness file in a temporary folder."""
    path = tmp_path / "freshness.json"
    monkeypatch.delenv("IISA_PUSH_TOKEN", raising=False)
    monkeypatch.delenv("IISA_REQUIRE_PUSH_TOKEN", raising=False)
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(path))
    iisa_http_endpoints.get_settings.cache_clear()
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints._state.initialize(Settings())
    yield path
    iisa_http_endpoints._state = IISAState()
    iisa_http_endpoints.get_settings.cache_clear()


@pytest.fixture
def client(freshness_file) -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def token(monkeypatch) -> None:
    monkeypatch.setenv("IISA_PUSH_TOKEN", TOKEN)
    iisa_http_endpoints.get_settings.cache_clear()


class TestPush:
    def test_good_push_is_applied_and_saved(self, client, freshness_file):
        minute = _minute()
        response = client.post(
            "/freshness",
            json=_push(minute, _outcome(), _outcome(indexer=OTHER, outcome="no_evidence")),
        )

        assert response.status_code == 200, response.text
        assert response.json() == {
            "status": "success",
            "minute": minute,
            "applied": True,
            "pairs": 2,
        }
        assert json.loads(freshness_file.read_text())["version"] == 1

    def test_empty_push_is_applied(self, client):
        response = client.post("/freshness", json=_push(_minute()))

        assert response.status_code == 200
        assert (response.json()["applied"], response.json()["pairs"]) == (True, 0)

    def test_repeated_or_older_minute_is_ignored(self, client):
        client.post("/freshness", json=_push(_minute(2), _outcome()))

        repeated = client.post("/freshness", json=_push(_minute(2), _outcome(outcome="fail")))
        older = client.post("/freshness", json=_push(_minute(3), _outcome(outcome="fail")))

        for response in (repeated, older):
            assert response.status_code == 200
            assert (response.json()["applied"], response.json()["pairs"]) == (False, 0)
        pair = client.get("/freshness").json()["chains"][CHAIN]["indexers"][INDEXER]
        assert (pair["last"]["outcome"], pair["counted_28d"]) == ("pass", 1)

    def test_minute_with_an_offset_is_read_as_utc(self, client):
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
        offset = now.astimezone(timezone(timedelta(hours=2))).isoformat()

        response = client.post("/freshness", json=_push(offset, _outcome()))

        assert response.status_code == 200
        assert response.json()["minute"] == now.isoformat().replace("+00:00", "Z")

    def test_indexer_is_compared_in_lower_case(self, client):
        mixed_case = "0x" + "A1" * 20
        response = client.post("/freshness", json=_push(_minute(3), _outcome(indexer=mixed_case)))
        client.post("/freshness", json=_push(_minute(2), _outcome()))

        assert response.status_code == 200
        assert list(client.get("/freshness").json()["chains"][CHAIN]["indexers"]) == [INDEXER]

    def test_save_failure_is_a_500_and_keeps_the_minute_applied(
        self, client, freshness_file, caplog
    ):
        iisa_http_endpoints._state.settings.freshness_file_path = str(
            freshness_file / "under-a-missing-folder.json"
        )
        freshness_file.write_text("a file where a folder would have to be")

        with caplog.at_level(logging.ERROR, logger="iisa-service"):
            failed = client.post("/freshness", json=_push(_minute(), _outcome()))
        retried = client.post("/freshness", json=_push(_minute(), _outcome()))

        assert failed.status_code == 500
        assert any("Failed to apply or save" in r.getMessage() for r in caplog.records)
        assert retried.json()["applied"] is False

    def test_uninitialised_service_is_a_503(self, client):
        iisa_http_endpoints._state = IISAState()

        response = client.post("/freshness", json=_push(_minute(), _outcome()))

        assert response.status_code == 503


class TestPushValidation:
    @pytest.mark.parametrize(
        "body",
        [
            _push(_minute(), _outcome(), _outcome(outcome="fail")),
            _push(_minute(), _outcome(), _outcome(indexer=INDEXER.replace("a", "A"))),
        ],
        ids=["same-pair", "same-pair-other-case"],
    )
    def test_duplicate_pair_is_a_422(self, client, body):
        response = client.post("/freshness", json=body)

        assert response.status_code == 422
        assert "more than once" in response.text

    @pytest.mark.parametrize(
        "overrides",
        [
            {"outcome": "stale"},
            {"outcome": None},
            {"age_seconds": -0.1},
            {"age_seconds": "0.8"},
            {"age_seconds": True},
            {"indexer": "0x1234"},
            {"indexer": "not an address"},
            {"chain": ""},
            {"chain": "c" * 65},
            {"reason": "Not Snake Case"},
            {"reason": "r" * 65},
            {"detail": "d" * 301},
            {"deployment": "Q" * 65},
            {"surprise": 1},
        ],
        ids=lambda overrides: next(iter(overrides)) + "=" + repr(next(iter(overrides.values()))),
    )
    def test_malformed_outcome_is_a_422(self, client, overrides):
        response = client.post("/freshness", json=_push(_minute(), _outcome(**overrides)))

        assert response.status_code == 422, response.text

    @pytest.mark.parametrize("age", [b"NaN", b"Infinity", b"-Infinity", b"1e999"])
    def test_non_finite_age_is_a_422(self, client, age):
        body = json.dumps(_push(_minute(), _outcome(age_seconds=0.5))).replace("0.5", "AGE")
        response = client.post(
            "/freshness",
            content=body.encode().replace(b"AGE", age),
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 422, response.text

    @pytest.mark.parametrize(
        "minute",
        [
            "2026-11-10T22:31:15Z",
            "2026-11-10T22:31:00.5Z",
            "2026-11-10T22:31:00",
            "yesterday",
            "",
            1_731_277_860,
            None,
            "9999-12-31T23:59:00-23:59",
            "0001-01-01T00:00:00+23:59",
        ],
    )
    def test_malformed_minute_is_a_422(self, client, minute):
        response = client.post("/freshness", json=_push(minute, _outcome()))

        assert response.status_code == 422, response.text

    def test_minute_ahead_of_the_service_clock_is_a_422(self, client):
        response = client.post("/freshness", json=_push(_minute(-10), _outcome()))

        assert response.status_code == 422
        assert "ahead" in response.text

    @pytest.mark.parametrize(
        "body",
        [{"outcomes": []}, {"minute": _minute()}, {**_push(_minute()), "extra": True}, []],
        ids=["no-minute", "no-outcomes", "unknown-field", "not-an-object"],
    )
    def test_malformed_push_is_a_422(self, client, body):
        response = client.post("/freshness", json=body)

        assert response.status_code == 422, response.text


class TestRead:
    def test_reports_nothing_before_the_first_push(self, client):
        response = client.get("/freshness")

        assert response.status_code == 200
        assert response.json() == {"last_minute": None, "chains": {}}

    def test_reflects_a_push(self, client):
        minute = _minute()
        client.post(
            "/freshness",
            json=_push(
                minute,
                _outcome(),
                _outcome(
                    indexer=OTHER,
                    outcome="fail",
                    reason="stale",
                    deployment="QmOther",
                    age_seconds=1778.9,
                ),
            ),
        )

        body = client.get("/freshness").json()

        assert body["last_minute"] == minute
        assert body["chains"][CHAIN]["indexers"][OTHER] == {
            "marked": True,
            "judged": False,
            "window_passed": 0,
            "window_counted": 1,
            "last": {
                "minute": minute,
                "outcome": "fail",
                "reason": "stale",
                "detail": None,
                "deployment": "QmOther",
                "age_seconds": 1778.9,
            },
            "passed_28d": 0,
            "counted_28d": 1,
            "share_28d": 0.0,
        }
        assert body["chains"][CHAIN]["indexers"][INDEXER]["window_passed"] == 1

    def test_mark_clears_after_5_passing_minutes(self, client):
        for minutes_ago in range(7, 2, -1):
            client.post("/freshness", json=_push(_minute(minutes_ago), _outcome()))

        pair = client.get("/freshness").json()["chains"][CHAIN]["indexers"][INDEXER]
        assert (pair["marked"], pair["judged"], pair["share_28d"]) == (False, True, 1.0)


class TestToken:
    def test_push_without_the_token_is_a_401(self, client, token):
        response = client.post("/freshness", json=_push(_minute(), _outcome()))

        assert response.status_code == 401

    def test_push_with_a_wrong_token_is_a_401(self, client, token):
        response = client.post(
            "/freshness",
            json=_push(_minute(), _outcome()),
            headers={"Authorization": "Bearer wrong"},
        )

        assert response.status_code == 401

    def test_read_without_the_token_is_a_401(self, client, token):
        assert client.get("/freshness").status_code == 401

    def test_push_and_read_with_the_token(self, client, token):
        pushed = client.post("/freshness", json=_push(_minute(), _outcome()), headers=AUTH)
        read = client.get("/freshness", headers=AUTH)

        assert pushed.status_code == 200
        assert INDEXER in read.json()["chains"][CHAIN]["indexers"]


class TestStartup:
    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_startup_loads_the_saved_state(
        self, mock_loader_class, mock_dm_class, client, freshness_file
    ):
        minute = _minute()
        client.post("/freshness", json=_push(minute, _outcome()))
        iisa_http_endpoints._state = IISAState()
        mock_dm_class.return_value = MagicMock(**{"load_scores.return_value": False})

        with TestClient(app) as restarted:
            body = restarted.get("/freshness").json()

        assert body["last_minute"] == minute
        assert INDEXER in body["chains"][CHAIN]["indexers"]

    @patch("iisa.iisa_http_endpoints.DataManager")
    @patch("iisa.iisa_http_endpoints.FileScoreLoader")
    def test_startup_survives_a_corrupt_file(
        self, mock_loader_class, mock_dm_class, freshness_file, caplog
    ):
        freshness_file.write_text("{ not json")
        iisa_http_endpoints._state = IISAState()
        mock_dm_class.return_value = MagicMock(**{"load_scores.return_value": False})

        with caplog.at_level(logging.WARNING):
            with TestClient(app) as restarted:
                body = restarted.get("/freshness").json()

        assert body == {"last_minute": None, "chains": {}}
        assert any("malformed" in r.getMessage() for r in caplog.records)
