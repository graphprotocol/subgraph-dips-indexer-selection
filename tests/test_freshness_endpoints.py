"""Tests for POST /freshness and GET /freshness."""

import json
import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from iisa import iisa_http_endpoints
from iisa.iisa_http_endpoints import IISAState, Settings, _log_safe, app

INDEXER = "0x" + "a1" * 20
OTHER = "0x" + "b2" * 20
CHAIN = "arbitrum-one"
OTHER_CHAIN = "matic"
TOKEN = "fake-freshness-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def _now_minute() -> datetime:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0)


def _minute(minutes_ago: int = 2, now: Optional[datetime] = None) -> str:
    """The minute ``minutes_ago`` before ``now``. A test that needs 2 minutes in a fixed order
    reads the clock once and passes it as ``now``, since the clock may pass a minute between.
    """
    moment = (now or _now_minute()) - timedelta(minutes=minutes_ago)
    return moment.isoformat().replace("+00:00", "Z")


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
            "refused": 0,
        }
        assert json.loads(freshness_file.read_text())["version"] == 1

    def test_empty_push_is_applied(self, client):
        response = client.post("/freshness", json=_push(_minute()))

        assert response.status_code == 200
        assert (response.json()["applied"], response.json()["pairs"]) == (True, 0)

    def test_repeated_or_older_minute_is_ignored(self, client):
        now = _now_minute()
        client.post("/freshness", json=_push(_minute(2, now), _outcome()))

        repeated = client.post("/freshness", json=_push(_minute(2, now), _outcome(outcome="fail")))
        older = client.post("/freshness", json=_push(_minute(3, now), _outcome(outcome="fail")))

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
        now = _now_minute()
        response = client.post(
            "/freshness", json=_push(_minute(3, now), _outcome(indexer=mixed_case))
        )
        client.post("/freshness", json=_push(_minute(2, now), _outcome()))

        assert response.status_code == 200
        assert list(client.get("/freshness").json()["chains"][CHAIN]["indexers"]) == [INDEXER]

    def test_save_failure_is_a_500_and_keeps_the_minute_applied(
        self, client, freshness_file, caplog
    ):
        iisa_http_endpoints._state.settings.freshness_file_path = str(
            freshness_file / "under-a-missing-folder.json"
        )
        freshness_file.write_text("a file where a folder would have to be")

        minute = _minute()
        with caplog.at_level(logging.ERROR, logger="iisa-service"):
            failed = client.post("/freshness", json=_push(minute, _outcome()))
        retried = client.post("/freshness", json=_push(minute, _outcome()))

        assert failed.status_code == 500
        assert any("Failed to apply or save" in r.getMessage() for r in caplog.records)
        assert retried.json()["applied"] is False

    def test_uninitialised_service_is_a_503(self, client):
        iisa_http_endpoints._state = IISAState()

        response = client.post("/freshness", json=_push(_minute(), _outcome()))

        assert response.status_code == 503


def _post_raw(client: TestClient, body: dict[str, Any], placeholder: str, raw: bytes) -> Any:
    """Post ``body`` as JSON with ``placeholder`` swapped for ``raw``, for values json.dumps
    won't write, such as NaN or half of a UTF-16 surrogate pair.
    """
    content = json.dumps(body).encode().replace(placeholder.encode(), raw)
    return client.post("/freshness", content=content, headers={"Content-Type": "application/json"})


def _refused_alone(response: Any, client: TestClient) -> None:
    """The push was answered 200 with OTHER's good outcome applied and the bad 1 refused."""
    assert response.status_code == 200, response.text
    assert (response.json()["pairs"], response.json()["refused"]) == (1, 1)
    assert list(client.get("/freshness").json()["chains"][CHAIN]["indexers"]) == [OTHER]


class TestRefusedOutcomes:
    """Each outcome is checked on its own. 1 that breaks the rules is left out and counted in
    refused, and the rest of the minute still counts.
    """

    def test_bad_outcome_among_good_ones_is_refused_and_the_rest_applied(self, client):
        minute = _minute()
        response = client.post(
            "/freshness",
            json=_push(
                minute,
                _outcome(),
                _outcome(indexer=OTHER, outcome="stale"),
                _outcome(chain=OTHER_CHAIN),
            ),
        )

        assert response.status_code == 200, response.text
        assert response.json() == {
            "status": "success",
            "minute": minute,
            "applied": True,
            "pairs": 2,
            "refused": 1,
        }
        chains = client.get("/freshness").json()["chains"]
        assert {chain: list(body["indexers"]) for chain, body in chains.items()} == {
            CHAIN: [INDEXER],
            OTHER_CHAIN: [INDEXER],
        }

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
            {"indexer": None},
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
    def test_malformed_outcome_is_refused_on_its_own(self, client, overrides):
        response = client.post(
            "/freshness",
            json=_push(_minute(), _outcome(**overrides), _outcome(indexer=OTHER)),
        )

        _refused_alone(response, client)

    def test_outcome_missing_a_required_field_is_refused(self, client):
        bare = {key: value for key, value in _outcome().items() if key != "outcome"}

        response = client.post("/freshness", json=_push(_minute(), bare, _outcome(indexer=OTHER)))

        _refused_alone(response, client)

    @pytest.mark.parametrize("item", [7, "pass", None, True, [], [_outcome()]], ids=repr)
    def test_item_that_is_not_an_object_is_refused(self, client, item):
        response = client.post("/freshness", json=_push(_minute(), item, _outcome(indexer=OTHER)))

        _refused_alone(response, client)

    @pytest.mark.parametrize("age", [b"NaN", b"Infinity", b"-Infinity", b"1e999"])
    def test_non_finite_age_is_refused(self, client, age):
        body = _push(_minute(), _outcome(age_seconds=0.5), _outcome(indexer=OTHER))

        _refused_alone(_post_raw(client, body, "0.5", age), client)

    @pytest.mark.parametrize("field", ["chain", "detail", "deployment"])
    def test_half_a_surrogate_pair_is_refused_and_not_stored(self, client, field):
        """Stored, such text could never be written out as UTF-8, and every later GET
        /freshness would fail.
        """
        body = _push(_minute(), _outcome(**{field: "x-HALF"}), _outcome(indexer=OTHER))

        _refused_alone(_post_raw(client, body, "-HALF", b"\\ud800"), client)

    def test_repeated_pair_keeps_the_first_and_refuses_the_rest(self, client):
        response = client.post(
            "/freshness",
            json=_push(
                _minute(),
                _outcome(),
                _outcome(outcome="fail"),
                _outcome(indexer=INDEXER.upper().replace("0X", "0x"), outcome="fail"),
            ),
        )

        assert (response.json()["pairs"], response.json()["refused"]) == (1, 2)
        pair = client.get("/freshness").json()["chains"][CHAIN]["indexers"][INDEXER]
        assert (pair["last"]["outcome"], pair["counted_28d"]) == ("pass", 1)

    def test_repeat_of_a_refused_outcome_is_applied(self, client):
        response = client.post(
            "/freshness",
            json=_push(_minute(), _outcome(age_seconds=-1), _outcome(outcome="fail")),
        )

        assert (response.json()["pairs"], response.json()["refused"]) == (1, 1)
        pair = client.get("/freshness").json()["chains"][CHAIN]["indexers"][INDEXER]
        assert pair["last"]["outcome"] == "fail"

    def test_every_outcome_refused_still_counts_the_minute_as_received(self, client):
        """OTHER's first outcome comes a minute after the refused push. Its window counting 2
        minutes, the refused one as not passed, shows that minute was received.
        """
        now = _now_minute()
        refused = client.post(
            "/freshness",
            json=_push(_minute(3, now), _outcome(outcome="stale"), _outcome(indexer="0x12")),
        )
        client.post("/freshness", json=_push(_minute(2, now), _outcome(indexer=OTHER)))

        assert refused.status_code == 200, refused.text
        assert refused.json() == {
            "status": "success",
            "minute": _minute(3, now),
            "applied": True,
            "pairs": 0,
            "refused": 2,
        }
        pair = client.get("/freshness").json()["chains"][CHAIN]["indexers"][OTHER]
        assert (pair["window_passed"], pair["window_counted"]) == (1, 2)

    def test_ignored_minute_still_reports_what_was_refused(self, client):
        now = _now_minute()
        client.post("/freshness", json=_push(_minute(2, now), _outcome()))

        repeated = client.post(
            "/freshness",
            json=_push(_minute(2, now), _outcome(), _outcome(outcome="stale")),
        )

        assert repeated.status_code == 200
        assert (repeated.json()["applied"], repeated.json()["pairs"]) == (False, 0)
        assert repeated.json()["refused"] == 1


class TestRefusalWarning:
    @staticmethod
    def _warnings(caplog: Any) -> list[logging.LogRecord]:
        return [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_1_warning_names_the_minute_count_and_first_refusal(self, client, caplog):
        minute = _minute()
        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            client.post(
                "/freshness",
                json=_push(
                    minute,
                    _outcome(indexer=OTHER),
                    _outcome(age_seconds=-1),
                    _outcome(outcome="stale"),
                    _outcome(indexer=OTHER),
                ),
            )

        (warning,) = self._warnings(caplog)
        message = warning.getMessage()
        assert minute in message
        assert "3 of 4" in message
        assert "outcomes[1]" in message and "age_seconds" in message

    def test_repeat_is_named_with_the_outcome_it_repeats(self, client, caplog):
        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            client.post("/freshness", json=_push(_minute(), _outcome(), _outcome()))

        (warning,) = self._warnings(caplog)
        assert "outcomes[1] repeats the indexer and chain of outcomes[0]" in warning.getMessage()

    @pytest.mark.parametrize(
        "name",
        ["sur\nprise", "sur\r\nprise", "sur\u2028prise", "x" * 10_000],
        ids=["newline", "crlf", "line-separator", "long"],
    )
    def test_text_from_the_request_stays_on_1_short_line(self, client, caplog, name):
        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            client.post("/freshness", json=_push(_minute(), {**_outcome(), name: 1}))

        (warning,) = self._warnings(caplog)
        message = warning.getMessage()
        assert len(message.splitlines()) == 1
        assert len(message) < 400

    def test_half_a_surrogate_pair_in_a_field_name_can_be_written_out(self, client, caplog):
        body = _push(_minute(), {**_outcome(), "x-HALF": 1})

        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            _post_raw(client, body, "-HALF", b"\\ud800")

        (warning,) = self._warnings(caplog)
        warning.getMessage().encode("utf-8")

    def test_request_text_is_escaped_and_cut(self):
        assert (
            _log_safe("a\nb\r\t\x1b\u2028\ud800\xe9\\") == "a\\nb\\r\\t\\x1b\\u2028\\ud800\\xe9\\\\"
        )
        assert _log_safe("x" * 201) == "x" * 200 + "..."
        assert _log_safe("x" * 200) == "x" * 200

    def test_no_warning_when_nothing_is_refused(self, client, caplog):
        with caplog.at_level(logging.WARNING, logger="iisa-service"):
            client.post("/freshness", json=_push(_minute(), _outcome()))

        assert self._warnings(caplog) == []


class TestPushValidation:
    """What makes the whole push a 422: anything wrong outside the outcomes themselves."""

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
        [
            {"outcomes": []},
            {"minute": _minute()},
            {**_push(_minute()), "extra": True},
            [],
            "text",
            {"minute": _minute(), "outcomes": {"0": _outcome()}},
            {"minute": _minute(), "outcomes": "outcomes"},
            {"minute": _minute(), "outcomes": None},
        ],
        ids=[
            "no-minute",
            "no-outcomes",
            "unknown-field",
            "not-an-object",
            "text",
            "outcomes-an-object",
            "outcomes-text",
            "outcomes-null",
        ],
    )
    def test_malformed_push_is_a_422(self, client, body):
        response = client.post("/freshness", json=body)

        assert response.status_code == 422, response.text
        assert client.get("/freshness").json()["last_minute"] is None

    def test_more_than_50_000_outcomes_is_a_422(self, client):
        response = client.post("/freshness", json={"minute": _minute(), "outcomes": [0] * 50_001})

        assert response.status_code == 422
        assert "too_long" in response.text
        assert client.get("/freshness").json()["last_minute"] is None

    def test_half_a_surrogate_pair_in_a_top_level_field_name_is_a_422(self, client):
        body = {**_push(_minute()), "x-HALF": 1}

        response = _post_raw(client, body, "-HALF", b"\\ud800")

        assert response.status_code == 422, response.text


class TestSchema:
    def test_documents_an_outcome_while_accepting_any_item(self):
        """Any item is accepted and only refused later, so the schema says so, while still
        describing the outcome that gets applied.
        """
        schemas = app.openapi()["components"]["schemas"]
        outcomes = schemas["FreshnessPush"]["properties"]["outcomes"]

        assert (outcomes["type"], outcomes["maxItems"]) == ("array", 50_000)
        outcome, anything = outcomes["items"]["anyOf"]
        assert anything == {}
        assert set(outcome["properties"]) == {
            "indexer",
            "chain",
            "outcome",
            "reason",
            "detail",
            "deployment",
            "age_seconds",
        }
        assert (outcome["required"], outcome["additionalProperties"]) == (
            ["indexer", "chain", "outcome"],
            False,
        )

    def test_response_lists_the_refused_count(self):
        response = app.openapi()["components"]["schemas"]["FreshnessAcceptedResponse"]

        assert response["properties"]["refused"]["type"] == "integer"
        assert "refused" in response["required"]


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
            "last_counted_minute": minute,
            "passed_28d": 0,
            "counted_28d": 1,
            "share_28d": 0.0,
        }
        assert body["chains"][CHAIN]["indexers"][INDEXER]["window_passed"] == 1

    def test_mark_clears_after_5_passing_minutes(self, client):
        now = _now_minute()
        for minutes_ago in range(7, 2, -1):
            client.post("/freshness", json=_push(_minute(minutes_ago, now), _outcome()))

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
