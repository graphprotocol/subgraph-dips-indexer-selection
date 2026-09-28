"""Tests for the retry loop behind every push to the IISA service.

The loop must return on a 2xx, give up at once on a 4xx that is our fault, keep trying
through 429s, 5xx responses and transport errors, and stop early on request errors that a
retry cannot fix.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

jobs_path = Path(__file__).parent.parent / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from iisa_client import RETRY_ATTEMPTS, IISAPushError, _request_with_retry  # noqa: E402

URL = "http://iisa:8080/scores"


def _response(status: int, text: str = "") -> MagicMock:
    response = MagicMock(spec=requests.Response)
    response.status_code = status
    response.text = text
    return response


class TestRequestWithRetry:
    def test_returns_response_on_first_2xx(self):
        ok = _response(200)
        with (
            patch("iisa_client.requests.request", return_value=ok) as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            result = _request_with_retry("POST", URL, token="t", json_body=[])

        assert result is ok
        assert mock_request.call_count == 1
        mock_sleep.assert_not_called()
        headers = mock_request.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer t"
        assert headers["Content-Type"] == "application/json"

    def test_4xx_raises_without_retrying(self):
        with (
            patch("iisa_client.requests.request", return_value=_response(401, "bad token")),
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            with pytest.raises(IISAPushError) as exc_info:
                _request_with_retry("POST", URL, token="t", json_body=[])

        assert "401" in str(exc_info.value)
        assert "bad token" in str(exc_info.value)
        mock_sleep.assert_not_called()

    def test_429_is_retried_until_it_succeeds(self):
        ok = _response(200)
        with (
            patch(
                "iisa_client.requests.request",
                side_effect=[_response(429, "slow down"), ok],
            ) as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            result = _request_with_retry("POST", URL, token="t", json_body=[])

        assert result is ok
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1

    def test_5xx_is_retried_until_it_succeeds(self):
        ok = _response(200)
        with (
            patch(
                "iisa_client.requests.request",
                side_effect=[_response(503), _response(502), ok],
            ) as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            result = _request_with_retry("GET", URL, token=None)

        assert result is ok
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2

    def test_transport_errors_exhaust_the_retry_budget(self):
        with (
            patch(
                "iisa_client.requests.request",
                side_effect=requests.ConnectionError("refused"),
            ) as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            with pytest.raises(IISAPushError) as exc_info:
                _request_with_retry("POST", URL, token="t", json_body=[])

        assert mock_request.call_count == RETRY_ATTEMPTS
        assert mock_sleep.call_count == RETRY_ATTEMPTS - 1
        assert f"{RETRY_ATTEMPTS} attempts" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, requests.ConnectionError)

    def test_unfixable_request_error_stops_after_one_attempt(self):
        with (
            patch(
                "iisa_client.requests.request",
                side_effect=requests.exceptions.InvalidURL("bad scheme"),
            ) as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            with pytest.raises(IISAPushError) as exc_info:
                _request_with_retry("POST", URL, token="t", json_body=[])

        assert mock_request.call_count == 1
        mock_sleep.assert_not_called()
        assert isinstance(exc_info.value.__cause__, requests.exceptions.InvalidURL)

    def test_exhaustion_error_reports_the_last_http_status(self):
        with (
            patch("iisa_client.requests.request", return_value=_response(503, "down")),
            patch("iisa_client.time.sleep"),
        ):
            with pytest.raises(IISAPushError) as exc_info:
                _request_with_retry("POST", URL, token="t", json_body=[])

        assert "last_status=503" in str(exc_info.value)

    def test_http_error_raised_by_the_transport_stops_after_one_attempt(self):
        """An HTTPError thrown by requests itself is not one of ours and must not be retried."""
        with (
            patch(
                "iisa_client.requests.request",
                side_effect=requests.HTTPError("boom"),
            ) as mock_request,
            patch("iisa_client.time.sleep") as mock_sleep,
        ):
            with pytest.raises(IISAPushError) as exc_info:
                _request_with_retry("POST", URL, token="t", json_body=[])

        assert mock_request.call_count == 1
        mock_sleep.assert_not_called()
        assert "last_status=None" in str(exc_info.value)
