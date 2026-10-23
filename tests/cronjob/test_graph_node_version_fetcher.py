"""
Tests for `_fetch_single_graph_node_version_async`, the per-indexer
fetcher that POSTs a GraphQL `version` query to <url>/status.

These tests stub aiohttp's session.post via a small inline fake so the
fetcher's success / failure / retry handling can be exercised without
running an HTTP server.
"""

import asyncio
import json as json_module
import sys
from pathlib import Path

import aiohttp
import pytest

jobs_path = Path(__file__).parents[2] / "cronjobs" / "compute_scores"
sys.path.insert(0, str(jobs_path))

from processing import _fetch_single_graph_node_version_async  # noqa: E402


class _FakeContent:
    """Mimics `aiohttp.ClientResponse.content.read(n)` for size-capped reads."""

    def __init__(self, body_bytes: bytes):
        self._body = body_bytes

    async def read(self, n: int) -> bytes:
        return self._body[:n]


class _FakeRequestInfo:
    """Satisfies `aiohttp.ClientResponseError.__str__`, which dereferences
    `request_info.real_url` unconditionally when the error is rendered."""

    def __init__(self, url: str = "http://i.example/status"):
        self.real_url = url


class _FakeResponse:
    def __init__(self, body, status=200, oversize_bytes: int = 0):
        # body is a Python dict — encode once and store. oversize_bytes lets a
        # test simulate a response that exceeds the configured cap so the
        # overflow guard can be exercised.
        encoded = json_module.dumps(body).encode("utf-8")
        if oversize_bytes:
            encoded = encoded + b"x" * oversize_bytes
        self.content = _FakeContent(encoded)
        self.status = status
        self.request_info = _FakeRequestInfo()
        self.history = ()

    def raise_for_status(self):
        if self.status >= 400 and self.status < 500:
            raise aiohttp.ClientResponseError(
                self.request_info,
                self.history,
                status=self.status,
                message="client error",
            )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _RaisingPost:
    """A drop-in for a scripted `.post(...)` return that raises on entry,
    used to simulate transient network failures in the retry loop."""

    def __init__(self, exc: Exception):
        self._exc = exc

    async def __aenter__(self):
        raise self._exc

    async def __aexit__(self, *a):
        return False


class _HtmlResponse:
    """Stub that returns raw non-JSON bytes from `content.read`. Used to
    cover the malformed-body branch: aiohttp itself is happy with the
    response (2xx, valid framing), only `json.loads` rejects the body."""

    def __init__(self, body_bytes: bytes = b"<html>Bad Gateway</html>", status: int = 200):
        self.content = _FakeContent(body_bytes)
        self.status = status
        self.request_info = _FakeRequestInfo()
        self.history = ()

    def raise_for_status(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    """Stub aiohttp session: returns scripted responses for each .post call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json})
        return self._responses.pop(0)


def _run(coro):
    # `asyncio.run` creates and tears down a fresh loop per call, which is
    # the recommended pattern on Python 3.10+. The older
    # `asyncio.get_event_loop().run_until_complete(...)` emits a
    # DeprecationWarning and will fail outright on 3.14+.
    return asyncio.run(coro)


@pytest.fixture
def semaphore():
    return asyncio.Semaphore(10)


def test_fetch_parses_version_and_commit(semaphore):
    # Arrange — happy path: graph-node returns the standard shape.
    session = _FakeSession(
        [_FakeResponse({"data": {"version": {"version": "0.40.1", "commit": "abc123"}}})]
    )

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xaaa", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — record carries indexer ID plus both fields verbatim.
    assert result == {
        "indexer": "0xaaa",
        "graph_node_version": "0.40.1",
        "graph_node_commit": "abc123",
    }
    # And the URL was assembled correctly with /status appended.
    assert session.calls[0]["url"] == "http://i.example/status"
    assert session.calls[0]["json"] == {"query": "{ version { version commit } }"}


def test_fetch_handles_missing_version_field(semaphore):
    # Arrange — indexer answered with an empty `data` envelope, e.g.
    # a graph-node build that doesn't have the version query enabled.
    session = _FakeSession([_FakeResponse({"data": {}})])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xbbb", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — fields fall through to None; the filter decides the fate.
    assert result == {
        "indexer": "0xbbb",
        "graph_node_version": None,
        "graph_node_commit": None,
    }


def test_fetch_handles_null_data_envelope(semaphore):
    # Arrange — a GraphQL error response where `data` is null.
    session = _FakeSession([_FakeResponse({"errors": [{"message": "boom"}]})])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xccc", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — no crash, both fields None.
    assert result["graph_node_version"] is None
    assert result["graph_node_commit"] is None


def test_fetch_trims_trailing_slash_from_url(semaphore):
    # Arrange — indexer URLs in the network subgraph occasionally carry
    # a trailing slash; the joiner must not produce a doubled slash.
    session = _FakeSession(
        [_FakeResponse({"data": {"version": {"version": "0.40.0", "commit": "x"}}})]
    )

    # Act
    _run(
        _fetch_single_graph_node_version_async(
            session,
            indexer="0xddd",
            url="http://i.example/",
            semaphore=semaphore,
        )
    )

    # Assert — single slash before /status.
    assert session.calls[0]["url"] == "http://i.example/status"


def test_fetch_retries_then_succeeds(semaphore, monkeypatch):
    # Arrange — first attempt raises a transient client error, second
    # attempt succeeds. Avoid the real backoff sleep so the test stays fast.
    async def _no_sleep(_):
        return None

    monkeypatch.setattr("processing.asyncio.sleep", _no_sleep)

    session = _FakeSession(
        [
            _RaisingPost(
                aiohttp.ClientConnectorError(
                    connection_key=None,
                    os_error=OSError("transient"),
                )
            ),
            _FakeResponse({"data": {"version": {"version": "0.40.0", "commit": "abc"}}}),
        ]
    )

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xeee", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — retry recovered, version was parsed, two calls were made.
    assert result["graph_node_version"] == "0.40.0"
    assert len(session.calls) == 2


def test_fetch_exhausts_retries_returns_unknown(semaphore, monkeypatch):
    # Arrange — every attempt fails with a transient error. The fetcher
    # should consume all retries (defaulted to 3) and return the all-None
    # record so the indexer surfaces as "unknown" in the version column.
    async def _no_sleep(_):
        return None

    monkeypatch.setattr("processing.asyncio.sleep", _no_sleep)

    session = _FakeSession([_RaisingPost(asyncio.TimeoutError()) for _ in range(3)])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xfff", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — record is all-None and all three attempts were made.
    assert result == {
        "indexer": "0xfff",
        "graph_node_version": None,
        "graph_node_commit": None,
    }
    assert len(session.calls) == 3


def test_fetch_4xx_short_circuits_without_retrying(semaphore, monkeypatch):
    # Arrange — an indexer-service that doesn't implement /status returns
    # 404. Retrying that won't change the outcome, so the fetcher should
    # break the loop and stop after the first attempt.
    sleep_calls = []

    async def _track_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr("processing.asyncio.sleep", _track_sleep)

    session = _FakeSession(
        [
            _FakeResponse({"errors": [{"message": "Not Found"}]}, status=404),
        ]
    )

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0x000", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — single attempt, no sleep, all-None record.
    assert result["graph_node_version"] is None
    assert len(session.calls) == 1
    assert sleep_calls == []


def test_fetch_oversized_response_returns_unknown(semaphore, monkeypatch):
    # Arrange — a misconfigured indexer streams a body that exceeds the
    # response cap. The fetcher must treat that as a failure and not
    # buffer the rest of the payload.
    async def _no_sleep(_):
        return None

    monkeypatch.setattr("processing.asyncio.sleep", _no_sleep)

    # Pad the body well past the cap (64 KiB default).
    session = _FakeSession(
        [
            _FakeResponse(
                {"data": {"version": {"version": "0.40.0", "commit": "x"}}},
                oversize_bytes=200_000,
            ),
            _FakeResponse(
                {"data": {"version": {"version": "0.40.0", "commit": "x"}}},
                oversize_bytes=200_000,
            ),
            _FakeResponse(
                {"data": {"version": {"version": "0.40.0", "commit": "x"}}},
                oversize_bytes=200_000,
            ),
        ]
    )

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xbig", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — overflow triggered the failure path, all retries consumed.
    assert result["graph_node_version"] is None


def test_fetch_5xx_retries_then_succeeds(semaphore, monkeypatch):
    # Arrange — a 500 response is transient by assumption, so the fetcher
    # should NOT short-circuit the way it does for 4xx. The second attempt
    # returns a valid version and the call succeeds.
    async def _no_sleep(_):
        return None

    monkeypatch.setattr("processing.asyncio.sleep", _no_sleep)

    session = _FakeSession(
        [
            _FakeResponse({"errors": [{"message": "boom"}]}, status=500),
            _FakeResponse({"data": {"version": {"version": "0.40.0", "commit": "abc"}}}),
        ]
    )

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0x500", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — 500 was retried, second attempt parsed cleanly.
    assert result["graph_node_version"] == "0.40.0"
    assert len(session.calls) == 2


def test_fetch_handles_malformed_json_body(semaphore, monkeypatch):
    # Arrange — an upstream proxy returns a 200 with an HTML error page in
    # the body. `json.loads` raises JSONDecodeError (a ValueError); without
    # the broadened catch tuple it would escape the per-task coroutine,
    # propagate through `asyncio.gather`, and crash the whole batch.
    async def _no_sleep(_):
        return None

    monkeypatch.setattr("processing.asyncio.sleep", _no_sleep)

    session = _FakeSession([_HtmlResponse(), _HtmlResponse(), _HtmlResponse()])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xhtml", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — JSONDecodeError was caught and treated like any transient
    # failure: three attempts, all-None record returned.
    assert result == {
        "indexer": "0xhtml",
        "graph_node_version": None,
        "graph_node_commit": None,
    }
    assert len(session.calls) == 3


def test_fetch_handles_invalid_utf8_body(semaphore, monkeypatch):
    # Arrange — `json.loads` decodes UTF-8 internally. Raw bytes that don't
    # form valid UTF-8 raise UnicodeDecodeError (a ValueError subclass).
    async def _no_sleep(_):
        return None

    monkeypatch.setattr("processing.asyncio.sleep", _no_sleep)

    # Provide one bad-bytes response per retry attempt; the decode error
    # is transient-like (could in principle be a partial read), so the
    # fetcher exhausts the retry budget before giving up.
    session = _FakeSession([_HtmlResponse(body_bytes=b"\xc3\x28\xff\xfe") for _ in range(3)])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xutf8", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — the decode error was caught; the record falls through to
    # all-None and the cron run continues.
    assert result["graph_node_version"] is None
    assert len(session.calls) == 3


def test_fetch_handles_non_dict_version_field(semaphore):
    # Arrange — a graph-node fork returns `data.version` as a bare string
    # instead of the expected `{version, commit}` object. The isinstance
    # guards must catch the shape mismatch and fall through without
    # raising AttributeError on `.get()`.
    session = _FakeSession([_FakeResponse({"data": {"version": "0.40.0"}})])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xmisshape", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — single attempt because the failure is deterministic; record
    # is all-None.
    assert result == {
        "indexer": "0xmisshape",
        "graph_node_version": None,
        "graph_node_commit": None,
    }
    assert len(session.calls) == 1


def test_fetch_handles_top_level_array_body(semaphore):
    # Arrange — a misbehaving proxy returns a JSON array as the top-level
    # body. `data.get(...)` would raise AttributeError without the
    # isinstance guard.
    session = _FakeSession([_FakeResponse([{"data": {"version": {"version": "0.40.0"}}}])])

    # Act
    result = _run(
        _fetch_single_graph_node_version_async(
            session, indexer="0xarray", url="http://i.example", semaphore=semaphore
        )
    )

    # Assert — shape mismatch handled cleanly, all-None record.
    assert result["graph_node_version"] is None
    assert result["graph_node_commit"] is None
