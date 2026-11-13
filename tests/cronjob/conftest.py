"""Fixtures shared by the scoring job's tests."""

import pytest


@pytest.fixture
def local_indexers_allowed(monkeypatch):
    """Let a test call a server on this machine as if it were an indexer.

    IISA refuses indexers at addresses that are not public, which a test server's loopback
    address is. A local network lifts the rule the same way.
    """
    monkeypatch.setenv("INDEXER_URLS_ALLOW_PRIVATE_ADDRESSES", "true")
