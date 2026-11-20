"""Copying the cache files to a bucket and restoring them from it."""

import http.client
import json
import logging
import time
import urllib.error
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from iisa import backup, score_loader
from iisa import iisa_http_endpoints as endpoints
from iisa.backup import Backup, BackupError, BackupLocation, GcsBucket
from iisa.freshness import Check, FreshnessStore, minute_index, minute_text
from iisa.iisa_http_endpoints import IISAState, Settings, app

CHAIN = "arbitrum-one"
INDEXER = "0x" + "a1" * 20
MINUTE = minute_index(datetime(2026, 11, 20, 9, tzinfo=timezone.utc))
TOKEN_ANSWER = json.dumps({"access_token": "tok", "expires_in": 3600}).encode()
REFUSED = BackupError("GET returned 403", retryable=False)
UNAVAILABLE = BackupError("GET returned 503", retryable=True)


class FakeBucket:
    """A bucket in memory that raises the queued failures, 1 per call, before it answers."""

    def __init__(self, objects: Optional[dict[str, bytes]] = None) -> None:
        self.objects = dict(objects or {})
        self.failures: list[BackupError] = []
        self.gets: list[str] = []
        self.puts: list[str] = []

    def get(self, name: str) -> Optional[bytes]:
        self.gets.append(name)
        if self.failures:
            raise self.failures.pop(0)
        return self.objects.get(name)

    def put(self, name: str, data: bytes) -> None:
        self.puts.append(name)
        if self.failures:
            raise self.failures.pop(0)
        self.objects[name] = data


class Clock:
    """A monotonic clock that only moves when slept on."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class Answer:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "Answer":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class Opener:
    """Stands in for urlopen: records each request and gives the next queued answer."""

    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.requests: list[Any] = []

    def __call__(self, request: Any, timeout: float) -> Answer:
        self.requests.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return Answer(answer)


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://storage.googleapis.com", code, "no", None, None)  # type: ignore[arg-type]


def _backup(bucket: FakeBucket, clock: Optional[Clock] = None) -> Backup:
    clock = clock or Clock()
    location = BackupLocation.parse("gs://iisa-test/testnet")
    return Backup(location, bucket, sleep=clock.sleep, clock=clock)


class TestLocation:
    @pytest.mark.parametrize(
        ("url", "bucket", "prefix"),
        [
            ("gs://iisa-test", "iisa-test", ""),
            ("gs://iisa-test/", "iisa-test", ""),
            ("gs://iisa-test/testnet-v2", "iisa-test", "testnet-v2/"),
            ("gs://iisa-test/a/b/", "iisa-test", "a/b/"),
        ],
    )
    def test_reads_a_bucket_and_a_path(self, url, bucket, prefix):
        location = BackupLocation.parse(url)

        assert (location.bucket, location.prefix) == (bucket, prefix)
        assert location.object_name("/app/scores/freshness.json") == f"{prefix}freshness.json"

    @pytest.mark.parametrize("url", ["iisa-test", "s3://iisa-test", "gs://", "gs:///x", "gs://A"])
    def test_refuses_anything_else(self, url):
        with pytest.raises(ValueError, match="gs://<bucket>"):
            BackupLocation.parse(url)

    def test_a_bad_setting_stops_startup_and_an_empty_one_is_off(self):
        with pytest.raises(ValidationError):
            Settings(backup_url="s3://iisa-test")

        assert Settings(backup_url="").backup_url is None


class TestGcsBucket:
    def test_put_sends_the_data_signed_in_as_the_pod(self):
        opener = Opener(TOKEN_ANSWER, b"{}")

        GcsBucket("iisa-test", open_url=opener).put("testnet/freshness.json", b"saved")

        token, upload = opener.requests
        assert token.get_header("Metadata-flavor") == "Google"
        assert upload.get_method() == "POST" and upload.data == b"saved"
        assert upload.full_url == (
            "https://storage.googleapis.com/upload/storage/v1/b/iisa-test/o"
            "?uploadType=media&name=testnet%2Ffreshness.json"
        )
        assert upload.get_header("Authorization") == "Bearer tok"

    def test_get_gives_the_object_and_reuses_the_token(self):
        opener = Opener(TOKEN_ANSWER, b"saved", b"again")
        bucket = GcsBucket("iisa-test", open_url=opener)

        assert bucket.get("testnet/freshness.json") == b"saved"
        assert bucket.get("testnet/freshness.json") == b"again"
        assert len(opener.requests) == 3
        assert opener.requests[1].full_url == (
            "https://storage.googleapis.com/storage/v1/b/iisa-test/o/testnet%2Ffreshness.json"
            "?alt=media"
        )

    def test_a_missing_object_is_none_but_a_missing_bucket_fails(self):
        opener = Opener(TOKEN_ANSWER, _http_error(404), _http_error(404))
        bucket = GcsBucket("iisa-test", open_url=opener)

        assert bucket.get("testnet/freshness.json") is None
        with pytest.raises(BackupError) as raised:
            bucket.put("testnet/freshness.json", b"saved")
        assert not raised.value.retryable

    @pytest.mark.parametrize(
        ("failure", "retryable"),
        [
            (_http_error(403), False),
            (_http_error(503), True),
            (_http_error(429), True),
            (urllib.error.URLError("no route"), True),
            (TimeoutError("timed out"), True),
            (http.client.IncompleteRead(b"par"), True),
            (http.client.BadStatusLine("garbled"), True),
        ],
    )
    def test_says_whether_a_failure_may_clear_up(self, failure, retryable):
        bucket = GcsBucket("iisa-test", open_url=Opener(TOKEN_ANSWER, failure))

        with pytest.raises(BackupError) as raised:
            bucket.get("testnet/freshness.json")

        assert raised.value.retryable is retryable

    def test_a_rejected_token_is_fetched_again(self):
        opener = Opener(TOKEN_ANSWER, _http_error(401), TOKEN_ANSWER, b"saved")
        bucket = GcsBucket("iisa-test", open_url=opener)

        with pytest.raises(BackupError) as raised:
            bucket.get("testnet/freshness.json")

        assert raised.value.retryable
        assert bucket.get("testnet/freshness.json") == b"saved"
        assert len(opener.requests) == 4

    @pytest.mark.parametrize(
        "failure", [urllib.error.URLError("no host"), http.client.IncompleteRead(b"{")]
    )
    def test_no_token_is_a_failure_that_may_clear_up(self, failure):
        bucket = GcsBucket("iisa-test", open_url=Opener(failure))

        with pytest.raises(BackupError, match="metadata server") as raised:
            bucket.get("testnet/freshness.json")

        assert raised.value.retryable


class TestRestore:
    def test_writes_the_copy_in_place(self, tmp_path):
        path = tmp_path / "freshness.json"
        bucket = FakeBucket({"testnet/freshness.json": b"saved"})

        assert _backup(bucket).restore(str(path))

        assert path.read_bytes() == b"saved"
        assert [p.name for p in tmp_path.iterdir()] == ["freshness.json"]

    def test_without_a_copy_writes_nothing(self, tmp_path):
        path = tmp_path / "freshness.json"

        assert not _backup(FakeBucket()).restore(str(path))

        assert not path.exists()

    def test_asks_again_while_the_bucket_may_come_back(self, tmp_path):
        bucket = FakeBucket({"testnet/freshness.json": b"saved"})
        bucket.failures = [UNAVAILABLE, UNAVAILABLE, UNAVAILABLE]
        clock = Clock()

        assert _backup(bucket, clock).restore(str(tmp_path / "freshness.json"))

        assert clock.sleeps == [1.0, 2.0, 4.0]

    def test_gives_up_once_the_wait_is_over(self, tmp_path, caplog):
        bucket = FakeBucket({"testnet/freshness.json": b"saved"})
        bucket.failures = [UNAVAILABLE] * 100
        clock = Clock()

        with caplog.at_level(logging.ERROR, logger="iisa.backup"):
            assert not _backup(bucket, clock).restore(str(tmp_path / "freshness.json"))

        assert sum(clock.sleeps) == backup.RESTORE_WAIT_SECONDS
        assert max(clock.sleeps) == backup.MAX_PAUSE_SECONDS
        assert "Could not restore" in caplog.records[-1].getMessage()

    def test_gives_up_at_once_on_a_refusal(self, tmp_path):
        bucket = FakeBucket({"testnet/freshness.json": b"saved"})
        bucket.failures = [REFUSED]
        clock = Clock()

        assert not _backup(bucket, clock).restore(str(tmp_path / "freshness.json"))

        assert clock.sleeps == []

    def test_a_held_file_is_not_copied_over_the_copy_it_could_not_read(self, tmp_path):
        path = tmp_path / "freshness.json"
        bucket = FakeBucket({"testnet/freshness.json": b"weeks of history"})
        bucket.failures = [REFUSED]
        kept = _backup(bucket)
        assert not kept.restore(str(path), hold=True)
        path.write_bytes(b"started again")

        kept.copy_if_changed(str(path))

        assert bucket.puts == [] and bucket.objects["testnet/freshness.json"] == b"weeks of history"

    def test_a_file_not_held_is_copied_after_a_failed_restore(self, tmp_path):
        path = tmp_path / "indexer_scores.json"
        bucket = FakeBucket()
        bucket.failures = [REFUSED]
        kept = _backup(bucket)
        assert not kept.restore(str(path))
        path.write_bytes(b"pushed")

        kept.copy_if_changed(str(path))

        assert bucket.objects["testnet/indexer_scores.json"] == b"pushed"

    def test_a_restored_file_is_not_copied_back(self, tmp_path):
        path = tmp_path / "freshness.json"
        bucket = FakeBucket({"testnet/freshness.json": b"saved"})
        kept = _backup(bucket)
        kept.restore(str(path))

        kept.copy_if_changed(str(path))

        assert bucket.puts == []


class TestCopy:
    def test_copies_a_file_only_when_it_changed(self, tmp_path):
        path = tmp_path / "freshness.json"
        bucket = FakeBucket()
        kept = _backup(bucket)

        path.write_bytes(b"first")
        kept.copy_if_changed(str(path))
        kept.copy_if_changed(str(path))
        path.write_bytes(b"second")
        kept.copy_if_changed(str(path))

        assert bucket.puts == ["testnet/freshness.json"] * 2
        assert bucket.objects["testnet/freshness.json"] == b"second"

    def test_a_missing_file_is_not_copied(self, tmp_path):
        bucket = FakeBucket()

        _backup(bucket).copy_if_changed(str(tmp_path / "freshness.json"))

        assert bucket.puts == []

    def test_a_failed_copy_is_logged_and_tried_again(self, tmp_path, caplog):
        path = tmp_path / "freshness.json"
        path.write_bytes(b"saved")
        bucket = FakeBucket()
        bucket.failures = [UNAVAILABLE]
        kept = _backup(bucket)

        with caplog.at_level(logging.ERROR, logger="iisa.backup"):
            kept.copy_if_changed(str(path))
        kept.copy_if_changed(str(path))

        assert "Could not copy" in caplog.records[0].getMessage()
        assert bucket.objects["testnet/freshness.json"] == b"saved"


def _scores_file() -> bytes:
    """A scores file the service loads, with 2 indexers of different operators and places."""
    records = [
        {
            "indexer": indexer,
            "url": f"https://{indexer[2:8]}.example/",
            "lat_lin_reg_coefficient": 0.012,
            "lat_coefficient_upper_bound": 0.016,
            "lat_normalized_score": 0.7,
            "uptime_score": 0.99,
            "success_rate": 0.99,
            "stake_to_fees": 100.0,
            "total_query_fees": 10.0,
            "last_known_slashable_stake": 1_000.0,
            "org": f"org-{indexer[2:4]}",
            "dst_lat": int(indexer[2:4], 16) / 4,
            "dst_lon": 4.9,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "dips_info_available": True,
            "dips_min_grt_per_30_days": f'{{"{CHAIN}": "450"}}',
            "dips_min_grt_per_billion_entities_per_30_days": "200",
            "dips_supported_networks": f'["{CHAIN}"]',
        }
        for indexer in (INDEXER, "0x" + "b2" * 20)
    ]
    return json.dumps(records).encode()


def _freshness_file() -> bytes:
    saved: list[dict[str, Any]] = []
    check = Check(indexer=INDEXER, chain=CHAIN, outcome="pass")
    FreshnessStore().apply(MINUTE, [check], saved.append)
    return json.dumps(saved[-1]).encode()


SELECTION = {"deployment_id": "QmBackup", "chain_id": CHAIN, "num_candidates": 1}


@pytest.fixture
def service(tmp_path, monkeypatch) -> Iterator[FakeBucket]:
    """A service with no tokens whose cache files sit in ``tmp_path``, backed up to a bucket in
    memory under gs://iisa-test/testnet."""
    for name in ("IISA_PUSH_TOKEN", "IISA_READ_TOKEN", "IISA_REQUIRE_PUSH_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("IISA_FRESHNESS_FILE_PATH", str(tmp_path / "freshness.json"))
    monkeypatch.setenv("IISA_SYNC_STATUS_FILE_PATH", str(tmp_path / "sync_status.json"))
    monkeypatch.setenv("IISA_BACKUP_URL", "gs://iisa-test/testnet")
    monkeypatch.setattr(score_loader, "SCORES_FILE_PATH", str(tmp_path / "indexer_scores.json"))
    bucket = FakeBucket()
    monkeypatch.setattr(backup, "GcsBucket", lambda _name: bucket)
    monkeypatch.setattr(endpoints, "_state", IISAState())
    endpoints.get_settings.cache_clear()
    yield bucket
    endpoints.get_settings.cache_clear()


def _wait_for(condition: Any, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


class TestService:
    def test_startup_restores_missing_files_from_the_bucket(self, service, tmp_path):
        service.objects = {
            "testnet/indexer_scores.json": _scores_file(),
            "testnet/freshness.json": _freshness_file(),
        }

        with TestClient(app) as client:
            selected = client.post("/select-indexers", json=SELECTION)
            freshness = client.get("/freshness").json()

        assert selected.status_code == 200
        assert freshness["last_minute"] == minute_text(MINUTE)
        assert (tmp_path / "freshness.json").read_bytes() == service.objects[
            "testnet/freshness.json"
        ]

    def test_startup_reads_the_volume_before_the_bucket(self, service, tmp_path):
        (tmp_path / "indexer_scores.json").write_bytes(_scores_file())
        (tmp_path / "freshness.json").write_bytes(_freshness_file())

        with TestClient(app) as client:
            assert client.post("/select-indexers", json=SELECTION).status_code == 200

        assert service.gets == []

    def test_startup_copies_what_the_volume_holds(self, service, tmp_path):
        (tmp_path / "indexer_scores.json").write_bytes(_scores_file())
        (tmp_path / "freshness.json").write_bytes(_freshness_file())

        with TestClient(app):
            copied = _wait_for(lambda: len(service.objects) == 2)

        assert copied, service.puts
        assert service.objects["testnet/indexer_scores.json"] == _read(
            tmp_path, "indexer_scores.json"
        )

    def test_a_push_copies_the_scores_even_after_a_failed_restore(self, service, tmp_path):
        service.failures = [REFUSED]

        with TestClient(app) as client:
            refused = client.post("/select-indexers", json=SELECTION)
            pushed = client.post("/scores", json=json.loads(_scores_file()))

        assert refused.status_code == 503 and pushed.status_code == 200
        assert service.objects["testnet/indexer_scores.json"] == _read(
            tmp_path, "indexer_scores.json"
        )

    def test_a_push_succeeds_when_the_bucket_cannot_be_written(self, service, caplog):
        with TestClient(app) as client:
            _wait_for(lambda: len(service.gets) == 2)
            service.failures = [REFUSED] * 10
            with caplog.at_level(logging.ERROR, logger="iisa.backup"):
                pushed = client.post("/scores", json=json.loads(_scores_file()))

        assert pushed.status_code == 200
        assert any("Could not copy" in r.getMessage() for r in caplog.records)


def _read(tmp_path: Any, name: str) -> bytes:
    return bytes((tmp_path / name).read_bytes())
