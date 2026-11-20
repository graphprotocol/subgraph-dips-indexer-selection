"""Copies of the service's cache files in a Cloud Storage bucket.

The scores and freshness files live on 1 disk. Lost scores come back with the next daily push,
but selections are refused until then, and the freshness history exists nowhere else. With
IISA_BACKUP_URL set, such as ``gs://iisa-backups/testnet-v2``, the service copies each file to
the bucket when it changes, and at startup restores a file that is missing or unreadable from
its copy, so neither waits on the next push nor starts again from nothing.
"""

import contextlib
import hashlib
import http.client
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Protocol, Self
from urllib.parse import quote

logger = logging.getLogger(__name__)

COPY_INTERVAL_SECONDS = 3600
# How long startup keeps trying a bucket it can't reach before it starts without the copy.
RESTORE_WAIT_SECONDS = 60.0
REQUEST_TIMEOUT_SECONDS = 30.0
MAX_PAUSE_SECONDS = 10.0
TOKEN_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
)
STORAGE_URL = "https://storage.googleapis.com"
# What each file's copy is called, fixed so that 2 files can't share one whatever their paths.
SCORES_COPY = "indexer_scores.json"
FRESHNESS_COPY = "freshness.json"
# Answers that may succeed if asked again; a 401 is among them, as a new token is fetched.
RETRYABLE_STATUSES = frozenset({401, 408, 429, 500, 502, 503, 504})
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]")


class BackupError(Exception):
    """A request to the bucket, or for a token to use on it, failed."""

    def __init__(self, message: str, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True)
class BackupLocation:
    """The bucket the copies go to, and the path in it, empty or ending in "/", they go under."""

    bucket: str
    prefix: str

    @classmethod
    def parse(cls, url: str) -> Self:
        """Read ``gs://<bucket>`` or ``gs://<bucket>/<path>``, raising ValueError otherwise."""
        rest = url.removeprefix("gs://")
        bucket, _, path = rest.partition("/")
        if rest == url or not _BUCKET_NAME.fullmatch(bucket):
            raise ValueError("expected gs://<bucket> or gs://<bucket>/<path>")
        path = path.strip("/")
        return cls(bucket, f"{path}/" if path else "")

    def object_name(self, name: str) -> str:
        """The full name of the copy called ``name``, under the prefix."""
        return f"{self.prefix}{name}"

    def __str__(self) -> str:
        return f"gs://{self.bucket}/{self.prefix}"


class Bucket(Protocol):
    def put(self, name: str, data: bytes) -> None: ...

    def get(self, name: str) -> Optional[bytes]: ...


class GcsBucket:
    """The objects in 1 bucket, through Cloud Storage's JSON API, signed in as the pod's Google
    service account with a token from the GKE metadata server.
    """

    def __init__(self, bucket: str, open_url: Callable[..., Any] = urllib.request.urlopen) -> None:
        self._bucket = quote(bucket, safe="")
        self._open = open_url
        self._token: Optional[tuple[str, float]] = None
        self._token_lock = threading.Lock()

    def put(self, name: str, data: bytes) -> None:
        """Write ``data`` as the object ``name``, replacing any object of that name."""
        url = f"{STORAGE_URL}/upload/storage/v1/b/{self._bucket}/o"
        url += f"?uploadType=media&name={quote(name, safe='')}"
        headers = {"Content-Type": "application/octet-stream"}
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        if self._send(request) is None:
            raise BackupError(f"No bucket at {url}", retryable=False)

    def get(self, name: str) -> Optional[bytes]:
        """The object ``name``'s content, or None when there is no such object."""
        url = f"{STORAGE_URL}/storage/v1/b/{self._bucket}/o/{quote(name, safe='')}?alt=media"
        return self._send(urllib.request.Request(url, method="GET"))

    def _send(self, request: urllib.request.Request) -> Optional[bytes]:
        """Send ``request`` with a token and return the answer's body, or None for a 404."""
        request.add_header("Authorization", f"Bearer {self._access_token()}")
        what = f"{request.get_method()} {request.full_url}"
        try:
            with self._open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return bytes(response.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 401:
                self._forget_token()
            raise BackupError(f"{what} returned {e.code}", e.code in RETRYABLE_STATUSES) from e
        except (OSError, http.client.HTTPException) as e:
            raise BackupError(f"{what} failed: {e!r}", retryable=True) from e

    def _access_token(self) -> str:
        """A token for the pod's Google service account, fetched again a minute before it ends."""
        with self._token_lock:
            if self._token is not None and time.monotonic() < self._token[1]:
                return self._token[0]
            request = urllib.request.Request(TOKEN_URL, headers={"Metadata-Flavor": "Google"})
            try:
                with self._open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    body = json.loads(response.read())
                token, lifetime = str(body["access_token"]), float(body["expires_in"])
            except (OSError, http.client.HTTPException, ValueError, KeyError, TypeError) as e:
                raise BackupError(f"No token from the GKE metadata server: {e!r}", True) from e
            self._token = (token, time.monotonic() + lifetime - 60)
            return token

    def _forget_token(self) -> None:
        with self._token_lock:
            self._token = None


class Backup:
    """Copies the cache files to a bucket and restores them from it."""

    def __init__(
        self,
        location: BackupLocation,
        bucket: Bucket,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        restore_wait: float = RESTORE_WAIT_SECONDS,
    ) -> None:
        self.location = location
        self._bucket = bucket
        self._sleep = sleep
        self._clock = clock
        self._restore_wait = restore_wait
        # The digest of what each file held when last copied or restored, so an unchanged file
        # isn't copied again.
        self._copied: dict[str, str] = {}
        # Files whose copy couldn't be read at startup. Copying them would replace that copy,
        # maybe the only full history, with what the service started again from.
        self._held: set[str] = set()
        self._lock = threading.Lock()

    def restore(self, path: str, copy: str, hold: bool = False) -> bool:
        """Write the bucket's copy called ``copy`` over the file at ``path``. True when it did;
        False when there is no copy, or the bucket stayed unreachable for the restore wait. With
        ``hold``, a file whose copy couldn't be read isn't copied until restart, as it builds on
        itself.
        """
        name = self.location.object_name(copy)
        try:
            data = self._get_retrying(name)
        except BackupError as e:
            logger.error(
                "Could not restore %s from gs://%s/%s (%s); starting without it",
                path,
                self.location.bucket,
                name,
                e,
            )
            if hold:
                self.hold(path)
            return False
        if data is None:
            logger.warning("No copy of %s in %s to restore it from", path, self.location)
            return False
        try:
            _write_atomically(path, data)
        except OSError as e:
            logger.error("Could not write the restored %s: %s", path, e)
            return False
        with self._lock:
            self._copied[path] = _digest(data)
        logger.info(
            "Restored %s from gs://%s/%s (%d bytes)", path, self.location.bucket, name, len(data)
        )
        return True

    def hold(self, path: str) -> None:
        """Stop copying the file at ``path`` until the service restarts, so the copy in the
        bucket, which it couldn't read or use, isn't replaced by what it started again from."""
        with self._lock:
            self._held.add(path)
        logger.error(
            "Not copying %s to %s until the service restarts, so the copy there is kept; the "
            "bucket's older versions keep it after that",
            path,
            self.location,
        )

    def copy_if_changed(self, path: str, copy: str) -> None:
        """Copy the file at ``path`` to the bucket as ``copy`` unless it holds what was last
        copied or restored. A failure is logged and the next call tries again.
        """
        with self._lock:
            if path in self._held:
                logger.warning(
                    "Not copying %s: its copy in the bucket couldn't be read at startup", path
                )
                return
            try:
                data = Path(path).read_bytes()
            except FileNotFoundError:
                logger.debug("No %s to copy to the bucket yet", path)
                return
            except OSError as e:
                logger.error("Could not read %s to copy it to the bucket: %s", path, e)
                return
            digest = _digest(data)
            if self._copied.get(path) == digest:
                logger.debug("%s is unchanged since its last copy", path)
                return
            name = self.location.object_name(copy)
            try:
                self._bucket.put(name, data)
            except BackupError as e:
                logger.error(
                    "Could not copy %s to gs://%s/%s: %s", path, self.location.bucket, name, e
                )
                return
            self._copied[path] = digest
        logger.info(
            "Copied %s to gs://%s/%s (%d bytes)", path, self.location.bucket, name, len(data)
        )

    def _get_retrying(self, name: str) -> Optional[bytes]:
        """The object ``name``, asked for again with growing pauses while a failure may clear up
        and the restore wait lasts.
        """
        deadline = self._clock() + self._restore_wait
        pause = 1.0
        while True:
            try:
                return self._bucket.get(name)
            except BackupError as e:
                left = deadline - self._clock()
                if not e.retryable or left <= 0:
                    raise
                wait = min(pause, left)
                logger.warning(
                    "Reading %s from the bucket failed (%s); retrying in %.0fs", name, e, wait
                )
                self._sleep(wait)
                pause = min(pause * 2, MAX_PAUSE_SECONDS)


def backup_from(url: Optional[str]) -> Optional[Backup]:
    """A Backup to the bucket ``url`` names, or None when it is unset."""
    if not url:
        return None
    location = BackupLocation.parse(url)
    return Backup(location, GcsBucket(location.bucket))


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_atomically(path: str, data: bytes) -> None:
    """Write ``data`` over ``path`` through a temp file beside it, named as the service's other
    writes name theirs so that startup removes one a crash left behind, then flush the folder so
    the rename survives a power cut.
    """
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp_path, "xb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)
        raise
    try:
        folder = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(folder)
        finally:
            os.close(folder)
    except OSError as e:
        # The file is already in place, so this only costs its survival through a power cut.
        logger.warning("Could not flush %s to disk after restoring %s: %s", parent, path, e)
