"""The score job's image installs exact, hash-checked versions on a base image fixed by digest.

Its packages are listed as ranges in requirements.in and compiled into requirements.txt, the lock
the image, CI and the audit install. These tests fail when the 2 files drift apart or when
anything an image is built from is named by a tag alone, which a later build could resolve
differently.
"""

import re
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

REPO = Path(__file__).resolve().parents[2]
JOB = REPO / "cronjobs" / "compute_scores"
DOCKERFILES = [REPO / "Dockerfile", JOB / "Dockerfile"]


def _requirements(path: Path) -> list[str]:
    """The requirement lines in `path`, with comments, hashes and line breaks taken out."""
    text = re.sub(r"\\\n", " ", path.read_text())
    lines = []
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            lines.append(line)
    return lines


def _locked() -> dict[str, str]:
    pins = {}
    for line in _requirements(JOB / "requirements.txt"):
        requirement = Requirement(line.split(" --hash", 1)[0].strip())
        pins[canonicalize_name(requirement.name)] = str(requirement.specifier)
    return pins


def test_every_locked_package_is_pinned_to_1_version_with_its_hashes():
    text = re.sub(r"\\\n", " ", (JOB / "requirements.txt").read_text())
    entries = [
        line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")
    ]

    assert entries
    for entry in entries:
        assert re.match(r"^[A-Za-z0-9._-]+(\[[^\]]+\])?==\S+", entry), entry
        assert "--hash=sha256:" in entry, entry


@pytest.mark.parametrize("line", _requirements(JOB / "requirements.in"))
def test_the_lock_holds_a_version_each_listed_range_accepts(line):
    wanted = Requirement(line)
    locked = _locked().get(canonicalize_name(wanted.name))

    assert locked is not None, f"{wanted.name} is not in requirements.txt; compile it again"
    assert wanted.specifier.contains(locked.removeprefix("=="), prereleases=True), (
        f"requirements.txt holds {wanted.name}{locked}, outside {wanted.specifier}; "
        "compile it again"
    )


@pytest.mark.parametrize("dockerfile", DOCKERFILES, ids=lambda path: str(path.relative_to(REPO)))
def test_every_base_image_is_fixed_by_digest(dockerfile):
    images = re.findall(
        r"^FROM\s+(?:--platform=\S+\s+)?(\S+)", dockerfile.read_text(), re.MULTILINE
    )
    mounted = re.findall(r"--mount=from=(\S+?),", dockerfile.read_text())

    assert images
    for image in images + mounted:
        assert re.search(r"@sha256:[0-9a-f]{64}$", image), image
