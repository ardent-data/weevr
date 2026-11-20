#!/usr/bin/env python3
"""Print the test-input fingerprint of a weevr checkout.

The fingerprint is a SHA-256 over every file in the checkout that can affect a
test run: tracked files plus untracked files that are not ignored, minus a
short denylist of files no test reads (the changelog, release-please state,
community files, and every ``.github/`` file except the two workflows that run
the tests).

Before hashing, the version fields that release-please rewrites are replaced
with a placeholder, so a release commit has the same fingerprint as the tree it
releases. Those fields come from ``extra-files`` in
``release-please-config.json``: a ``generic`` entry normalizes the lines that
carry the ``x-release-please-version`` marker, and a ``toml`` entry on
``uv.lock`` normalizes one package's ``version``. Any other entry is an error,
so the two lists cannot drift apart silently.

The Spark Tests workflow uses the fingerprint to reuse an earlier passing run
of the same inputs. If in doubt, a path is hashed: a missed input would let a
broken tree reuse a green run, while an extra one only costs a full run.

Usage:
    python scripts/input_fingerprint.py [--explain] [PATH]

Only the standard library is used, so no environment sync is needed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Paths no test reads. Exact names, or a prefix ending in "/".
DENYLIST_EXACT = frozenset(
    {
        "CHANGELOG.md",
        "release-please-config.json",
        ".release-please-manifest.json",
        "CODE_OF_CONDUCT.md",
        "SECURITY.md",
        "LICENSE",
    }
)
DENYLIST_PREFIXES = (".github/", "CLA-")
# Files under a denylisted prefix that are still test inputs.
ALLOWLIST = frozenset(
    {
        ".github/workflows/spark-tests.yaml",
        ".github/workflows/code-quality.yaml",
    }
)

VERSION_MARKER = "x-release-please-version"
BLOCK_START = "x-release-please-start-version"
BLOCK_END = "x-release-please-end"
PLACEHOLDER = "<release-version>"
SEMVER = re.compile(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?")
LOCK_JSONPATH = re.compile(r"^\$\.package\[\?\(@\.name\.value==['\"]([^'\"]+)['\"]\)\]\.version$")


class FingerprintError(Exception):
    """The checkout can't be fingerprinted (e.g. an unknown extra-files entry)."""


@dataclass
class Explanation:
    """A fingerprint with the facts behind it."""

    digest: str
    file_count: int
    denylisted: list[str] = field(default_factory=list)


def is_denylisted(path: str) -> bool:
    """Return True when no test reads ``path`` (repo-relative, POSIX)."""
    if path in ALLOWLIST:
        return False
    return path in DENYLIST_EXACT or path.startswith(DENYLIST_PREFIXES)


def _list_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-co", "--exclude-standard", "-z"],
        check=True,
        capture_output=True,
    )
    return sorted({p for p in result.stdout.decode().split("\0") if p})


def _normalization_rules(root: Path) -> tuple[set[str], dict[str, set[str]]]:
    """Read extra-files: generic paths, and lock path -> package names."""
    config_path = root / "release-please-config.json"
    if not config_path.exists():
        return set(), {}
    config = json.loads(config_path.read_text())
    generic: set[str] = set()
    toml_packages: dict[str, set[str]] = {}
    for package in config.get("packages", {}).values():
        for entry in package.get("extra-files", []):
            if isinstance(entry, str):
                generic.add(entry)
                continue
            path, kind = entry.get("path", ""), entry.get("type", "generic")
            if kind == "generic":
                generic.add(path)
                continue
            match = LOCK_JSONPATH.match(entry.get("jsonpath", ""))
            if kind == "toml" and Path(path).name == "uv.lock" and match:
                toml_packages.setdefault(path, set()).add(match.group(1))
                continue
            raise FingerprintError(
                f"release-please extra-files entry for {path!r} (type {kind!r}) "
                "has a shape the fingerprint can't normalize; teach "
                "scripts/input_fingerprint.py about it"
            )
    return generic, toml_packages


def _normalize_generic(text: str) -> str:
    out: list[str] = []
    in_block = False
    for line in text.splitlines(keepends=True):
        if BLOCK_START in line:
            in_block = True
        if in_block or VERSION_MARKER in line:
            line = SEMVER.sub(PLACEHOLDER, line)
        if BLOCK_END in line:
            in_block = False
        out.append(line)
    return "".join(out)


def _normalize_lock(text: str, packages: set[str]) -> str:
    out: list[str] = []
    name: str | None = None
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("["):
            name = None
        elif stripped.startswith("name = "):
            name = stripped.removeprefix("name = ").strip('"')
        elif name in packages and stripped.startswith("version = "):
            line = f'version = "{PLACEHOLDER}"\n'
        out.append(line)
    return "".join(out)


def _content(root: Path, path: str, generic: set[str], lock: dict[str, set[str]]) -> bytes | None:
    full = root / path
    if full.is_symlink():
        return b"symlink:" + str(full.readlink()).encode()
    if not full.is_file():
        return None  # tracked but deleted: leaving it out changes the digest
    data = full.read_bytes()
    if path in generic or path in lock:
        text = data.decode()
        if path in generic:
            text = _normalize_generic(text)
        if path in lock:
            text = _normalize_lock(text, lock[path])
        data = text.encode()
    return data


def explain(root: Path) -> Explanation:
    """Fingerprint ``root`` and report the file count and denylisted paths."""
    generic, lock = _normalization_rules(root)
    digest = hashlib.sha256()
    count = 0
    denylisted: list[str] = []
    for path in _list_files(root):
        if is_denylisted(path):
            denylisted.append(path)
            continue
        data = _content(root, path, generic, lock)
        if data is None:
            continue
        digest.update(path.encode() + b"\0" + data + b"\0")
        count += 1
    return Explanation(digest.hexdigest(), count, denylisted)


def fingerprint(root: Path) -> str:
    """Return the test-input fingerprint of the checkout at ``root``."""
    return explain(root).digest


def main(argv: list[str] | None = None) -> int:
    """Print the fingerprint; with ``--explain``, report details on stderr."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", nargs="?", default=".", help="checkout root (default: .)")
    parser.add_argument("--explain", action="store_true", help="report details on stderr")
    args = parser.parse_args(argv)
    try:
        info = explain(Path(args.path))
    except (FingerprintError, subprocess.CalledProcessError, OSError, ValueError) as exc:
        print(f"input_fingerprint: {exc}", file=sys.stderr)
        return 1
    if args.explain:
        print(f"files hashed: {info.file_count}", file=sys.stderr)
        print(f"denylisted ({len(info.denylisted)}):", file=sys.stderr)
        for path in info.denylisted:
            print(f"  {path}", file=sys.stderr)
    print(info.digest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
