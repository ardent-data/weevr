"""Tests for scripts/input_fingerprint.py, the test-input fingerprint."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from input_fingerprint import FingerprintError, explain, fingerprint

REPO_ROOT = Path(__file__).resolve().parents[1]

RELEASE_CONFIG = {
    "packages": {
        ".": {
            "release-type": "python",
            "extra-files": [
                {
                    "path": "uv.lock",
                    "type": "toml",
                    "jsonpath": "$.package[?(@.name.value=='weevr')].version",
                },
                {
                    "path": "uv.lock",
                    "type": "toml",
                    "jsonpath": "$.package[?(@.name.value=='weevr-core')].version",
                },
                {"path": "packages/weevr/pyproject.toml", "type": "generic"},
                {"path": "docs/reference/compatibility.md", "type": "generic"},
            ],
        }
    }
}

UV_LOCK = """\
version = 1

[[package]]
name = "pyspark"
version = "4.1.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "weevr"
version = "{weevr}"
source = { editable = "packages/weevr" }

[[package]]
name = "weevr-core"
version = "{core}"
source = { editable = "packages/weevr-core" }
"""

PYPROJECT = """\
[project]
name = "weevr"
version = "{v}" # x-release-please-version
dependencies = [
    "weevr-core=={v}", # x-release-please-version
    "pydantic>=2.0,<3.0",
]
"""

COMPAT = "Last validated against weevr **{v}**. <!-- x-release-please-version -->\n"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout


def _write(repo: Path, rel: str, text: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _release(repo: Path, version: str) -> None:
    """Rewrite every version field the way a release-please commit does."""
    _write(repo, "uv.lock", UV_LOCK.replace("{weevr}", version).replace("{core}", version))
    _write(repo, "packages/weevr/pyproject.toml", PYPROJECT.replace("{v}", version))
    _write(repo, "docs/reference/compatibility.md", COMPAT.replace("{v}", version))
    _write(repo, ".release-please-manifest.json", json.dumps({".": version}) + "\n")
    changelog = repo / "CHANGELOG.md"
    _write(repo, "CHANGELOG.md", f"## {version}\n\n" + changelog.read_text())


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A committed miniature weevr checkout at version 1.0.0."""
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _write(root, "release-please-config.json", json.dumps(RELEASE_CONFIG, indent=2))
    _write(root, "CHANGELOG.md", "# Changelog\n")
    _write(root, ".gitignore", "ignored.txt\n")
    _write(root, "packages/weevr/src/weevr/engine.py", "def run():\n    return 1\n")
    _write(root, "tests/test_engine.py", "def test_run():\n    assert True\n")
    _write(root, ".github/workflows/spark-tests.yaml", "name: Spark Tests\n")
    _write(root, ".github/workflows/code-quality.yaml", "name: Code Quality\n")
    _write(root, ".github/workflows/docs.yml", "name: Docs\n")
    _write(root, ".github/dependabot.yml", "version: 2\n")
    for name in ("CODE_OF_CONDUCT.md", "CLA-I.md", "SECURITY.md", "LICENSE", "README.md"):
        _write(root, name, f"{name}\n")
    _write(root, "CHANGELOG.md", "# Changelog\n")
    _release(root, "1.0.0")
    _commit(root, "initial")
    return root


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", message)


def test_output_is_a_sha256_hex_digest(repo: Path) -> None:
    digest = fingerprint(repo)
    assert len(digest) == 64
    int(digest, 16)


def test_identical_trees_match(repo: Path, tmp_path: Path) -> None:
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", str(repo), str(clone)], check=True)
    assert fingerprint(clone) == fingerprint(repo)


@pytest.mark.parametrize(
    "rel",
    [
        "packages/weevr/src/weevr/engine.py",
        "tests/test_engine.py",
        "README.md",
        ".github/workflows/spark-tests.yaml",
        ".github/workflows/code-quality.yaml",
        "docs/guide.md",
        "examples/sample.thread",
    ],
)
def test_input_change_changes_fingerprint(repo: Path, rel: str) -> None:
    before = fingerprint(repo)
    path = repo / rel
    _write(repo, rel, (path.read_text() if path.exists() else "") + "# changed\n")
    assert fingerprint(repo) != before


@pytest.mark.parametrize(
    "rel",
    [
        "CHANGELOG.md",
        "release-please-config.json",
        ".release-please-manifest.json",
        ".github/workflows/docs.yml",
        ".github/dependabot.yml",
        ".github/ISSUE_TEMPLATE/new.yml",
        "CODE_OF_CONDUCT.md",
        "CLA-I.md",
        "CLA-E.md",
        "SECURITY.md",
        "LICENSE",
    ],
)
def test_denylisted_change_keeps_fingerprint(repo: Path, rel: str) -> None:
    before = fingerprint(repo)
    path = repo / rel
    _write(repo, rel, (path.read_text() if path.exists() else "") + "\n \n")
    assert fingerprint(repo) == before


def test_release_version_bump_keeps_fingerprint(repo: Path) -> None:
    before = fingerprint(repo)
    _release(repo, "1.0.1-alpha.2")
    assert fingerprint(repo) == before


def test_block_markers_are_not_normalized(repo: Path) -> None:
    # Only lines carrying the x-release-please-version marker are normalized.
    # Nothing in weevr uses the start/end block form, so a version inside one
    # is hashed like any other content.
    block = "<!-- x-release-please-start-version -->\nweevr {v}\n<!-- x-release-please-end -->\n"
    path = repo / "docs/reference/compatibility.md"
    base = path.read_text()
    path.write_text(base + block.replace("{v}", "1.0.0"))
    before = fingerprint(repo)
    path.write_text(base + block.replace("{v}", "1.0.1"))
    assert fingerprint(repo) != before


def test_non_marker_pyproject_line_changes_fingerprint(repo: Path) -> None:
    before = fingerprint(repo)
    path = repo / "packages/weevr/pyproject.toml"
    path.write_text(path.read_text().replace("pydantic>=2.0,<3.0", "pydantic>=2.1,<3.0"))
    assert fingerprint(repo) != before


def test_other_package_version_in_lock_changes_fingerprint(repo: Path) -> None:
    before = fingerprint(repo)
    path = repo / "uv.lock"
    path.write_text(path.read_text().replace('version = "4.1.1"', 'version = "4.1.2"'))
    assert fingerprint(repo) != before


def test_inter_wheel_pin_without_marker_changes_fingerprint(repo: Path) -> None:
    before = fingerprint(repo)
    path = repo / "packages/weevr/pyproject.toml"
    path.write_text(path.read_text().replace('"pydantic>=2.0,<3.0",', '"weevr-core>=1.0",'))
    assert fingerprint(repo) != before


@pytest.mark.parametrize(
    "entry",
    [
        {"path": "package.json", "type": "json", "jsonpath": "$.version"},
        {"path": "uv.lock", "type": "toml", "jsonpath": "$.tool.version"},
        {"path": "uv.lock", "type": "yaml", "jsonpath": "$.version"},
    ],
)
def test_unknown_extra_file_entry_errors(repo: Path, entry: dict[str, str]) -> None:
    config = json.loads((repo / "release-please-config.json").read_text())
    config["packages"]["."]["extra-files"].append(entry)
    _write(repo, "release-please-config.json", json.dumps(config))
    with pytest.raises(FingerprintError, match=entry["path"]):
        fingerprint(repo)


def test_untracked_file_counts_ignored_file_does_not(repo: Path) -> None:
    before = fingerprint(repo)
    _write(repo, "ignored.txt", "ignored\n")
    assert fingerprint(repo) == before
    _write(repo, "packages/weevr/src/weevr/new.py", "X = 1\n")
    assert fingerprint(repo) != before


def test_deleted_tracked_file_changes_fingerprint(repo: Path) -> None:
    before = fingerprint(repo)
    (repo / "tests/test_engine.py").unlink()
    assert fingerprint(repo) != before


def test_explain_reports_count_and_denylisted_paths(repo: Path) -> None:
    info = explain(repo)
    assert info.digest == fingerprint(repo)
    assert info.file_count > 0
    assert "CHANGELOG.md" in info.denylisted
    assert ".github/workflows/spark-tests.yaml" not in info.denylisted


def test_cli_prints_digest_and_explains_on_stderr(repo: Path) -> None:
    script = REPO_ROOT / "scripts" / "input_fingerprint.py"
    result = subprocess.run(
        ["python3", str(script), "--explain", str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == fingerprint(repo)
    assert "CHANGELOG.md" in result.stderr


def test_cli_exits_nonzero_on_unknown_entry(repo: Path) -> None:
    config = json.loads((repo / "release-please-config.json").read_text())
    config["packages"]["."]["extra-files"].append({"path": "x.json", "type": "json"})
    _write(repo, "release-please-config.json", json.dumps(config))
    script = REPO_ROOT / "scripts" / "input_fingerprint.py"
    result = subprocess.run(["python3", str(script), str(repo)], capture_output=True, text=True)
    assert result.returncode != 0
    assert "x.json" in result.stderr
    assert result.stdout == ""


# Real release commits: one per line. The CI checkout is shallow, so these run
# only where the history is present (a full local clone).
RELEASE_COMMITS = ["a06d9d9b", "b7df7092"]


def _has_commit(sha: str) -> bool:
    probe = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "cat-file", "-e", f"{sha}^{{commit}}"],
        capture_output=True,
    )
    parent = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "cat-file", "-e", f"{sha}^^{{commit}}"],
        capture_output=True,
    )
    return probe.returncode == 0 and parent.returncode == 0


@pytest.mark.parametrize("sha", RELEASE_COMMITS)
def test_real_release_commit_matches_its_parent(sha: str, tmp_path: Path) -> None:
    if not _has_commit(sha):
        pytest.skip(f"{sha} or its parent is not in this clone (shallow checkout)")
    clone = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", "-q", "--shared", "--no-checkout", str(REPO_ROOT), str(clone)],
        check=True,
    )
    _git(clone, "checkout", "-q", sha)
    released = fingerprint(clone)
    _git(clone, "checkout", "-q", f"{sha}^")
    assert fingerprint(clone) == released
