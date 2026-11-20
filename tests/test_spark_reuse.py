"""Tests for scripts/spark_reuse.py, the Spark Tests reuse-record lookup."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from spark_reuse import RECORD_PREFIX, WORKFLOW_PATH, lookup, select_record, write_record

REPO_ID = 1151043568
FORK_ID = 99
FP = "a" * 64
NAME = f"{RECORD_PREFIX}{FP}"


def _artifact(
    run_id: int,
    *,
    name: str = NAME,
    expired: bool = False,
    head_repo: int = REPO_ID,
    created: str = "2026-11-20T10:00:00Z",
) -> dict[str, Any]:
    return {
        "name": name,
        "expired": expired,
        "created_at": created,
        "workflow_run": {"id": run_id, "head_repository_id": head_repo, "repository_id": REPO_ID},
    }


def _run(
    run_id: int,
    *,
    path: str = WORKFLOW_PATH,
    conclusion: str | None = "success",
    head_repo: int = REPO_ID,
) -> dict[str, Any]:
    return {
        "id": run_id,
        "path": path,
        "conclusion": conclusion,
        "html_url": f"https://github.com/o/r/actions/runs/{run_id}",
        "head_sha": f"sha{run_id}",
        "event": "pull_request",
        "head_repository": {"id": head_repo},
    }


def _runs(*runs: dict[str, Any]):
    by_id = {r["id"]: r for r in runs}
    return lambda run_id: by_id[run_id]


def test_trusted_record_is_selected() -> None:
    found = select_record([_artifact(1)], _runs(_run(1)), REPO_ID, FP)
    assert found is not None
    assert found["run_id"] == 1
    assert found["run_url"].endswith("/runs/1")


@pytest.mark.parametrize(
    ("artifact", "run"),
    [
        pytest.param(_artifact(1, expired=True), _run(1), id="expired"),
        pytest.param(_artifact(1, head_repo=FORK_ID), _run(1), id="fork-artifact"),
        pytest.param(_artifact(1), _run(1, head_repo=FORK_ID), id="fork-run"),
        pytest.param(_artifact(1), _run(1, path=".github/workflows/docs.yml"), id="other-workflow"),
        pytest.param(_artifact(1), _run(1, conclusion="failure"), id="failed-run"),
        pytest.param(_artifact(1), _run(1, conclusion=None), id="in-progress-run"),
        pytest.param(_artifact(1, name=f"{NAME}x"), _run(1), id="name-mismatch"),
    ],
)
def test_untrusted_record_is_never_selected(artifact: dict, run: dict) -> None:
    assert select_record([artifact], _runs(run), REPO_ID, FP) is None


def test_newest_trusted_record_wins() -> None:
    artifacts = [
        _artifact(1, created="2026-11-19T10:00:00Z"),
        _artifact(2, created="2026-11-20T10:00:00Z"),
        _artifact(3, created="2026-11-21T10:00:00Z", head_repo=FORK_ID),
    ]
    found = select_record(artifacts, _runs(_run(1), _run(2), _run(3)), REPO_ID, FP)
    assert found is not None
    assert found["run_id"] == 2


def test_untrusted_newest_falls_back_to_older_trusted() -> None:
    artifacts = [
        _artifact(1, created="2026-11-19T10:00:00Z"),
        _artifact(2, created="2026-11-20T10:00:00Z"),
    ]
    runs = _runs(_run(1), _run(2, conclusion="failure"))
    found = select_record(artifacts, runs, REPO_ID, FP)
    assert found is not None
    assert found["run_id"] == 1


def _fetcher(artifacts: list[dict], runs: list[dict]):
    by_id = {r["id"]: r for r in runs}

    def fetch(endpoint: str) -> dict:
        if "/actions/artifacts" in endpoint:
            assert f"name={NAME}" in endpoint
            return {"total_count": len(artifacts), "artifacts": artifacts}
        return by_id[int(endpoint.rsplit("/", 1)[1])]

    return fetch


def test_lookup_found() -> None:
    out = lookup(FP, "o/r", REPO_ID, fresh=False, fetch=_fetcher([_artifact(7)], [_run(7)]))
    assert out == {
        "reuse_url": "https://github.com/o/r/actions/runs/7",
        "reuse_run_id": "7",
        "reason": "found",
    }


def test_lookup_no_record() -> None:
    out = lookup(FP, "o/r", REPO_ID, fresh=False, fetch=_fetcher([], []))
    assert out == {"reuse_url": "", "reuse_run_id": "", "reason": "no-record"}


def test_lookup_forced_makes_no_call() -> None:
    def fetch(endpoint: str) -> dict:
        raise AssertionError("no API call expected")

    out = lookup(FP, "o/r", REPO_ID, fresh=True, fetch=fetch)
    assert out["reason"] == "forced"
    assert out["reuse_url"] == ""


def test_lookup_api_error_is_a_full_run_not_a_failure() -> None:
    def fetch(endpoint: str) -> dict:
        raise RuntimeError("HTTP 502")

    out = lookup(FP, "o/r", REPO_ID, fresh=False, fetch=fetch)
    assert out["reuse_url"] == ""
    assert out["reason"].startswith("lookup-error: ")
    assert "HTTP 502" in out["reason"]


@pytest.mark.parametrize("fp", ["", "not-a-digest"])
def test_lookup_rejects_a_missing_or_malformed_fingerprint(fp: str) -> None:
    out = lookup(fp, "o/r", REPO_ID, fresh=False, fetch=_fetcher([], []))
    assert out["reuse_url"] == ""
    assert out["reason"].startswith("lookup-error: ")


def test_lookup_error_reason_is_one_line() -> None:
    def fetch(endpoint: str) -> dict:
        raise RuntimeError("line one\nline two")

    out = lookup(FP, "o/r", REPO_ID, fresh=False, fetch=fetch)
    assert "\n" not in out["reason"]


def test_write_record(tmp_path: Path) -> None:
    out = tmp_path / "record.json"
    write_record(out, fingerprint=FP, run_url="u", sha="s", event="push", coverage="91.2")
    assert json.loads(out.read_text()) == {
        "fingerprint": FP,
        "run_url": "u",
        "sha": "s",
        "event": "push",
        "coverage": "91.2",
    }
