#!/usr/bin/env python3
"""Find, or write, the record of a passing Spark Tests run for a fingerprint.

A full Spark Tests run that passes uploads a small artifact named
``spark-pass-<fingerprint>`` (see ``scripts/input_fingerprint.py``). A later
run with the same fingerprint looks it up and, when it can trust it, reports
the earlier run instead of testing the same inputs again.

A record is trusted only when all of these hold:

* the artifact name matches exactly and the artifact has not expired;
* the run that uploaded it was triggered from this repository, not a fork
  (``head_repository_id`` is set by GitHub, so a fork PR can't forge it);
* that run is the Spark Tests workflow and concluded ``success``.

A lookup problem never fails the job: it reports ``lookup-error`` and the
workflow runs in full.

Usage:
    python scripts/spark_reuse.py lookup FINGERPRINT [--fresh]
    python scripts/spark_reuse.py record OUT --fingerprint FP --run-url URL
        --sha SHA --event EVENT --coverage PCT    (one line)

``lookup`` prints ``key=value`` lines for ``$GITHUB_OUTPUT``: ``reuse_url``,
``reuse_run_id`` and ``reason`` (``found``, ``no-record``, ``forced`` or
``lookup-error: <cause>``). It reads ``GITHUB_REPOSITORY`` and
``GITHUB_REPOSITORY_ID`` and calls the API through ``gh api``.

Only the standard library is used.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

RECORD_PREFIX = "spark-pass-"
WORKFLOW_PATH = ".github/workflows/spark-tests.yaml"
DIGEST = re.compile(r"^[0-9a-f]{64}$")

Fetch = Callable[[str], dict[str, Any]]


def select_record(
    artifacts: list[dict[str, Any]],
    fetch_run: Callable[[int], dict[str, Any]],
    repo_id: int,
    fingerprint: str,
) -> dict[str, Any] | None:
    """Return the newest trusted record among ``artifacts``, or None."""
    name = f"{RECORD_PREFIX}{fingerprint}"
    candidates = [
        a
        for a in artifacts
        if a.get("name") == name
        and not a.get("expired", True)
        and (a.get("workflow_run") or {}).get("head_repository_id") == repo_id
    ]
    candidates.sort(key=lambda a: a.get("created_at") or "", reverse=True)
    for artifact in candidates:
        run = fetch_run(int(artifact["workflow_run"]["id"]))
        if (
            run.get("path") == WORKFLOW_PATH
            and run.get("conclusion") == "success"
            and (run.get("head_repository") or {}).get("id") == repo_id
        ):
            return {
                "run_id": run["id"],
                "run_url": run.get("html_url", ""),
                "sha": run.get("head_sha", ""),
                "event": run.get("event", ""),
            }
    return None


def lookup(
    fingerprint: str, repo: str, repo_id: int, *, fresh: bool, fetch: Fetch
) -> dict[str, str]:
    """Decide whether this run can reuse an earlier one; never raises."""
    result = {"reuse_url": "", "reuse_run_id": "", "reason": "no-record"}
    if fresh:
        result["reason"] = "forced"
        return result
    try:
        if not DIGEST.match(fingerprint):
            raise ValueError(f"no usable fingerprint ({fingerprint!r})")
        listing = fetch(
            f"repos/{repo}/actions/artifacts?name={RECORD_PREFIX}{fingerprint}&per_page=100"
        )
        found = select_record(
            listing.get("artifacts", []),
            lambda run_id: fetch(f"repos/{repo}/actions/runs/{run_id}"),
            repo_id,
            fingerprint,
        )
    except Exception as exc:  # any lookup failure means a full run, never a failed job
        cause = " ".join(str(exc).split()) or type(exc).__name__
        result["reason"] = f"lookup-error: {cause}"
        return result
    if found:
        result.update(reuse_url=found["run_url"], reuse_run_id=str(found["run_id"]), reason="found")
    return result


def write_record(
    out: Path, *, fingerprint: str, run_url: str, sha: str, event: str, coverage: str
) -> None:
    """Write the JSON body of a ``spark-pass-<fingerprint>`` artifact."""
    body = {
        "fingerprint": fingerprint,
        "run_url": run_url,
        "sha": sha,
        "event": event,
        "coverage": coverage,
    }
    out.write_text(json.dumps(body, indent=2) + "\n")


def _gh_fetch(endpoint: str) -> dict[str, Any]:
    result = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gh api {endpoint.split('?')[0]}: {result.stderr.strip()[:200]}")
    return json.loads(result.stdout)


def main(argv: list[str] | None = None) -> int:
    """Run the ``lookup`` or ``record`` subcommand."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    sub = parser.add_subparsers(dest="command", required=True)
    find = sub.add_parser("lookup", help="find a trusted passing run")
    find.add_argument("fingerprint")
    find.add_argument("--fresh", action="store_true", help="skip the lookup; run in full")
    rec = sub.add_parser("record", help="write a record body")
    rec.add_argument("out", type=Path)
    for flag in ("--fingerprint", "--run-url", "--sha", "--event", "--coverage"):
        rec.add_argument(flag, required=True)
    args = parser.parse_args(argv)

    if args.command == "record":
        write_record(
            args.out,
            fingerprint=args.fingerprint,
            run_url=args.run_url,
            sha=args.sha,
            event=args.event,
            coverage=args.coverage,
        )
        return 0

    repo = os.environ.get("GITHUB_REPOSITORY", "")
    repo_id = int(os.environ.get("GITHUB_REPOSITORY_ID") or 0)
    out = lookup(args.fingerprint, repo, repo_id, fresh=args.fresh, fetch=_gh_fetch)
    for key, value in out.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
