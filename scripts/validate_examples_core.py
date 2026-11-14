#!/usr/bin/env python3
"""Load and validate every example config using weevr-core alone.

Run with an interpreter that has only the weevr-core wheel installed (no
engine wheel, no PySpark). It proves the core's validation surface works on
the lowest Python the core supports. CI runs it on Python 3.12.

Every ``.thread``, ``.weave`` and ``.loom`` under ``examples/`` must load with
``load_config`` against its project root, and every ``.warp`` must validate.
The script reports every failing config and exits non-zero if any fails, or
if PySpark is importable at all.

Usage:
    python scripts/validate_examples_core.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

from weevr.config import load_config
from weevr.model.warp import WarpConfig

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
FABCON_DIR = EXAMPLES_DIR / "fabcon"

# Required params per example, keyed by path relative to examples/. Kept in
# step with tests/weevr/test_examples.py.
REQUIRED_PARAMS: dict[str, dict[str, str]] = {
    "sales_lakehouse.loom": {"lakehouse_path": "/lakehouse/default"},
    "fabcon/daily.loom": {"lakehouse_path": "/lakehouse/default"},
}


def _project_root(path: Path) -> Path:
    """Return the project root an example resolves its references against."""
    return FABCON_DIR if FABCON_DIR in path.parents else EXAMPLES_DIR


def main() -> int:
    """Validate every example; return a process exit code."""
    # weevr.context ships only in the engine wheel.
    for module in ("pyspark", "weevr.context"):
        if importlib.util.find_spec(module) is not None:
            print(
                f"{module} is importable: run this with weevr-core installed alone", file=sys.stderr
            )
            return 1

    configs = sorted(
        p for ext in ("*.thread", "*.weave", "*.loom") for p in EXAMPLES_DIR.rglob(ext)
    )
    warps = sorted(EXAMPLES_DIR.rglob("*.warp"))
    if not configs or not warps:
        print(f"no examples found under {EXAMPLES_DIR}", file=sys.stderr)
        return 1

    failures = 0
    for path in configs:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        try:
            load_config(
                path, runtime_params=REQUIRED_PARAMS.get(rel), project_root=_project_root(path)
            )
        except Exception as exc:
            failures += 1
            print(f"FAIL {rel}: {exc}", file=sys.stderr)
    for path in warps:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        try:
            WarpConfig.model_validate(yaml.safe_load(path.read_text()))
        except Exception as exc:
            failures += 1
            print(f"FAIL {rel}: {exc}", file=sys.stderr)

    total = len(configs) + len(warps)
    print(f"{total - failures}/{total} examples valid on Python {sys.version.split()[0]}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
