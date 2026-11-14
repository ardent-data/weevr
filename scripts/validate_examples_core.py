#!/usr/bin/env python3
"""Load and validate every example config using weevr-core alone.

Run with an interpreter that has only the weevr-core wheel installed (no
engine wheel, no PySpark). It proves the core's validation surface works on
the lowest Python the core supports. CI runs it on Python 3.12.

Every ``.thread``, ``.weave`` and ``.loom`` under ``examples/`` must load with
``load_config`` against its project root, every ``.warp`` must validate, and
every named warp reference must resolve. The script reports every failing
config and exits non-zero if any fails, or if PySpark is importable at all.

``tests/weevr/test_examples.py`` imports the example discovery and required
params from here, so both checks cover the same files the same way.

Usage:
    python scripts/validate_examples_core.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import yaml

from weevr.config import load_config
from weevr.config.warp import resolve_warp
from weevr.model import Thread
from weevr.model.warp import WarpConfig

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
FABCON_DIR = EXAMPLES_DIR / "fabcon"

CONFIG_FILES = sorted(
    p for ext in ("*.thread", "*.weave", "*.loom") for p in EXAMPLES_DIR.rglob(ext)
)
WARP_FILES = sorted(EXAMPLES_DIR.rglob("*.warp"))

# Required params per example, keyed by path relative to examples/. A required
# param missing here fails the load instead of silently defaulting.
REQUIRED_PARAMS: dict[str, dict[str, Any]] = {
    "sales_lakehouse.loom": {"lakehouse_path": "/lakehouse/default"},
    "fabcon/daily.loom": {"lakehouse_path": "/lakehouse/default"},
}


def rel(path: Path) -> str:
    """Return an example's path relative to ``examples/``."""
    return path.relative_to(EXAMPLES_DIR).as_posix()


def project_root(path: Path) -> Path:
    """Return the project root an example resolves its references against."""
    return FABCON_DIR if FABCON_DIR in path.parents else EXAMPLES_DIR


def names_warp(path: Path) -> bool:
    """Return whether a config's target names its warp by string."""
    target = (yaml.safe_load(path.read_text()) or {}).get("target") or {}
    return isinstance(target.get("warp"), str)


WARP_NAMING_THREADS = [p for p in CONFIG_FILES if p.suffix == ".thread" and names_warp(p)]


def load_example(path: Path) -> Any:
    """Load an example config with its required params and project root."""
    return load_config(
        path, runtime_params=REQUIRED_PARAMS.get(rel(path)), project_root=project_root(path)
    )


def validate_warp(path: Path) -> WarpConfig:
    """Validate a ``.warp`` file as a WarpConfig."""
    return WarpConfig.model_validate(yaml.safe_load(path.read_text()))


def check_warp_reference(path: Path) -> None:
    """Raise if a thread that names its warp can't resolve it."""
    thread = load_example(path)
    if not isinstance(thread, Thread) or not isinstance(thread.target.warp, str):
        raise TypeError(f"expected a thread naming its warp, got {type(thread).__name__}")
    if resolve_warp(thread.target.warp, path.parent, project_root(path)) is None:
        raise LookupError(f"warp {thread.target.warp!r} not found")


def main() -> int:
    """Validate every example; return a process exit code."""
    # weevr.context ships only in the engine wheel.
    for module in ("pyspark", "weevr.context"):
        if importlib.util.find_spec(module) is not None:
            print(
                f"{module} is importable: run this with weevr-core installed alone", file=sys.stderr
            )
            return 1

    if not CONFIG_FILES or not WARP_FILES or not WARP_NAMING_THREADS:
        print(f"no examples found under {EXAMPLES_DIR}", file=sys.stderr)
        return 1

    checks = [
        *((p, load_example) for p in CONFIG_FILES),
        *((p, validate_warp) for p in WARP_FILES),
        *((p, check_warp_reference) for p in WARP_NAMING_THREADS),
    ]
    failures = 0
    for path, check in checks:
        try:
            check(path)
        except Exception as exc:
            failures += 1
            print(f"FAIL {rel(path)}: {exc}", file=sys.stderr)

    total = len(checks)
    print(f"{total - failures}/{total} example checks passed on Python {sys.version.split()[0]}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
