"""Mkdocs hook that materializes a merged weevr/ source tree.

The federated weevr / weevr-core split puts ``weevr/__init__.py`` in
the engine wheel and the pure-Python helpers (config core, model,
errors, telemetry contracts, planner, formatting, plus the root-level
``result``, ``duration``, and ``reserved_words``) under weevr-core's
``src/``. Mkdocstrings' griffe-based resolver does not merge namespace
contributions across multiple search paths once a regular package
(``__init__.py``) is found in one of them — so a global
``paths: [packages/weevr-core/src, packages/weevr/src]`` always loses
half the surface.

This hook runs at module-import time (before mkdocstrings initializes
its python handler, which globs ``paths`` once and caches the result),
deep-merges both ``src/weevr/`` trees into a single workspace under
``.docs-merged/``, and points ``mkdocstrings.handlers.python.paths``
at the merged tree only. Every ``::: weevr.X`` directive then resolves
cleanly.

The merged tree is symlink-only (no copies), regenerated on each
import, and lives outside ``site/`` so mkdocs' clean-site step does
not wipe it before the build sees it. ``.gitignore`` excludes it.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOTS = (
    REPO_ROOT / "packages" / "weevr-core" / "src" / "weevr",
    REPO_ROOT / "packages" / "weevr" / "src" / "weevr",
)
MERGED_PARENT = REPO_ROOT / ".docs-merged"
MERGED_PACKAGE = MERGED_PARENT / "weevr"


def _merge_dir(src_dirs: list[Path], dest: Path) -> None:
    """Deep-merge each src_dir into dest using symlinks for files."""
    dest.mkdir(parents=True, exist_ok=True)
    # Track which sub-package directories appear in each src so we can
    # recurse with both contributors when a federated sub-package
    # spans both wheels (e.g. weevr.config has __init__.py in core but
    # fabric.py and paths.py in engine).
    sub_dirs: dict[str, list[Path]] = {}
    files: dict[str, Path] = {}
    for src in src_dirs:
        for entry in src.iterdir():
            if entry.name == "__pycache__":
                continue
            if entry.is_dir():
                sub_dirs.setdefault(entry.name, []).append(entry)
            else:
                files.setdefault(entry.name, entry)
    for name, file_src in files.items():
        target = dest / name
        if not target.exists():
            try:
                target.symlink_to(file_src.resolve())
            except OSError as exc:
                raise OSError(
                    f"mkdocs_merge_weevr: failed to symlink {file_src} -> {target}: {exc}"
                ) from exc
    for name, dirs in sub_dirs.items():
        _merge_dir(dirs, dest / name)


def _materialize_merged_tree() -> None:
    if MERGED_PARENT.exists():
        shutil.rmtree(MERGED_PARENT)
    MERGED_PARENT.mkdir()
    _merge_dir(list(SRC_ROOTS), MERGED_PACKAGE)


# Materialize the merged tree at module-import time. mkdocs imports
# this hook module while parsing mkdocs.yml — before mkdocstrings
# initializes its python handler. By the time mkdocstrings globs
# paths: [.docs-merged], the merged tree is already on disk.
_materialize_merged_tree()


def on_config(config: Any, **_kwargs: Any) -> Any:
    """Re-materialize on every reload (mkdocs serve picks up source edits)."""
    _materialize_merged_tree()
    return config
