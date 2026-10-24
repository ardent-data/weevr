"""The federated-namespace warning fires only for a partial weevr workspace.

Each case runs a fresh interpreter whose ``sys.path`` has the workspace's
editable ``src`` roots removed and a copied package tree put first, so the
three layouts can be exercised from the dev environment:

* a regular install (package tree in a ``site-packages``-style directory) with
  an unrelated ``src`` directory on the path — must stay quiet;
* a partial editable workspace (weevr-core's ``src/weevr`` alone) — must warn.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CORE_TREE = REPO_ROOT / "packages" / "weevr-core" / "src" / "weevr"
ENGINE_TREE = REPO_ROOT / "packages" / "weevr" / "src" / "weevr"

_SCRIPT = """
import sys, warnings
sys.path[:] = [p for p in sys.path if p.rstrip("/").rsplit("/", 1)[-1] != "src"]
sys.path[0:0] = {extra!r}
warnings.filterwarnings("error", message="weevr federated namespace")
{imports}
"""


def _import(extra: list[Path], imports: str) -> subprocess.CompletedProcess[str]:
    script = _SCRIPT.format(extra=[str(p) for p in extra], imports=imports)
    return subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )


def _unrelated_src(root: Path) -> Path:
    src = root / "unrelated" / "src"
    (src / "otherpkg").mkdir(parents=True)
    (src / "otherpkg" / "__init__.py").write_text("")
    return src


def _copy_tree(tree: Path, dest: Path) -> None:
    shutil.copytree(tree, dest, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))


def test_core_install_quiet_with_unrelated_src(tmp_path: Path) -> None:
    site = tmp_path / "site-packages"
    _copy_tree(CORE_TREE, site / "weevr")
    result = _import([site, _unrelated_src(tmp_path)], "import weevr.config")
    assert result.returncode == 0, result.stderr


def test_full_install_quiet_with_unrelated_src(tmp_path: Path) -> None:
    site = tmp_path / "site-packages"
    _copy_tree(CORE_TREE, site / "weevr")
    _copy_tree(ENGINE_TREE, site / "weevr")
    result = _import(
        [site, _unrelated_src(tmp_path)],
        "import weevr, weevr.config, weevr.engine, weevr.telemetry",
    )
    assert result.returncode == 0, result.stderr


def test_partial_workspace_still_warns(tmp_path: Path) -> None:
    src = tmp_path / "workspace" / "src"
    _copy_tree(CORE_TREE, src / "weevr")
    result = _import([src], "import weevr.config")
    assert result.returncode != 0
    assert "no sibling contributor" in result.stderr
