"""Dual-install smoke for the federated weevr / weevr-core wheels.

Builds both wheels via ``uv build --all-packages``, installs them into
two isolated venvs, and exercises the documented public import paths in
each mode:

* ``weevr-core`` alone — the validation-API persona used by tooling
  (weevr-cli, docs generation) that wants the Pydantic models, the
  config resolver, the planner graph, and the telemetry contracts
  without paying for PySpark. Submodule paths
  (``from weevr.config import load_config``, ``from weevr.model.thread
  import Thread``, etc.) must resolve. Top-level lazy imports
  (``from weevr import Context``) MUST NOT — the engine wheel is not
  installed and the namespace package has no ``weevr/__init__.py``.
  Crucially, no validator or model module may pull in PySpark
  transitively; if it does, the carve-out is leaking and the
  weevr-cli unblock regresses.

* ``weevr`` (full) — the existing ``pip install weevr`` UX without
  PySpark in the venv (PySpark is a runtime requirement of the engine
  but not a declared install dependency, matching the user-facing
  install model). The full install must still expose every pure
  validation path and the lazy ``Context`` accessor as a name
  (resolution to ``weevr.context.Context`` is deferred until accessed).

The test is gated by the ``install_smoke`` marker so the slow venv
churn does not run in the default suite. CI's release workflow runs
``pytest -m install_smoke`` as a pre-publish gate.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Pure-Python imports that must work in BOTH install modes. These touch
# only weevr-core's surface; nothing in this list runs through any
# Spark-bound __init__.py.
SHARED_IMPORTS = [
    "from weevr.config import load_config",
    "from weevr.model.thread import Thread",
    "from weevr.engine.planner import build_plan",
    "from weevr.engine.formatting import format_duration",
    "from weevr.telemetry.results import ThreadTelemetry",
    "from weevr.reserved_words import resolve_effective_words",
]

# In core-only mode there is no top-level weevr/__init__.py (engine
# wheel ships it), so the lazy convenience accessors are unavailable.
CORE_ONLY_REJECTS = [
    "from weevr import Context",
]

# Full-install adds the lazy convenience accessors back. Touching the
# *name* must succeed (Phase A's PEP 562 stub holds the import lazy);
# accessing the attribute would resolve weevr.context which pulls in
# PySpark, so we deliberately stop at hasattr to keep the test
# self-contained without installing PySpark.
FULL_INSTALL_LAZY_NAMES = ["Context", "RunResult", "ExecutionMode", "LoadedConfig"]
FULL_INSTALL_PROBE = (
    "import weevr; "
    "missing = [n for n in " + repr(FULL_INSTALL_LAZY_NAMES) + " if n not in weevr.__all__]; "
    "assert not missing, f'missing from __all__: {missing}'"
)


def _build_wheels(dist_dir: Path) -> tuple[Path, Path]:
    """Build both wheels into dist_dir; return (engine_wheel, core_wheel)."""
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    dist_dir.mkdir(parents=True)
    subprocess.run(
        ["uv", "build", "--all-packages", "-o", str(dist_dir)],
        cwd=REPO_ROOT,
        check=True,
    )
    engine = next(dist_dir.glob("weevr-*.whl"))
    core = next(dist_dir.glob("weevr_core-*.whl"))
    return engine, core


def _make_venv(venv_dir: Path) -> Path:
    """Create a Python 3.11 venv at venv_dir; return its python interpreter."""
    subprocess.run(
        ["uv", "venv", "--python", "3.11", str(venv_dir)],
        check=True,
    )
    return venv_dir / "bin" / "python"


def _pip_install(python: Path, *args: str) -> None:
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(python.parent.parent)
    subprocess.run(
        ["uv", "pip", "install", "--python", str(python), *args],
        check=True,
        env=env,
    )


def _run_imports(python: Path, statements: list[str], *, expect_ok: bool) -> None:
    for stmt in statements:
        result = subprocess.run(
            [str(python), "-c", stmt],
            capture_output=True,
            text=True,
        )
        if expect_ok:
            assert result.returncode == 0, (
                f"Expected import to succeed: {stmt!r}\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        else:
            assert result.returncode != 0, (
                f"Expected import to FAIL but it succeeded: {stmt!r}\nstdout: {result.stdout}"
            )


@pytest.fixture(scope="module")
def wheels(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    # When WEEVR_SMOKE_DIST is set (the release workflow exports the
    # build job's dist/ directory), reuse those wheels so the smoke
    # test exercises the exact artifacts the publish step will upload.
    # Otherwise build a fresh pair into a temp directory.
    prebuilt = os.environ.get("WEEVR_SMOKE_DIST")
    if prebuilt:
        dist = Path(prebuilt).resolve()
        if not dist.is_dir():
            raise RuntimeError(
                f"WEEVR_SMOKE_DIST={prebuilt!r} does not point to an existing directory"
            )
        engine = next(dist.glob("weevr-*.whl"))
        core = next(dist.glob("weevr_core-*.whl"))
        return engine, core
    dist = tmp_path_factory.mktemp("dist")
    return _build_wheels(dist)


@pytest.mark.install_smoke
def test_core_only_install(
    wheels: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    """``pip install weevr-core`` exposes every pure-Python validation path."""
    _, core = wheels
    venv = tmp_path / "core_only"
    python = _make_venv(venv)
    _pip_install(python, str(core))

    _run_imports(python, SHARED_IMPORTS, expect_ok=True)
    _run_imports(python, CORE_ONLY_REJECTS, expect_ok=False)


@pytest.mark.install_smoke
def test_core_only_install_does_not_pull_pyspark(
    wheels: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    """No core-only import path may transitively load PySpark."""
    _, core = wheels
    venv = tmp_path / "core_only_no_pyspark"
    python = _make_venv(venv)
    _pip_install(python, str(core))

    statements = [
        "import weevr.model",
        "import weevr.config",
        "from weevr.config import load_config",
        "import sys; assert 'pyspark' not in sys.modules",
    ]
    _run_imports(python, statements, expect_ok=True)


@pytest.mark.install_smoke
def test_full_install(
    wheels: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    """``pip install weevr`` (engine + core + PySpark) keeps every pure path."""
    engine, core = wheels
    venv = tmp_path / "full"
    python = _make_venv(venv)
    # Install both wheels at once so pip resolves the engine's pinned
    # weevr-core dependency to the local core wheel rather than PyPI.
    # PySpark is not a declared install dependency of weevr — Microsoft
    # Fabric provides it at runtime — but exercising weevr.engine.*
    # eagerly-imported submodules requires it, so install it here.
    _pip_install(
        python,
        str(engine),
        str(core),
        "pyspark>=3.5.0,<3.6.0",
    )

    _run_imports(python, SHARED_IMPORTS, expect_ok=True)
    _run_imports(python, [FULL_INSTALL_PROBE], expect_ok=True)
