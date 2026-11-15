"""Pinned output of the config load across every entry point and entry form.

The corpus is every example config, the ``load_parity`` fixture project, and
the ``forms`` project (each entry form at each scope: inline, file-referenced
and convention weaves; inline, file-referenced and ``as``/``params`` template
threads; ``foreach``; defaults; lookups, column sets and variables declared at
every level with shadowing; dependencies and conditions on entries).

Each config is loaded through ``Context.load``, ``load_resolved`` and
``load_config``. The models, children, execution order and warnings match
committed expectations, and the three entry points agree. Set
``WEEVR_UPDATE_GOLDEN=1`` to rewrite the expectation files after an intended
change.
"""

from __future__ import annotations

import ast
import inspect
import json
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from validate_examples_core import CONFIG_FILES, EXAMPLES_DIR, FABCON_DIR, REQUIRED_PARAMS

from weevr.config import load_config, load_resolved
from weevr.context import Context
from weevr.result import LoadedConfig

FIXTURES = Path(__file__).parent / "fixtures" / "load_parity"
FIXTURE_PROJECT = FIXTURES / "parity.weevr"
FORMS_PROJECT = FIXTURES / "forms.weevr"
GOLDEN_DIR = FIXTURES / "expected"

PARITY_CONFIGS = ["nightly.loom", "weaves/sales.weave", "threads/stg_orders.thread"]
FORMS_CONFIGS = [
    "forms.loom",
    "conv.weave",
    "pair_file.weave",
    "pair_inline.weave",
    "threads/base.thread",
]

# Fields derived from where a thread was written rather than what it says.
LOCATION_FIELDS = ("qualified_key", "template_ref")


@dataclass(frozen=True)
class Case:
    """One corpus config: the project it lives in and how to load it."""

    project: Path
    config: str
    params: dict[str, Any] | None = None

    @property
    def id(self) -> str:
        return f"{self.project.name}/{self.config}"

    @property
    def golden(self) -> Path:
        if self.project == FIXTURE_PROJECT:
            # The original parity goldens keep their names.
            return GOLDEN_DIR / (self.config.replace("/", "__") + ".json")
        name = self.project.name.removesuffix(".weevr")
        return GOLDEN_DIR / name / (self.config.replace("/", "__") + ".json")


def _example_case(path: Path) -> Case:
    project = FABCON_DIR if FABCON_DIR in path.parents else EXAMPLES_DIR
    rel = path.relative_to(EXAMPLES_DIR).as_posix()
    return Case(project, path.relative_to(project).as_posix(), REQUIRED_PARAMS.get(rel))


CORPUS = [
    *(Case(FIXTURE_PROJECT, c) for c in PARITY_CONFIGS),
    *(Case(FORMS_PROJECT, c) for c in FORMS_CONFIGS),
    *(_example_case(p) for p in CONFIG_FILES),
]


def _copy_project(source: Path, tmp_path: Path) -> Path:
    """Copy a project into a ``.weevr`` directory so resolved paths are stable."""
    name = source.name if source.name.endswith(".weevr") else f"{source.name}.weevr"
    target = tmp_path / name
    shutil.copytree(source, target)
    return target


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Copy the fixture project so resolved paths are stable per test."""
    return _copy_project(FIXTURE_PROJECT, tmp_path)


def _mask(data: Any, root: Path) -> Any:
    return json.loads(json.dumps(data).replace(str(root), "<root>"))


def _snapshot(loaded: Any, root: Path) -> dict[str, Any]:
    """Serialize a loaded config and its children with the project root masked."""
    data = {
        "config_type": loaded.config_type,
        "config_name": loaded.config_name,
        "model": loaded.model.model_dump(mode="json"),
        "weaves": {
            name: weave.model_dump(mode="json") for name, weave in loaded.resolved_weaves.items()
        },
        "threads": {
            weave: {name: thread.model_dump(mode="json") for name, thread in threads.items()}
            for weave, threads in loaded.resolved_threads.items()
        },
        "execution_order": [plan.execution_order for plan in loaded.execution_plan or []],
    }
    return _mask(data, root)


def _write_or_compare(path: Path, actual: dict[str, Any]) -> None:
    if os.environ.get("WEEVR_UPDATE_GOLDEN"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(actual, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    assert actual == json.loads(path.read_text())


def _context(root: Path, params: dict[str, Any] | None = None) -> Context:
    return Context(spark=MagicMock(spec=SparkSession), project=str(root), params=params)


class TestCorpusAcrossEntryPoints:
    """Every corpus config loads the same through all three entry points."""

    @pytest.mark.parametrize("case", CORPUS, ids=lambda c: c.id)
    def test_context_load_matches_golden(self, case: Case, tmp_path: Path) -> None:
        root = _copy_project(case.project, tmp_path)
        loaded = _context(root, case.params).load(case.config)
        snapshot = _snapshot(loaded, root)
        snapshot["warnings"] = _mask(list(loaded.warnings), root)
        _write_or_compare(case.golden, snapshot)

    @pytest.mark.parametrize("case", CORPUS, ids=lambda c: c.id)
    def test_load_resolved_matches_context(self, case: Case, tmp_path: Path) -> None:
        root = _copy_project(case.project, tmp_path)
        via_context = _context(root, case.params).load(case.config)
        direct = load_resolved(root / case.config, runtime_params=case.params, project_root=root)
        assert _snapshot(direct, root) == _snapshot(via_context, root)
        assert direct.warnings == via_context.warnings

    @pytest.mark.parametrize("case", CORPUS, ids=lambda c: c.id)
    def test_load_config_matches_load_resolved(
        self, case: Case, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        root = _copy_project(case.project, tmp_path)
        resolved = load_resolved(root / case.config, runtime_params=case.params, project_root=root)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="weevr.config"):
            model = load_config(root / case.config, runtime_params=case.params, project_root=root)
        assert not isinstance(model, dict)
        assert model.model_dump(mode="json") == resolved.model.model_dump(mode="json")
        logged = [r.getMessage() for r in caplog.records if r.name == "weevr.config"]
        assert logged == list(resolved.warnings)


class TestInlineMatchesFile:
    """A thread written inline hydrates like the same thread in its own file."""

    def test_inline_and_file_threads_match(self, tmp_path: Path) -> None:
        root = _copy_project(FORMS_PROJECT, tmp_path)
        by_file = load_resolved(root / "pair_file.weave", project_root=root)
        inline = load_resolved(root / "pair_inline.weave", project_root=root)

        def body(loaded: Any, weave: str) -> dict[str, Any]:
            dumped = loaded.resolved_threads[weave]["pair"].model_dump(mode="json")
            for field in LOCATION_FIELDS:
                dumped.pop(field)
            return dumped

        assert body(by_file, "pair_file") == body(inline, "pair_inline")

    def test_location_fields_differ(self, tmp_path: Path) -> None:
        """The excluded fields really are location-derived, so excluding them hides nothing else."""
        root = _copy_project(FORMS_PROJECT, tmp_path)
        by_file = load_resolved(root / "pair_file.weave", project_root=root)
        inline = load_resolved(root / "pair_inline.weave", project_root=root)
        file_thread = by_file.resolved_threads["pair_file"]["pair"]
        inline_thread = inline.resolved_threads["pair_inline"]["pair"]
        assert file_thread.qualified_key == "threads/pair.thread"
        assert inline_thread.qualified_key == "pair"
        assert file_thread.template_ref == "threads/pair.thread"
        assert inline_thread.template_ref is None


class TestContextLoadParity:
    """``Context.load`` warnings for the parity project keep their wording."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            (
                "nightly.loom",
                [
                    "WARN: loom '(unnamed)' declares defaults.execution (log_level)",
                    "WARN: thread 'stg_orders' declares an execution block (log_level)",
                ],
            ),
            (
                "weaves/sales.weave",
                ["WARN: thread 'stg_orders' declares an execution block (log_level)"],
            ),
            (
                "threads/stg_orders.thread",
                ["WARN: thread '#0' declares an execution block (log_level)"],
            ),
        ],
    )
    def test_load_warnings(
        self,
        project: Path,
        config: str,
        expected: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        with caplog.at_level(logging.WARNING, logger="weevr.context"):
            ctx.load(config)
        warnings = [r.message for r in caplog.records if r.message.startswith("WARN:")]
        assert [w.split(" — ")[0] for w in warnings] == expected


_CYCLE_THREAD = """\
config_version: "1.0"
sources:
  data:
    type: delta
    alias: raw.{name}
target:
  alias: out.{name}
"""

_CYCLE_WEAVE = """\
config_version: "1.0"
threads:
  - ref: a.thread
    dependencies: [b]
  - ref: b.thread
    dependencies: [a]
"""


_CONFLICT_WEAVE = """\
config_version: "1.0"
threads:
  - ref: c.thread
  - ref: d.thread
"""

_CONFLICT_THREAD = """\
config_version: "1.0"
sources:
  data:
    type: delta
    alias: raw.{name}
target:
  alias: out.shared
"""


class TestValidateWording:
    """``Context.run(mode="validate")`` DAG messages keep their current wording."""

    @staticmethod
    def _project(tmp_path: Path) -> Path:
        project = tmp_path / "cycle.weevr"
        project.mkdir()
        for name in ("a", "b"):
            (project / f"{name}.thread").write_text(_CYCLE_THREAD.format(name=name))
        (project / "cyc.weave").write_text(_CYCLE_WEAVE)
        (project / "cyc.loom").write_text('config_version: "1.0"\nweaves:\n  - ref: cyc.weave\n')
        for name in ("c", "d"):
            (project / f"{name}.thread").write_text(_CONFLICT_THREAD.format(name=name))
        (project / "dup.weave").write_text(_CONFLICT_WEAVE)
        (project / "both.loom").write_text(
            'config_version: "1.0"\nweaves:\n  - ref: cyc.weave\n  - ref: dup.weave\n'
        )
        return project

    @staticmethod
    def _dag_errors(project: Path, config: str) -> list[str]:
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        result = ctx.run(config, mode="validate")
        return [e for e in result.validation_errors or [] if e.startswith("DAG validation")]

    def test_weave_cycle_wording(self, tmp_path: Path) -> None:
        errors = self._dag_errors(self._project(tmp_path), "cyc.weave")
        assert errors == [
            "DAG validation failed: Circular dependency detected in weave 'cyc': a → b → a"
        ]

    def test_loom_cycle_wording(self, tmp_path: Path) -> None:
        errors = self._dag_errors(self._project(tmp_path), "cyc.loom")
        assert errors == [
            "DAG validation failed for weave 'cyc': "
            "Circular dependency detected in weave 'cyc': a → b → a"
        ]

    def test_loom_reports_every_failing_weave(self, tmp_path: Path) -> None:
        errors = self._dag_errors(self._project(tmp_path), "both.loom")
        cycle = (
            "DAG validation failed for weave 'cyc': "
            "Circular dependency detected in weave 'cyc': a → b → a"
        )
        conflict = (
            "DAG validation failed for weave 'dup': "
            "Threads 'c', 'd' all write target 'out.shared' in weave 'dup' but are not ordered "
            "by explicit dependencies, so concurrent writes would corrupt the target. Declare "
            "explicit dependencies to serialize the writers, give each thread its own target, "
            "or put a mutually exclusive condition on every writer."
        )
        assert errors == [cycle, conflict]

    def test_missing_weave_wording(self, tmp_path: Path) -> None:
        # Config loading fails before a loom can lose a weave, so drop one by hand.
        project = self._project(tmp_path)
        full = load_resolved(project / "both.loom", project_root=project)
        loaded = LoadedConfig(
            model=full.model,
            config_type=full.config_type,
            config_name=full.config_name,
            threads=dict(full.resolved_threads),
            weaves={"cyc": full.resolved_weaves["cyc"]},
        )
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        result = ctx._run_validate(loaded)
        assert "Weave 'dup' not found" in (result.validation_errors or [])


_LOAD_STEPS = (
    "parse_yaml",
    "resolve_config_variables",
    "resolve_variables",
    "resolve_references",
    "apply_inheritance",
    "expand_foreach",
    "hydrate_children",
    "hydrate_model",
)


def _context_module_tree() -> ast.Module:
    source_file = inspect.getsourcefile(Context)
    assert source_file is not None
    return ast.parse(Path(source_file).read_text())


def test_context_has_no_load_steps_of_its_own() -> None:
    """Context loads only through the shared loader — no step of its own."""
    tree = _context_module_tree()
    called = {
        node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name | ast.Attribute)
    }
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    defined = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.ClassDef)
    }
    assert called & set(_LOAD_STEPS) == set()
    assert (names | defined) & {"_ResolvedConfig", "_resolve_and_hydrate"} == set()


def test_context_reads_children_through_resolved_properties() -> None:
    """Context reads children via ``resolved_*``; ``.threads`` / ``.weaves`` proxy to the model."""
    reads = [
        node.attr
        for node in ast.walk(_context_module_tree())
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "loaded"
        and node.attr in ("threads", "weaves")
    ]
    assert reads == []
