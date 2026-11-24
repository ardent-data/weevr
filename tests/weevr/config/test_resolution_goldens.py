"""Resolved-config goldens for every config that uses only namespaced references.

Characterizes today's resolution so later changes to the param/variable binding
model prove they resolve the same configs identically. The corpus is every
``.thread`` / ``.weave`` / ``.loom`` under ``examples/`` and the ``load_parity``
fixture projects, minus any file holding a bare ``${name}`` reference (a
``${...}`` whose first segment is not ``param``, ``fabric``, ``var``, ``run``,
``thread``, ``weave`` or ``loom``): those change by design.

Each golden is the hydrated models' ``model_dump(mode="json", exclude_none=True,
exclude_defaults=True)`` for the loaded config, its weaves and its threads, with
sorted keys. Dropping ``None`` and default-valued fields keeps a golden stable when
a later change adds an optional model field. Absolute paths are masked as
``<root>`` (project) and ``<repo>`` (checkout).

Set ``WEEVR_UPDATE_GOLDENS=1`` to rewrite the goldens after an intended change;
a plain run only compares, and a missing golden fails.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from validate_examples_core import CONFIG_FILES, EXAMPLES_DIR, REQUIRED_PARAMS, project_root

from documents import write_document
from weevr.config import load_resolved
from weevr.errors import ConfigError

REPO = Path(__file__).resolve().parents[3]
GOLDEN_DIR = Path(__file__).parent / "goldens"
PARITY_ROOT = REPO / "tests" / "weevr" / "fixtures" / "load_parity"
PARITY_PROJECTS = [PARITY_ROOT / "parity.weevr", PARITY_ROOT / "forms.weevr"]

NAMESPACES = ("param", "fabric", "var", "run", "thread", "weave", "loom")
BARE_REFERENCE = re.compile(r"\$\{(?!(?:" + "|".join(NAMESPACES) + r")\.)[^}]*\}")

# Corpus configs that cannot load core-only, with the reason. Empty today.
SKIPPED: dict[str, str] = {}

DUMP = {"mode": "json", "exclude_none": True, "exclude_defaults": True}


@dataclass(frozen=True)
class Case:
    """One corpus config: the project it lives in and the file to load."""

    project: Path
    path: Path
    params: dict[str, Any] | None
    label: str

    @property
    def golden(self) -> Path:
        return GOLDEN_DIR / (self.label.replace("/", "__") + ".json")


# The entry points the load_parity test loads. The other files in those projects are
# templates or children that only load through their parent (they lean on lookups a
# parent scope supplies), so they are covered through the weaves that reference them.
PARITY_CONFIGS = {
    "parity.weevr": ["nightly.loom", "weaves/sales.weave", "threads/stg_orders.thread"],
    "forms.weevr": [
        "forms.loom",
        "conv.weave",
        "pair_file.weave",
        "pair_inline.weave",
        "threads/base.thread",
    ],
}


def _parity_cases() -> list[Case]:
    cases = []
    for project in PARITY_PROJECTS:
        for rel in PARITY_CONFIGS[project.name]:
            cases.append(Case(project, project / rel, None, f"load_parity/{project.stem}/{rel}"))
    return cases


def _example_cases() -> list[Case]:
    cases = []
    for path in CONFIG_FILES:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        cases.append(Case(project_root(path), path, REQUIRED_PARAMS.get(rel), f"examples/{rel}"))
    return cases


def _is_namespaced_only(case: Case) -> bool:
    return not BARE_REFERENCE.search(case.path.read_text())


ALL_CASES = [*_parity_cases(), *_example_cases()]
CORPUS = [c for c in ALL_CASES if _is_namespaced_only(c)]
BARE_EXCLUDED = [c for c in ALL_CASES if not _is_namespaced_only(c)]


def _dump(model: Any) -> dict[str, Any]:
    return model.model_dump(**DUMP)


def _mask(data: Any, case: Case) -> Any:
    text = json.dumps(data)
    for prefix, token in ((case.project, "<root>"), (REPO, "<repo>")):
        for variant in {str(prefix), str(prefix.resolve())}:
            text = text.replace(variant, token)
    return json.loads(text)


def snapshot(case: Case) -> dict[str, Any]:
    """Load a corpus config and dump its resolved models, machine-independent."""
    loaded = load_resolved(case.path, runtime_params=case.params, project_root=case.project)
    document = loaded.document
    data = {
        "config_type": loaded.config_type,
        "config_name": loaded.config_name,
        "model": _dump(loaded.model),
        "weaves": {name: _dump(weave) for name, weave in document.weaves().items()},
        "threads": {
            weave: {name: _dump(thread) for name, thread in threads.items()}
            for weave, threads in document.weave_threads().items()
        },
    }
    return _mask(data, case)


def _serialize(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


class TestCorpus:
    def test_bare_reference_fixtures_are_the_only_exclusions(self) -> None:
        assert sorted(c.path.name for c in BARE_EXCLUDED) == ["forms.loom", "nightly.loom"]

    @pytest.mark.parametrize("case", CORPUS, ids=[c.label for c in CORPUS])
    def test_resolves_to_golden(self, case: Case) -> None:
        if case.label in SKIPPED:
            pytest.skip(SKIPPED[case.label])
        actual = _serialize(snapshot(case))
        if os.environ.get("WEEVR_UPDATE_GOLDENS") == "1":
            case.golden.parent.mkdir(parents=True, exist_ok=True)
            case.golden.write_text(actual)
        assert case.golden.exists(), f"missing golden {case.golden}; set WEEVR_UPDATE_GOLDENS=1"
        assert actual == case.golden.read_text(), (
            f"{case.golden.name} changed; if intended, rerun with WEEVR_UPDATE_GOLDENS=1"
        )

    def test_no_orphan_goldens(self) -> None:
        expected = {c.golden.name for c in CORPUS}
        orphans = sorted({p.name for p in GOLDEN_DIR.glob("*.json")} - expected)
        assert not orphans, f"orphan goldens, delete them: {orphans}"


THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}


def _thread(tmp_path: Path, params: dict[str, Any], **body: Any) -> Path:
    """Write a thread declaring ``params`` (name -> spec fields) plus ``body`` overrides."""
    declared = {name: {"name": name, **spec} for name, spec in params.items()}
    return write_document(tmp_path / "t.thread", json.dumps({**THREAD, "params": declared, **body}))


def _load(path: Path, runtime: dict[str, Any] | None = None) -> Any:
    return load_resolved(path, runtime_params=runtime, project_root=path.parent).model


def _code(path: Path, runtime: dict[str, Any] | None = None) -> str:
    with pytest.raises(ConfigError) as exc:
        _load(path, runtime)
    assert len(exc.value.diagnostics) == 1
    return exc.value.diagnostics[0].code


class TestExplicitNoneRuntimeValue:
    """An explicit ``None`` runtime value is absent: default or required check apply."""

    def test_declared_default_applies(self, tmp_path: Path) -> None:
        path = _thread(
            tmp_path,
            {"n": {"type": "string", "required": False, "default": "dflt"}},
            target={"alias": "out.${param.n}"},
        )
        assert _load(path, {"n": None}).target.alias == "out.dflt"

    def test_required_check_raises(self, tmp_path: Path) -> None:
        path = _thread(
            tmp_path,
            {"n": {"type": "string", "required": True}},
            target={"alias": "out.${param.n}"},
        )
        assert _code(path, {"n": None}) == "P101"


class TestOptionalParamWithoutDefault:
    """An optional param with no default is unbound: its reference fails unless ``:-``."""

    SPEC = {"n": {"type": "string", "required": False}}

    def test_reference_fails(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, self.SPEC, target={"alias": "out.${param.n}"})
        assert _code(path) == "P110"

    def test_fallback_applies(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, self.SPEC, target={"alias": "out.${param.n:-fb}"})
        assert _load(path).target.alias == "out.fb"

    def test_fallback_applies_for_explicit_none(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, self.SPEC, target={"alias": "out.${param.n:-fb}"})
        assert _load(path, {"n": None}).target.alias == "out.fb"


class TestWholeValueReferencesKeepNativeType:
    """A value that is only a reference resolves to the param's native type."""

    @pytest.mark.parametrize(
        ("ptype", "value"),
        [("int", 7), ("bool", True), ("float", 1.5), ("list[string]", ["a", "b"])],
    )
    def test_native_type(self, tmp_path: Path, ptype: str, value: Any) -> None:
        path = _thread(
            tmp_path,
            {"p": {"type": ptype, "required": True}},
            tags=None,
            target={"alias": "out.t", "mode": "overwrite"},
            sources={
                "s": {
                    "type": "delta",
                    "alias": "raw.t",
                    "options": {"probe": "${param.p}"},
                }
            },
        )
        source = _load(path, {"p": value}).sources["s"]
        resolved = source.options["probe"]
        assert resolved == value
        assert type(resolved) is type(value)
