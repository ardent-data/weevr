"""Every ``${...}`` reference names its namespace; bare and unknown ones fail at load.

The namespaces are ``param`` and ``fabric`` (resolved at load) and ``var``,
``run``, ``thread``, ``weave`` and ``loom`` (left in place for execution). Each
resolves on its own: a runtime param never answers ``${fabric.x}``-style
lookups of another namespace, and a ``defaults:`` scalar is not a variable.

A bare ``${x}`` fails ``P111`` with the rewritten reference in ``expected``:
``${param.x}`` for a param (declared, supplied, or neither), ``${param.x}``
with a declare-it hint for a ``defaults:`` key, ``${fabric.x}`` for a Fabric
key. A dotted ``${a.b}`` rewrites the same way by its first segment when
``a`` is declared, supplied or a ``defaults:`` key; otherwise ``a`` is an
unknown namespace. An unknown namespace, or an unknown key in a hierarchy
namespace, fails ``P112``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from documents import write_document
from weevr.config import load_resolved
from weevr.config.interpolation import NAMESPACES, resolve_variables
from weevr.config.loader import resolve_config
from weevr.config.locations import LocalConfigLocation
from weevr.diagnostics import Diagnostic
from weevr.errors import VariableResolutionError

SEVEN = ["param", "fabric", "var", "run", "thread", "weave", "loom"]


def _thread(alias: str, **extra: Any) -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "sources": {"s": {"type": "delta", "alias": "raw.t"}},
        "target": {"alias": alias},
        **extra,
    }


def _specs(**defaults: str | None) -> dict[str, Any]:
    return {
        name: {"name": name, "type": "string", "required": False, "default": default}
        for name, default in defaults.items()
    }


def _load(tmp_path: Path, body: dict[str, Any], runtime: dict[str, Any] | None = None) -> Any:
    path = write_document(tmp_path / "t.thread", body)
    return load_resolved(path, runtime_params=runtime, project_root=tmp_path)


def _failure(tmp_path: Path, body: dict[str, Any], runtime: dict[str, Any] | None = None):
    with pytest.raises(VariableResolutionError) as exc:
        _load(tmp_path, body, runtime)
    assert len(exc.value.diagnostics) == 1
    return exc.value.diagnostics[0]


def _rewrite(found: Diagnostic) -> str:
    assert found.code == "P111"
    assert found.expected is not None and len(found.expected) == 1
    return found.expected[0]


class TestNamespaceSet:
    def test_exactly_seven(self) -> None:
        assert list(NAMESPACES) == SEVEN


class TestBareReference:
    """Rules 1-4: a bare ``${x}`` fails ``P111`` with its rewrite."""

    def test_rule1_declared_param(self, tmp_path: Path) -> None:
        body = _thread("out.${x}", params=_specs(x="a"))
        assert _rewrite(_failure(tmp_path, body)) == "${param.x}"

    def test_rule1_declared_wins_over_fabric_key(self, tmp_path: Path) -> None:
        body = _thread("out.${workspace_id}", params=_specs(workspace_id="w"))
        assert _rewrite(_failure(tmp_path, body)) == "${param.workspace_id}"

    def test_rule2_defaults_scalar(self, tmp_path: Path) -> None:
        body = _thread("out.${region}", defaults={"region": "eu"})
        found = _failure(tmp_path, body)
        assert _rewrite(found) == "${param.region}"
        assert found.hint is not None and "${param.region}" in found.hint

    def test_rule2_defaults_scalar_is_not_a_variable(self, tmp_path: Path) -> None:
        body = _thread("out.${param.region}", defaults={"region": "eu"})
        assert _failure(tmp_path, body).code == "P110"

    @pytest.mark.parametrize("key", ["workspace_id", "lakehouse_id", "workspace_name"])
    def test_rule3_fabric_key(self, tmp_path: Path, key: str) -> None:
        body = _thread(f"out.${{{key}}}")
        assert _rewrite(_failure(tmp_path, body)) == f"${{fabric.{key}}}"

    def test_rule4_otherwise_param(self, tmp_path: Path) -> None:
        assert _rewrite(_failure(tmp_path, _thread("out.${anything}"))) == "${param.anything}"

    def test_rule4_supplied_runtime_param(self, tmp_path: Path) -> None:
        found = _failure(tmp_path, _thread("out.${env}"), {"env": "dev"})
        assert _rewrite(found) == "${param.env}"

    def test_keeps_fallback(self, tmp_path: Path) -> None:
        found = _failure(tmp_path, _thread("out.${env:-dev}"))
        assert _rewrite(found) == "${param.env:-dev}"

    def test_whole_value_reference(self, tmp_path: Path) -> None:
        assert _rewrite(_failure(tmp_path, _thread("${env}"))) == "${param.env}"

    def test_points_at_the_value(self, tmp_path: Path) -> None:
        found = _failure(tmp_path, _thread("out.${env}"))
        assert found.pointer == "/target/alias"


class TestDottedBareReference:
    """Rule 5: ``${a.b}`` with ``a`` not a namespace rewrites by its first segment."""

    def test_supplied_first_segment(self, tmp_path: Path) -> None:
        runtime = {"env": {"lakehouse": "lh"}}
        found = _failure(tmp_path, _thread("out.${env.lakehouse}"), runtime)
        assert _rewrite(found) == "${param.env.lakehouse}"

    def test_declared_first_segment(self, tmp_path: Path) -> None:
        body = _thread("out.${env.lakehouse:-x}", params=_specs(env=None))
        assert _rewrite(_failure(tmp_path, body)) == "${param.env.lakehouse:-x}"

    def test_defaults_first_segment(self, tmp_path: Path) -> None:
        body = _thread("out.${env.lakehouse}", defaults={"env": {"lakehouse": "lh"}})
        assert _rewrite(_failure(tmp_path, body)) == "${param.env.lakehouse}"

    def test_unknown_first_segment_is_an_unknown_namespace(self, tmp_path: Path) -> None:
        found = _failure(tmp_path, _thread("out.${env.lakehouse}"))
        assert found.code == "P112"
        assert list(found.expected or []) == SEVEN


class TestUnknownNamespace:
    @pytest.mark.parametrize("ref", ["${params.x}", "${prm.x}", "${params.x:-d}"])
    def test_fails_listing_the_namespaces(self, tmp_path: Path, ref: str) -> None:
        found = _failure(tmp_path, _thread(f"out.{ref}"))
        assert found.code == "P112"
        assert list(found.expected or []) == SEVEN

    @pytest.mark.parametrize("ref", ["${thread.nme}", "${weave.nmae}", "${loom.id}"])
    def test_unknown_hierarchy_key(self, tmp_path: Path, ref: str) -> None:
        found = _failure(tmp_path, _thread(f"out.{ref}"))
        assert found.code == "P112"
        assert found.pointer == "/target/alias"

    def test_unknown_hierarchy_key_lists_the_known_ones(self, tmp_path: Path) -> None:
        found = _failure(tmp_path, _thread("out.${thread.nme}"))
        assert "${thread.name}" in (found.expected or [])

    @pytest.mark.parametrize(
        "ref", ["${thread.name}", "${weave.name}", "${loom.name}", "${var.x}", "${run.id}"]
    )
    def test_execution_time_references_stay_in_place(self, tmp_path: Path, ref: str) -> None:
        audit = {"c": f"'{ref}'"}
        body = {**_thread("out.t"), "target": {"alias": "out.t", "audit_columns": audit}}
        assert _load(tmp_path, body).model.target.audit_columns == audit


class TestIndependentNamespaces:
    def test_param_traverses_into_its_value(self, tmp_path: Path) -> None:
        runtime = {"env": {"lakehouse": "lh"}}
        loaded = _load(tmp_path, _thread("out.${param.env.lakehouse}"), runtime)
        assert loaded.model.target.alias == "out.lh"

    def test_runtime_param_does_not_answer_a_hierarchy_name(self, tmp_path: Path) -> None:
        runtime = {"thread": {"nme": "x"}}
        assert _failure(tmp_path, _thread("out.${thread.nme}"), runtime).code == "P112"

    def test_fabric_reads_only_the_fabric_context(self, tmp_path: Path) -> None:
        path = write_document(tmp_path / "t.thread", _thread("out.${fabric.workspace_id}"))
        resolved = resolve_config(
            LocalConfigLocation(path),
            runtime_params={"workspace_id": "from-param"},
            project_root=LocalConfigLocation(tmp_path),
            fabric_context={"fabric.workspace_id": "ws"},
        )
        assert resolved.data["target"]["alias"] == "out.ws"


class TestNestedDocuments:
    def test_bare_reference_in_a_ref_thread_fails(self, tmp_path: Path) -> None:
        write_document(tmp_path / "t.thread", _thread("out.${x}"))
        weave = write_document(
            tmp_path / "w.weave",
            {"config_version": "2.0", "threads": [{"ref": "t.thread", "params": {"x": "a"}}]},
        )
        with pytest.raises(VariableResolutionError) as exc:
            load_resolved(weave, project_root=tmp_path)
        assert [d.code for d in exc.value.diagnostics] == ["P111"]
        assert list(exc.value.diagnostics[0].expected or []) == ["${param.x}"]

    def test_bare_pin_value_fails(self, tmp_path: Path) -> None:
        write_document(tmp_path / "t.thread", _thread("out.${param.x}"))
        weave = write_document(
            tmp_path / "w.weave",
            {
                "config_version": "2.0",
                "threads": [{"ref": "t.thread", "params": {"x": "${region}"}}],
            },
        )
        with pytest.raises(VariableResolutionError) as exc:
            load_resolved(weave, runtime_params={"region": "eu"}, project_root=tmp_path)
        assert [d.code for d in exc.value.diagnostics] == ["P111"]


class TestSeveralReferencesInOneString:
    """A string holding several references is interpolated, never one whole value."""

    CONTEXT: dict[str, Any] = {"param": {"a": 1, "b": 2, "n": 7}}

    def test_fallback_then_another_reference(self) -> None:
        result = resolve_variables("${param.a:-x} ${param.b}", self.CONTEXT)
        assert result == "1 2"

    def test_fallback_then_dotted_reference(self) -> None:
        context = {"param": {"a": 1, "b": {"c": 2}}}
        assert resolve_variables("${param.a:-x} and ${param.b.c}", context) == "1 and 2"

    def test_fallback_then_bare_reference_fails(self) -> None:
        with pytest.raises(VariableResolutionError) as exc:
            resolve_variables("${param.a:-x} ${bare}", self.CONTEXT)
        assert [d.code for d in exc.value.diagnostics] == ["P111"]

    def test_hierarchy_fallback_then_param(self) -> None:
        assert resolve_variables("${thread.name:-d} ${param.a}", self.CONTEXT) == "d 1"

    def test_whole_value_with_fallback_keeps_native_type(self) -> None:
        assert resolve_variables("${param.n:-5}", self.CONTEXT) == 7


class TestNamespaceWithoutKey:
    """A reference that is only a namespace name fails ``P112`` with the namespace's form."""

    @pytest.mark.parametrize("namespace", ["param", "fabric", "var", "run"])
    def test_names_the_expected_form(self, namespace: str) -> None:
        with pytest.raises(VariableResolutionError) as exc:
            resolve_variables(f"out.${{{namespace}}}", {"param": {"param": "x"}})
        (found,) = exc.value.diagnostics
        assert found.code == "P112"
        assert list(found.expected or []) == [f"${{{namespace}.<name>}}"]

    def test_hierarchy_namespace_lists_its_keys(self) -> None:
        with pytest.raises(VariableResolutionError) as exc:
            resolve_variables("${thread}", {})
        (found,) = exc.value.diagnostics
        assert found.code == "P112"
        assert "${thread.name}" in (found.expected or [])
