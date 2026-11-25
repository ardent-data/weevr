"""``${fabric.x}`` references in core loads and under ``Context``.

The core loaders resolve ``${fabric.x}`` from the ``fabric_context`` they are
given. Given none, a reference is left in place with warning ``P113`` (an
error under ``strict_params``). ``Context`` always supplies its session's
values, so a key the session lacks fails ``P110`` there. An unknown fabric
key fails ``P112`` in every mode, and runtime params never supply the
``fabric`` namespace.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from documents import thread_body, weave_body, write_named, write_project
from resolving import collected_at, load_at, load_failure, with_code
from weevr.config import load_config
from weevr.context import Context
from weevr.diagnostics import Severity
from weevr.errors import ConfigError

FABRIC = {"workspace_id": "ws-1", "lakehouse_id": "lh-1", "workspace_name": "sales"}
SESSION_CONF = {
    "trident.workspace.id": "ws-1",
    "trident.lakehouse.id": "lh-1",
    "trident.workspace.name": "sales",
}
ALL_KEYS = "${fabric.workspace_id}.${fabric.lakehouse_id}.${fabric.workspace_name}"


def _alias(loaded: Any, *names: str) -> str:
    document = loaded.document
    for name in names:
        document = document.children[name]
    return document.model.target.alias


class TestCoreLoadWithFabricContext:
    """``fabric_context=`` supplies the three keys, in the root and in ref'd children."""

    def test_load_resolved_resolves_every_key(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(ALL_KEYS))
        loaded = load_at(path, fabric_context=FABRIC)
        assert _alias(loaded) == "ws-1.lh-1.sales"
        assert with_code(loaded.diagnostics, "P113") == []

    def test_load_config_resolves_every_key(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(ALL_KEYS))
        thread = load_config(path, project_root=tmp_path, fabric_context=FABRIC)
        assert thread.target.alias == "ws-1.lh-1.sales"  # type: ignore[union-attr]

    def test_prefixed_keys_are_accepted(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        loaded = load_at(path, fabric_context={"fabric.workspace_id": "ws-2"})
        assert _alias(loaded) == "out.ws-2"

    def test_a_refd_child_sees_the_context(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        loaded = load_at(path, fabric_context=FABRIC)
        assert _alias(loaded, "t") == "out.ws-1"

    def test_a_key_the_context_lacks_fails_p110(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.lakehouse_id}"))
        error = load_failure(path, fabric_context={"workspace_id": "ws-1"})
        assert [(d.code, d.pointer) for d in error.diagnostics] == [("P110", "/target/alias")]

    def test_an_empty_context_is_a_context(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        error = load_failure(path, fabric_context={})
        assert [d.code for d in error.diagnostics] == ["P110"]

    def test_a_none_value_has_no_value(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        error = load_failure(path, fabric_context={"workspace_id": None})
        assert [d.code for d in error.diagnostics] == ["P110"]

    def test_a_default_covers_a_missing_key(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.lakehouse_id:-lh}"))
        assert _alias(load_at(path, fabric_context={})) == "out.lh"


class TestCoreLoadWithoutFabricContext:
    """With no ``fabric_context``, each reference stays in place and warns ``P113``."""

    def test_embedded_reference_is_left_in_place(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        loaded = load_at(path)
        assert _alias(loaded) == "out.${fabric.workspace_id}"
        (found,) = with_code(loaded.diagnostics, "P113")
        assert (found.severity, found.file, found.pointer) == (
            Severity.WARNING,
            str(path),
            "/target/alias",
        )
        assert list(found.expected or []) == ["fabric_context"]

    def test_whole_value_reference_is_left_in_place(self, tmp_path: Path) -> None:
        path = write_named(
            tmp_path, "t.thread", thread_body("out.t", tags=["${fabric.workspace_name}"])
        )
        loaded = load_at(path)
        assert loaded.model.tags == ["${fabric.workspace_name}"]
        assert [d.pointer for d in with_code(loaded.diagnostics, "P113")] == ["/tags/0"]

    def test_one_warning_per_reference(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(ALL_KEYS))
        loaded = load_at(path)
        assert _alias(loaded) == ALL_KEYS
        assert [d.pointer for d in with_code(loaded.diagnostics, "P113")] == ["/target/alias"] * 3

    def test_a_refd_child_reference_names_its_file(self, tmp_path: Path) -> None:
        child = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        loaded = load_at(path)
        assert _alias(loaded, "t") == "out.${fabric.workspace_id}"
        (found,) = with_code(loaded.diagnostics, "P113")
        assert (found.file, found.pointer) == (str(child), "/target/alias")

    def test_a_default_is_used(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id:-local}"))
        loaded = load_at(path)
        assert _alias(loaded) == "out.local"
        assert with_code(loaded.diagnostics, "P113") == []

    def test_load_config_defers_too(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        thread = load_config(path, project_root=tmp_path)
        assert thread.target.alias == "out.${fabric.workspace_id}"  # type: ignore[union-attr]


class TestStrictDeferral:
    """Under ``strict_params`` a deferred reference is an error, same code."""

    def test_strict_document_fails_the_load(self, tmp_path: Path) -> None:
        path = write_named(
            tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}", strict_params=True)
        )
        error = load_failure(path)
        (found,) = error.diagnostics
        assert (found.code, found.is_error, found.pointer) == ("P113", True, "/target/alias")

    def test_api_override_fails_the_load(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        assert [d.code for d in load_failure(path, strict_params=True).diagnostics] == ["P113"]

    def test_api_override_relaxes_a_strict_document(self, tmp_path: Path) -> None:
        path = write_named(
            tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}", strict_params=True)
        )
        (found,) = with_code(load_at(path, strict_params=False).diagnostics, "P113")
        assert not found.is_error

    def test_strictness_is_the_referencing_scopes(self, tmp_path: Path) -> None:
        write_named(
            tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}", strict_params=True)
        )
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        assert [d.code for d in load_failure(path).diagnostics] == ["P113"]

    def test_validate_records_the_error(self, tmp_path: Path) -> None:
        path = write_named(
            tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}", strict_params=True)
        )
        document, warnings, errors = collected_at(path)
        assert document is not None
        assert [(d.code, d.is_error) for d in errors] == [("P113", True)]
        assert with_code(warnings, "P113") == []


class TestUnknownFabricKey:
    """An unknown key fails ``P112`` whether or not a context is supplied."""

    @pytest.mark.parametrize("fabric_context", [None, FABRIC], ids=["no-context", "context"])
    def test_unknown_key_fails(self, tmp_path: Path, fabric_context: Any) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspce_id}"))
        error = load_failure(path, fabric_context=fabric_context)
        (found,) = error.diagnostics
        assert (found.code, found.pointer) == ("P112", "/target/alias")
        assert list(found.expected or []) == [
            "${fabric.workspace_id}",
            "${fabric.lakehouse_id}",
            "${fabric.workspace_name}",
        ]
        assert "Did you mean ${fabric.workspace_id}?" in (found.hint or "")

    def test_unknown_key_with_a_default_still_fails(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.lakehouse:-x}"))
        assert [d.code for d in load_failure(path).diagnostics] == ["P112"]


class TestRuntimeParamsDoNotSupplyFabric:
    """A runtime ``fabric`` key is a param like any other, not the Fabric context."""

    def test_runtime_fabric_key_is_ignored_and_warns_unused(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        loaded = load_at(path, runtime={"fabric": {"workspace_id": "from-runtime"}})
        assert _alias(loaded) == "out.${fabric.workspace_id}"
        assert [d.code for d in loaded.diagnostics if d.code in ("P113", "P121")] == [
            "P113",
            "P121",
        ]
        (unused,) = with_code(loaded.diagnostics, "P121")
        assert list(unused.expected or []) == ["fabric"]

    def test_runtime_fabric_key_does_not_beat_the_context(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        loaded = load_at(
            path,
            runtime={"fabric": {"workspace_id": "from-runtime"}},
            fabric_context=FABRIC,
        )
        assert _alias(loaded) == "out.ws-1"


def _session(conf: dict[str, str]) -> MagicMock:
    spark = MagicMock(spec=SparkSession)
    spark.conf.get.side_effect = lambda key: conf.get(key, "")
    return spark


class TestContextAlwaysResolves:
    """``Context`` supplies its session's values; it never defers."""

    def test_root_and_refd_child_resolve(self, tmp_path: Path) -> None:
        root = write_project(tmp_path / "p.weevr")
        write_named(root, "t.thread", thread_body(ALL_KEYS))
        write_named(root, "w.weave", weave_body([{"ref": "t.thread"}]))
        loaded = Context(_session(SESSION_CONF), str(root)).load("w.weave")
        assert _alias(loaded, "t") == "ws-1.lh-1.sales"
        assert with_code(loaded.diagnostics, "P113") == []

    def test_a_key_the_session_lacks_fails_p110(self, tmp_path: Path) -> None:
        root = write_project(tmp_path / "p.weevr")
        write_named(root, "t.thread", thread_body("out.${fabric.lakehouse_id}"))
        write_named(root, "w.weave", weave_body([{"ref": "t.thread"}]))
        conf = {"trident.workspace.id": "ws-1"}
        with pytest.raises(ConfigError) as info:
            Context(_session(conf), str(root)).load("w.weave")
        assert [(d.code, d.pointer) for d in info.value.diagnostics] == [("P110", "/target/alias")]


class TestFabricContextKeys:
    """Each ``fabric_context`` key must name a Fabric value, referenced or not."""

    def test_misspelled_key_fails_p112(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.t"))
        (error,) = load_failure(path, fabric_context={"workspce_id": "x"}).diagnostics
        assert (error.code, error.pointer) == ("P112", None)
        assert list(error.expected or []) == ["workspace_id", "lakehouse_id", "workspace_name"]
        assert (error.hint or "").startswith("Did you mean workspace_id? ")

    def test_validate_records_a_misspelled_key(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.t"))
        document, _, errors = collected_at(path, fabric_context={"workspce_id": "x"})
        assert document is None
        assert [d.code for d in errors] == ["P112"]

    def test_misspelled_prefixed_key_fails_p112(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_id}"))
        error = load_failure(
            path, fabric_context={"fabric.workspace_id": "x", "fabric.lakehous": 1}
        )
        assert [d.code for d in error.diagnostics] == ["P112"]

    def test_load_config_checks_the_keys(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.t"))
        with pytest.raises(ConfigError) as info:
            load_config(path, project_root=tmp_path, fabric_context={"workspce_id": "x"})
        assert [d.code for d in info.value.diagnostics] == ["P112"]

    @pytest.mark.parametrize("key", ["workspace_name", "fabric.workspace_name"])
    def test_bare_and_prefixed_valid_keys_are_accepted(self, tmp_path: Path, key: str) -> None:
        path = write_named(tmp_path, "t.thread", thread_body("out.${fabric.workspace_name}"))
        assert _alias(load_at(path, fabric_context={key: "sales"})) == "out.sales"

    def test_context_load_passes_the_check(self, tmp_path: Path) -> None:
        root = write_project(tmp_path / "p.weevr")
        write_named(root, "t.thread", thread_body("out.t"))
        assert _alias(Context(_session({}), str(root)).load("t.thread")) == "out.t"
