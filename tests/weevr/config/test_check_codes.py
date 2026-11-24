"""Cross-field and registry checks report coded diagnostics."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from documents import write_document, write_project
from weevr.config import load_resolved
from weevr.config.validation import (
    collect_execution_scope_warnings,
    dimension_override_diagnostics,
    execution_scope_diagnostics,
    incremental_config_diagnostics,
    rename_column_set_diagnostics,
    render_diagnostics,
    resolve_lookup_diagnostics,
    source_lookup_diagnostics,
    trace_composition_diagnostics,
    validate_incremental_config,
)
from weevr.context import Context
from weevr.diagnostics import REGISTRY, Severity
from weevr.errors import ConfigError
from weevr.model.execution import ExecutionConfig

from .conftest import MakeProject


def _codes(diagnostics: list[Any]) -> list[str]:
    return [d.code for d in diagnostics]


def _thread(steps: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "sources": {"main": {"type": "delta", "alias": "raw.sales"}},
        "steps": steps or [],
        "target": {"alias": "gold.fact_sales"},
        **extra,
    }


class TestIncrementalCodes:
    def test_cdc_without_merge(self) -> None:
        (diag,) = incremental_config_diagnostics(
            {"load": {"mode": "cdc"}, "write": {"mode": "append"}}
        )
        assert diag.code == "C480"
        assert diag.severity is Severity.ERROR
        assert diag.expected == ("merge",)

    def test_cdc_collision_ops_without_watermark(self) -> None:
        load = {"mode": "cdc", "cdc": {"operation_column": "op", "delete_value": "D"}}
        diags = incremental_config_diagnostics({"load": load, "write": {"mode": "merge"}})
        assert _codes(diags) == ["C481"]

    def test_incremental_watermark_overwrite(self) -> None:
        (diag,) = incremental_config_diagnostics({"load": {"mode": "incremental_watermark"}})
        assert diag.code == "C482"
        assert diag.expected == ("append", "merge")

    def test_parameter_mode_without_param_warns(self) -> None:
        (diag,) = incremental_config_diagnostics({"load": {"mode": "incremental_parameter"}})
        assert (diag.code, diag.severity) == ("C483", Severity.WARNING)

    def test_inclusive_watermark_append_warns(self) -> None:
        load = {"mode": "incremental_watermark", "watermark_inclusive": True}
        (diag,) = incremental_config_diagnostics({"load": load, "write": {"mode": "append"}})
        assert (diag.code, diag.severity) == ("C484", Severity.WARNING)

    def test_strings_are_rendered_from_the_diagnostics(self) -> None:
        config = {"load": {"mode": "cdc"}, "write": {"mode": "append"}}
        (diag,) = incremental_config_diagnostics(config)
        assert validate_incremental_config(config) == [f"ERROR: {diag.message}"]


class TestDimensionCodes:
    @staticmethod
    def _config(**extra: Any) -> dict[str, Any]:
        return {"target": {"dimension": {}}, **extra}

    def test_each_forbidden_field_is_its_own_diagnostic(self) -> None:
        config = self._config(
            write={"match_keys": ["id"], "on_match": "update", "mode": "merge"},
            keys={"business_key": ["id"]},
        )
        diags = dimension_override_diagnostics(config)
        assert _codes(diags) == ["C485", "C485", "C486", "C487"]
        assert [d.resolved_path for d in diags] == [
            "/write/match_keys",
            "/write/on_match",
            "/write/mode",
            "/keys/business_key",
        ]

    def test_without_dimension_there_is_nothing(self) -> None:
        assert dimension_override_diagnostics({"write": {"mode": "merge"}}) == []


class TestReferenceCodes:
    def test_undefined_resolve_lookup(self) -> None:
        step = {"resolve": {"lookup": "dim", "on_failure": "abort"}}
        (diag,) = resolve_lookup_diagnostics(_thread([step]), available_lookups={"other"})
        assert (diag.code, diag.severity) == ("R120", Severity.ERROR)
        assert diag.expected == ("other",)
        assert diag.step_type == "resolve"

    def test_resolve_lookup_warn_is_a_warning_code(self) -> None:
        step = {"resolve": {"lookup": "dim", "on_failure": "warn"}}
        (diag,) = resolve_lookup_diagnostics(_thread([step]), available_lookups=set())
        assert (diag.code, diag.severity) == ("R121", Severity.WARNING)

    def test_batch_lookup_always_errors(self) -> None:
        step = {"resolve": {"batch": [{"lookup": "a"}], "on_failure": "warn"}}
        (diag,) = resolve_lookup_diagnostics(_thread([step]), available_lookups=set())
        assert diag.code == "R120"

    def test_undefined_source_lookup(self) -> None:
        thread = _thread(sources={"s": {"lookup": "nope"}})
        (diag,) = source_lookup_diagnostics(thread, available_lookups={"x"})
        assert (diag.code, diag.severity) == ("R122", Severity.ERROR)
        assert diag.resolved_path == "/sources/s/lookup"
        assert diag.expected == ("x",)

    def test_standalone_source_lookup_warns(self) -> None:
        thread = _thread(sources={"s": {"lookup": "nope"}})
        (diag,) = source_lookup_diagnostics(thread, available_lookups=set(), standalone=True)
        assert (diag.code, diag.severity) == ("R123", Severity.WARNING)

    def test_undefined_column_set_names_the_step(self) -> None:
        steps = [{"filter": {"expr": "a > 1"}}, {"rename": {"column_set": "nope"}}]
        (diag,) = rename_column_set_diagnostics(_thread(steps), available_column_sets={"std"})
        assert (diag.code, diag.severity) == ("R124", Severity.ERROR)
        assert (diag.step_index, diag.step_type) == (1, "rename")
        assert diag.expected == ("std",)

    def test_standalone_column_set_warns(self) -> None:
        steps = [{"rename": {"column_set": "nope"}}]
        (diag,) = rename_column_set_diagnostics(
            _thread(steps), available_column_sets=set(), standalone=True
        )
        assert (diag.code, diag.severity) == ("R125", Severity.WARNING)


class TestExecutionCodes:
    def test_scope_defaults_and_thread_blocks(self) -> None:
        diags = execution_scope_diagnostics(
            [("loom 'l'", {"execution": {"log_level": "debug"}})],
            [{"name": "t", "execution": {"trace": True}}],
        )
        assert _codes(diags) == ["C490", "C491"]
        assert all(d.severity is Severity.WARNING for d in diags)
        assert diags[1].node == "t"

    def test_strings_match_the_diagnostics(self) -> None:
        scopes = [("loom 'l'", {"execution": {"log_level": "debug"}})]
        (diag,) = execution_scope_diagnostics(scopes, [])
        assert collect_execution_scope_warnings(scopes, []) == [f"WARN: {diag.message}"]

    def test_trace_composition(self) -> None:
        (diag,) = trace_composition_diagnostics(
            ExecutionConfig(trace=False), {"facts": ExecutionConfig(trace=True)}
        )
        assert (diag.code, diag.severity, diag.node) == ("C492", Severity.WARNING, "facts")


def test_check_codes_are_registered_with_expected_severity() -> None:
    warnings = {"C483", "C484", "C490", "C491", "C492", "R121", "R123", "R125"}
    for code in sorted(warnings):
        assert REGISTRY[code].severity is Severity.WARNING
    for code in ("C350", "C480", "C481", "C482", "C485", "C486", "C487", "R120", "R122", "R124"):
        assert REGISTRY[code].severity is Severity.ERROR


class TestLoadReportsEveryError:
    def test_dimension_errors_are_located_and_listed_separately(
        self, make_project: MakeProject
    ) -> None:
        dimension = {
            "business_key": ["id"],
            "surrogate_key": {"name": "sk", "columns": ["id"]},
        }
        thread = _thread(
            write={"match_keys": ["id"], "on_match": "update"},
            keys={"business_key": ["id"]},
        )
        thread["target"] = {"alias": "gold.dim", "dimension": dimension}
        project = make_project(thread)
        with pytest.raises(ConfigError) as exc:
            load_resolved(project / "l.loom", project_root=project)
        diags = exc.value.diagnostics
        assert _codes(diags) == ["C485", "C485", "C487"]
        thread_file = str(project / "fact.thread")
        assert {d.file for d in diags} == {thread_file}
        assert [d.pointer for d in diags] == [
            "/write/match_keys",
            "/write/on_match",
            "/keys/business_key",
        ]
        # the project files are single-line JSON, so every position is on line 1
        assert [d.line for d in diags] == [1, 1, 1]

    def test_incremental_error_names_thread_and_file(self, make_project: MakeProject) -> None:
        thread = _thread(
            load={
                "mode": "incremental_watermark",
                "watermark_column": "ts",
                "watermark_type": "long",
            }
        )
        project = make_project(thread)
        with pytest.raises(ConfigError) as exc:
            load_resolved(project / "w.weave", project_root=project)
        (diag,) = exc.value.diagnostics
        assert diag.code == "C482"
        assert diag.node == "fact"
        assert diag.file == str(project / "fact.thread")

    def test_incremental_warning_string_is_unchanged(self, make_project: MakeProject) -> None:
        thread = _thread(
            load={"mode": "incremental_parameter"},
            write={"mode": "append"},
        )
        project = make_project(thread)
        loaded = load_resolved(project / "w.weave", project_root=project)
        assert [
            w for w in render_diagnostics(loaded.diagnostics) if "incremental_parameter" in w
        ] == [
            "WARN: thread 'fact': incremental_parameter mode but no ${param.*} "
            "references found in steps or sources"
        ]

    def test_every_undefined_reference_is_listed(self, make_project: MakeProject) -> None:
        steps = [{"rename": {"column_set": "a"}}, {"rename": {"column_set": "b"}}]
        project = make_project(_thread(steps))
        with pytest.raises(ConfigError) as exc:
            load_resolved(project / "w.weave", project_root=project)
        diags = exc.value.diagnostics
        assert _codes(diags) == ["R124", "R124"]
        assert [d.step_index for d in diags] == [0, 1]
        assert {d.node for d in diags} == {"fact"}
        assert {d.file for d in diags} == {str(project / "fact.thread")}

    def test_undefined_lookup_error_is_coded(self, make_project: MakeProject) -> None:
        step = {"resolve": {"lookup": "nope", "on_failure": "abort"}}
        project = make_project(_thread([step]))
        with pytest.raises(ConfigError) as exc:
            load_resolved(project / "w.weave", project_root=project)
        (diag,) = exc.value.diagnostics
        assert diag.code == "R120"
        assert diag.file == str(project / "fact.thread")

    def test_standalone_warning_strings_are_unchanged(self, make_project: MakeProject) -> None:
        project = make_project(_thread([{"rename": {"column_set": "std"}}]))
        loaded = load_resolved(project / "fact.thread", project_root=project)
        (warning,) = [w for w in render_diagnostics(loaded.diagnostics) if "'std'" in w]
        assert warning.startswith("WARN: thread 'fact': rename step 1 references column set")

    def test_duplicate_weave_name_points_at_the_entry(self, tmp_path: Path) -> None:
        project = write_project(tmp_path / "dup.weevr")
        (project / "a").mkdir(parents=True)
        (project / "b").mkdir()
        thread = json.dumps(_thread())
        for folder in ("a", "b"):
            write_document(project / folder / "x.thread", thread)
            write_document(
                project / folder / "main.weave",
                json.dumps({"config_version": "2.0", "threads": [{"ref": f"{folder}/x.thread"}]}),
            )
        write_document(
            project / "all.loom",
            'config_version: "2.0"\nweaves:\n  - ref: a/main.weave\n  - ref: b/main.weave\n',
        )
        with pytest.raises(ConfigError) as exc:
            load_resolved(project / "all.loom", project_root=project)
        (diag,) = exc.value.diagnostics
        assert diag.code == "C350"
        assert diag.pointer == "/weaves/1"
        assert diag.line == 5
        assert diag.file == str(project / "all.loom")
        assert diag.node == "main"


class TestStandaloneRunCode:
    def test_standalone_error_has_the_load_time_code(self, make_project: MakeProject) -> None:
        steps = [{"rename": {"column_set": "std"}}]
        project = make_project(_thread(steps), weave={"column_sets": {"std": {"param": "m"}}})
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        with pytest.raises(ConfigError) as standalone:
            ctx.run("fact.thread", mode="plan")
        (diag,) = standalone.value.diagnostics
        assert diag.code == "R124"
        assert diag.severity is Severity.ERROR
        assert diag.node == "fact"
        assert diag.step_index == 0
