"""Integration tests for full config loading pipeline."""

from pathlib import Path
from typing import Any

import pytest

from documents import write_document, write_project
from weevr.config import load_config, load_resolved
from weevr.errors import (
    ConfigError,
    ConfigParseError,
    ConfigSchemaError,
    ConfigVersionError,
    ModelValidationError,
    ReferenceResolutionError,
    VariableResolutionError,
)
from weevr.model import Loom, Thread, Weave

FIXTURES = Path(__file__).parent / "fixtures"


class TestLoadConfigHappyPath:
    """Test successful config loading scenarios."""

    def test_load_simple_thread(self):
        """load_config returns a Thread model for a thread config."""
        result = load_config(FIXTURES / "valid_thread.thread")
        assert isinstance(result, Thread)
        assert result.config_version == "2.0"
        assert "customers" in result.sources
        assert result.target is not None

    def test_load_thread_from_project(self):
        """Load a thread from project fixtures."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "dimensions" / "dim_customer.thread", project_root=project)
        assert isinstance(result, Thread)
        assert result.config_version == "2.0"
        assert "customers" in result.sources

    def test_load_thread_with_variable_resolution(self, tmp_path):
        """load_config resolves variables and returns a typed Thread."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: ${param.lakehouse}.customers
target:
  path: /data/${param.env}_output
""",
            kind="thread",
        )

        result = load_config(
            thread_file,
            runtime_params={"lakehouse": "bronze", "env": "dev"},
        )

        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert result.target.path == "/data/dev_output"

    def test_standalone_thread_drops_disabled_export(self, tmp_path):
        """A thread loaded on its own does not keep an export with enabled: false."""
        thread_file = tmp_path / "thread.thread"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: raw.customers
target:
  path: /data/output
exports:
  - name: archive
    type: parquet
    path: /data/archive
  - name: extract
    type: csv
    path: /data/extract
    enabled: false
""",
        )

        result = load_config(thread_file)

        assert isinstance(result, Thread)
        assert result.exports is not None
        assert [e.name for e in result.exports] == ["archive"]

    def test_load_thread_with_runtime_params(self, tmp_path):
        """load_config resolves variables from runtime params."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: ${param.lakehouse}.customers
target:
  path: /data/${param.env}_output
""",
            kind="thread",
        )

        result = load_config(thread_file, runtime_params={"lakehouse": "bronze", "env": "dev"})

        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert result.target.path == "/data/dev_output"

    def test_runtime_params_override_defaults(self, tmp_path):
        """Runtime params override declared param defaults."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
params:
  env:
    name: env
    type: string
    default: dev
sources:
  data:
    type: delta
    alias: lakehouse
target:
  path: /data/output_${param.env}
""",
            kind="thread",
        )

        result = load_config(
            thread_file,
            runtime_params={"env": "prod"},
        )

        assert isinstance(result, Thread)
        assert result.target.path == "/data/output_prod"

    def test_variable_with_fallback_default(self, tmp_path):
        """Variables with fallback defaults use the default when unset."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  path: ${param.output_table:-default_output}
""",
            kind="thread",
        )

        result = load_config(thread_file)

        assert isinstance(result, Thread)
        assert result.target.path == "default_output"

    def test_load_weave_returns_weave_model(self):
        """load_config returns a Weave model for a weave config."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "dimensions.weave", project_root=project)
        assert isinstance(result, Weave)
        assert result.config_version == "2.0"
        assert any(e.ref == "dimensions/dim_customer.thread" for e in result.threads)

    def test_load_loom_returns_loom_model(self):
        """load_config returns a Loom model for a loom config."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "nightly.loom", project_root=project)
        assert isinstance(result, Loom)
        assert result.config_version == "2.0"
        assert any(e.ref == "dimensions.weave" for e in result.weaves)


class TestLoadConfigNameInjection:
    """Test that load_config() derives and injects name from file path."""

    def test_thread_name_from_project_path(self):
        """Thread loaded from project path gets stem as name."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "dimensions" / "dim_customer.thread", project_root=project)
        assert isinstance(result, Thread)
        assert result.name == "dim_customer"

    def test_thread_name_from_top_level_path(self):
        """Thread loaded from a top-level file gets stem as name."""
        result = load_config(FIXTURES / "valid_thread.thread")
        assert isinstance(result, Thread)
        assert result.name == "valid_thread"

    def test_weave_name_derived(self):
        """Weave loaded from project path gets name from stem."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "dimensions.weave", project_root=project)
        assert isinstance(result, Weave)
        assert result.name == "dimensions"

    def test_loom_name_derived(self):
        """Loom loaded from project path gets name from stem."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "nightly.loom", project_root=project)
        assert isinstance(result, Loom)
        assert result.name == "nightly"

    def test_thread_name_not_overridden_when_present(self, tmp_path):
        """Explicit name in config is preserved over derived name."""
        thread_file = tmp_path / "threads" / "my_thread.yaml"
        thread_file.parent.mkdir(parents=True)
        write_document(
            thread_file,
            """
config_version: "2.0"
name: custom.name
sources:
  data:
    type: delta
    alias: lakehouse
target:
  alias: test
""",
            kind="thread",
        )
        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert result.name == "custom.name"


class TestLoadConfigErrorHandling:
    """Test error handling in config loading."""

    def test_missing_file(self):
        """Raise ConfigParseError for missing file."""
        with pytest.raises(ConfigParseError) as exc_info:
            load_config(FIXTURES / "nonexistent.yaml")
        assert "not found" in str(exc_info.value)

    def test_invalid_yaml_syntax(self):
        """Raise ConfigParseError for invalid YAML."""
        with pytest.raises(ConfigParseError) as exc_info:
            load_config(FIXTURES / "invalid_yaml.yaml")
        assert "Invalid YAML syntax" in str(exc_info.value)

    def test_missing_config_version(self):
        """Raise ConfigParseError for missing config_version."""
        with pytest.raises(ConfigParseError) as exc_info:
            load_config(FIXTURES / "missing_version.yaml")
        assert [d.code for d in exc_info.value.diagnostics] == ["C201"]

    def test_unsupported_version(self):
        """Raise ConfigVersionError for unsupported version."""
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(FIXTURES / "bad_version.yaml")
        assert [d.code for d in exc_info.value.diagnostics] == ["C203"]

    def test_schema_validation_failure(self, tmp_path):
        """Raise ConfigSchemaError for structurally invalid schema."""
        bad_file = tmp_path / "bad_schema.yaml"
        write_document(
            bad_file,
            """
config_version: "2.0"
threads: not_a_list
""",
            kind="thread",
        )

        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(bad_file)
        assert "Schema validation failed" in exc_info.value.message

    def test_unresolved_variable(self, tmp_path):
        """Raise VariableResolutionError for missing variable."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: ${param.missing_var}.customers
target:
  alias: test
""",
            kind="thread",
        )

        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(thread_file)
        assert "Unresolved variable" in str(exc_info.value)
        assert "missing_var" in str(exc_info.value)

    def test_model_validation_error_on_bad_thread(self, tmp_path):
        """Raise ModelValidationError when hydration fails (e.g., invalid step type)."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  alias: test
steps:
  - pivot:
      columns: [x]
""",
            kind="thread",
        )

        with pytest.raises(ModelValidationError):
            load_config(thread_file)

    def test_unsupported_source_type_fails_at_load(self, tmp_path):
        """A source with an unsupported type fails at load, before any read."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  book:
    type: excel
    path: /data/book.xlsx
target:
  alias: test
""",
            kind="thread",
        )

        with pytest.raises(ModelValidationError, match="unsupported source type"):
            load_config(thread_file)


class TestLoadConfigForeach:
    """Test foreach macro expansion in the load_config pipeline."""

    def test_foreach_expands_steps(self, tmp_path):
        """Foreach block in steps is expanded before hydration."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  alias: test
steps:
  - foreach:
      values: [name, email]
      as: col
      steps:
        - cast:
            columns:
              "{col}": string
""",
            kind="thread",
        )

        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert len(result.steps) == 2
        from weevr.model.pipeline import CastStep

        assert isinstance(result.steps[0], CastStep)
        assert result.steps[0].cast.columns == {"name": "string"}
        assert isinstance(result.steps[1], CastStep)
        assert result.steps[1].cast.columns == {"email": "string"}

    def test_foreach_mixed_with_regular_steps(self, tmp_path):
        """Foreach interleaved with regular steps preserves order."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  alias: test
steps:
  - filter:
      expr: "active = true"
  - foreach:
      values: [a, b]
      as: col
      steps:
        - rename:
            columns:
              "{col}": "{col}_clean"
  - select:
      columns: [id]
""",
            kind="thread",
        )

        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert len(result.steps) == 4

    def test_foreach_with_variables(self, tmp_path):
        """Foreach works alongside variable resolution."""
        thread_file = tmp_path / "thread.yaml"
        write_document(
            thread_file,
            """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: ${param.schema}.customers
target:
  alias: test
steps:
  - foreach:
      values: [x, y]
      as: col
      steps:
        - cast:
            columns:
              "{col}": string
""",
            kind="thread",
        )

        result = load_config(thread_file, runtime_params={"schema": "bronze"})
        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert len(result.steps) == 2


class TestLoadConfigTypedExtensions:
    """Test M09 typed extension behaviors."""

    def test_name_derived_from_stem(self, tmp_path):
        """Filename stem becomes the component name."""
        thread_file = tmp_path / "dim_customer.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\nsources:\n  data:\n    type: delta\n'
            "    alias: raw.customers\ntarget:\n  alias: test\n",
        )
        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert result.name == "dim_customer"

    def test_declared_name_matches_stem(self, tmp_path):
        """Declared name matching stem passes validation."""
        thread_file = tmp_path / "dim_customer.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\nname: dim_customer\nsources:\n  data:\n'
            "    type: delta\n    alias: raw.customers\ntarget:\n  alias: test\n",
        )
        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert result.name == "dim_customer"

    def test_declared_name_mismatch_raises(self, tmp_path):
        """Declared name not matching stem raises ConfigError."""
        thread_file = tmp_path / "dim_customer.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\nname: wrong_name\nsources:\n  data:\n'
            "    type: delta\n    alias: raw.customers\ntarget:\n  alias: test\n",
        )
        with pytest.raises(ConfigError, match="does not match filename stem"):
            load_config(thread_file)

    def test_qualified_key_set_with_project_root(self, tmp_path):
        """Qualified key is set relative to project root."""
        project = write_project(tmp_path / "test.weevr")
        thread_dir = project / "dims"
        thread_dir.mkdir(parents=True)
        thread_file = thread_dir / "dim_customer.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\nsources:\n  data:\n    type: delta\n'
            "    alias: raw.customers\ntarget:\n  alias: test\n",
        )
        result = load_config(thread_file, project_root=project)
        assert isinstance(result, Thread)
        assert result.qualified_key == "dims/dim_customer.thread"

    def test_end_to_end_weevr_project(self):
        """Load full hierarchy from .weevr project fixture."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "nightly.loom", project_root=project)
        assert isinstance(result, Loom)
        assert result.name == "nightly"
        assert result.qualified_key == "nightly.loom"


_V1_THREAD = (
    'config_version: "1.0"\nsources:\n  data:\n    type: delta\n'
    "    alias: raw.customers\ntarget:\n  alias: test\n"
)


class TestLoadConfigEnvelope:
    """v1 and kind-less documents fail at the envelope, naming the file and the migrate command."""

    @pytest.mark.parametrize("name", ["legacy.thread", "legacy.yaml"])
    def test_v1_document_without_kind_is_c203(self, tmp_path, name):
        path = tmp_path / name
        path.write_text(_V1_THREAD)
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(path)
        [diag] = exc_info.value.diagnostics
        assert diag.code == "C203"
        assert diag.file == str(path)
        assert diag.hint is not None and "weevr migrate" in diag.hint

    def test_v1_document_with_kind_is_c203(self, tmp_path):
        path = tmp_path / "legacy.thread"
        path.write_text("kind: thread\n" + _V1_THREAD)
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(path)
        assert [d.code for d in exc_info.value.diagnostics] == ["C203"]

    @pytest.mark.parametrize("name", ["no_kind.thread", "no_kind.yaml"])
    def test_v2_document_without_kind_is_c206(self, tmp_path, name):
        path = tmp_path / name
        path.write_text(_V1_THREAD.replace('"1.0"', '"2.0"'))
        with pytest.raises(ConfigError) as exc_info:
            load_config(path)
        [diag] = exc_info.value.diagnostics
        assert diag.code == "C206"
        assert diag.file == str(path)
        assert diag.hint is not None and "weevr migrate" in diag.hint

    def test_referenced_thread_without_kind_names_thread(self, tmp_path):
        thread = tmp_path / "t.thread"
        thread.write_text(_V1_THREAD.replace('"1.0"', '"2.0"'))
        weave = tmp_path / "w.weave"
        write_document(weave, 'config_version: "2.0"\nthreads:\n  - ref: t.thread\n')
        with pytest.raises(ConfigError) as exc_info:
            load_config(weave, project_root=tmp_path)
        [diag] = exc_info.value.diagnostics
        assert diag.code == "C206"
        assert diag.file == str(thread)

    @pytest.mark.parametrize(
        ("name", "body"),
        [
            ("t.thread", _V1_THREAD.replace('"1.0"', '"2.0"')),
            ("w.weave", 'config_version: "2.0"\nthreads: []\n'),
            ("l.loom", 'config_version: "2.0"\nweaves: []\n'),
        ],
    )
    def test_root_type_is_renamed_to_kind(self, tmp_path, name, body):
        """A v1 root ``type:`` is reported as renamed to ``kind``, beside a declared kind."""
        path = tmp_path / name
        kind = path.suffix.lstrip(".")
        write_document(path, f"type: {kind}\n{body}")
        with pytest.raises(ConfigError) as exc_info:
            load_config(path)
        c112 = [d for d in exc_info.value.diagnostics if d.code == "C112"]
        assert len(c112) == 1
        assert c112[0].pointer == "/type"
        assert c112[0].expected == ("/kind",)
        assert c112[0].file == str(path)

    def test_nested_type_keys_are_not_renamed(self, tmp_path):
        """``type:`` inside a source stays a valid key."""
        path = tmp_path / "t.thread"
        write_document(path, _V1_THREAD.replace('"1.0"', '"2.0"'))
        assert isinstance(load_config(path), Thread)


class TestLoadConfigLoomDeclaredParams:
    """Loom-level ``params:`` block is reachable as ``${param.x}`` within the same file."""

    def _make_stub_weave(self, project: Path, name: str = "stub") -> None:
        """Write a minimal ``<name>.weave`` plus the thread it references."""
        thread_dir = project / "threads"
        thread_dir.mkdir(exist_ok=True)
        write_document(
            thread_dir / "stub.thread",
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n",
        )
        write_document(
            project / f"{name}.weave",
            'config_version: "2.0"\nthreads:\n  - ref: threads/stub.thread\n',
        )

    def _bug_report_loom(self, tmp_path: Path) -> Path:
        project = write_project(tmp_path / "silver.weevr")
        self._make_stub_weave(project)
        loom_file = project / "spartech_silver.loom"
        write_document(
            loom_file,
            """
config_version: "2.0"

weaves:
  - ref: stub.weave

params:
  workspace_id:
    name: workspace_id
    type: string
    required: true
  mirror_db_id:
    name: mirror_db_id
    type: string
    required: true

connections:
  bronze:
    type: onelake
    workspace: "${param.workspace_id}"
    lakehouse: "${param.mirror_db_id}"
""",
        )
        return loom_file

    def test_bug_report_loom_loads_with_runtime_params(self, tmp_path):
        """Bug-report loom resolves end-to-end with runtime-supplied params."""
        loom_file = self._bug_report_loom(tmp_path)
        result = load_config(
            loom_file,
            runtime_params={
                "workspace_id": "ws-aaaa-bbbb",
                "mirror_db_id": "lh-1234-5678",
            },
            project_root=loom_file.parent,
        )
        assert isinstance(result, Loom)
        assert result.connections is not None
        bronze = result.connections["bronze"]
        assert bronze.workspace == "ws-aaaa-bbbb"
        assert bronze.lakehouse == "lh-1234-5678"

    def test_required_loom_param_missing_raises(self, tmp_path):
        """Required loom-declared param with no runtime value raises with file context."""
        loom_file = self._bug_report_loom(tmp_path)
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(loom_file, runtime_params={}, project_root=loom_file.parent)
        msg = str(exc_info.value)
        assert "workspace_id" in msg
        assert "spartech_silver.loom" in msg

    def test_loom_declared_default_used_when_runtime_omits(self, tmp_path):
        """Optional loom param with default falls back to default value."""
        project = write_project(tmp_path / "p.weevr")
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        write_document(
            loom_file,
            """
config_version: "2.0"

weaves:
  - ref: stub.weave

params:
  region:
    name: region
    type: string
    required: false
    default: eastus

connections:
  bronze:
    type: onelake
    workspace: "${param.region}"
    lakehouse: lh
""",
        )
        result = load_config(loom_file, project_root=project)
        assert isinstance(result, Loom)
        assert result.connections is not None
        assert result.connections["bronze"].workspace == "eastus"

    def test_defaults_scalar_is_not_a_variable(self, tmp_path):
        """A bare reference to a ``defaults:`` scalar fails ``P111`` with its rewrite."""
        project = write_project(tmp_path / "p.weevr")
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        write_document(
            loom_file,
            """
config_version: "2.0"

weaves:
  - ref: stub.weave

defaults:
  env: dev

params:
  workspace_id:
    name: workspace_id
    type: string
    required: true

connections:
  bronze:
    type: onelake
    workspace: "${param.workspace_id}"
    lakehouse: "${env}"
""",
        )
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(loom_file, runtime_params={"workspace_id": "ws-1"}, project_root=project)
        assert [d.code for d in exc_info.value.diagnostics] == ["P111"]
        assert exc_info.value.diagnostics[0].expected == ("${param.env}",)

    def test_param_typo_lists_available_names(self, tmp_path):
        """Typo on ``${param.x}`` raises with available declared param names."""
        project = write_project(tmp_path / "p.weevr")
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        write_document(
            loom_file,
            """
config_version: "2.0"

weaves:
  - ref: stub.weave

params:
  workspace_id:
    name: workspace_id
    type: string
    required: true

connections:
  bronze:
    type: onelake
    workspace: "${param.workspaceid}"
    lakehouse: lh
""",
        )
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(
                loom_file,
                runtime_params={"workspace_id": "ws-1"},
                project_root=project,
            )
        msg = str(exc_info.value)
        assert "param.workspaceid" in msg
        assert "workspace_id" in msg

    def test_loom_param_type_mismatch_raises(self, tmp_path):
        """Runtime value of wrong declared type raises ConfigSchemaError."""
        project = write_project(tmp_path / "p.weevr")
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        write_document(
            loom_file,
            """
config_version: "2.0"

weaves:
  - ref: stub.weave

params:
  max_rows:
    name: max_rows
    type: int
    required: true

connections:
  bronze:
    type: onelake
    workspace: ws-1
    lakehouse: lh-1
""",
        )
        with pytest.raises(ConfigSchemaError, match="expected int"):
            load_config(loom_file, runtime_params={"max_rows": "abc"}, project_root=project)


class TestLoadConfigExportEnabled:
    """A weave's export is suppressed by any enabled value the model reads as false."""

    def _project(self, tmp_path: Path, enabled: str) -> Path:
        write_document(
            tmp_path / "t.thread",
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n",
        )
        write_document(
            tmp_path / "w.weave",
            'config_version: "2.0"\n'
            "defaults:\n  exports:\n"
            "    - name: archive\n      type: parquet\n      path: /data/archive\n"
            f"      enabled: {enabled}\n"
            "params:\n  write_archive:\n    name: write_archive\n    type: string\n"
            "    required: false\n"
            "threads:\n  - ref: t.thread\n",
        )
        return tmp_path / "w.weave"

    def test_param_default_false_suppresses(self, tmp_path):
        """enabled: ${param.x:-false} with no runtime value drops the export."""
        weave = self._project(tmp_path, '"${param.write_archive:-false}"')
        result = load_resolved(weave, project_root=tmp_path)
        assert not result.document.weave_threads()["w"]["t"].exports

    def test_param_runtime_true_keeps(self, tmp_path):
        """enabled: ${param.x:-false} with x=true at runtime keeps the export."""
        weave = self._project(tmp_path, '"${param.write_archive:-false}"')
        result = load_resolved(
            weave, runtime_params={"write_archive": "true"}, project_root=tmp_path
        )
        exports = result.document.weave_threads()["w"]["t"].exports
        assert exports is not None
        assert [e.name for e in exports] == ["archive"]

    def test_name_only_string_override_suppresses(self, tmp_path):
        """A thread override naming the export with enabled: "false" drops it."""
        weave = self._project(tmp_path, "true")
        with (tmp_path / "t.thread").open("a") as thread_file:
            thread_file.write('exports:\n  - name: archive\n    enabled: "false"\n')
        result = load_resolved(weave, project_root=tmp_path)
        assert not result.document.weave_threads()["w"]["t"].exports

    def test_unreadable_enabled_still_fails(self, tmp_path):
        """An enabled value the model can't read as a bool still fails the load."""
        weave = self._project(tmp_path, "maybe")
        with pytest.raises(ConfigError) as exc_info:
            load_resolved(weave, project_root=tmp_path)
        assert [d.code for d in exc_info.value.diagnostics] == ["C102"]


class TestLoadConfigWeaveDeclaredParams:
    """Weave-level ``params:`` block is reachable as ``${param.x}`` within the same file."""

    def test_weave_loaded_directly_resolves_declared_params(self, tmp_path):
        """A standalone .weave file with declared params: honors ${param.x} from runtime."""
        project = write_project(tmp_path / "p.weevr")
        thread_dir = project / "threads"
        thread_dir.mkdir()
        write_document(
            thread_dir / "stub.thread",
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n",
        )
        weave_file = project / "bronze_to_silver.weave"
        write_document(
            weave_file,
            """
config_version: "2.0"

threads:
  - ref: threads/stub.thread

params:
  workspace_id:
    name: workspace_id
    type: string
    required: true

connections:
  source:
    type: onelake
    workspace: "${param.workspace_id}"
    lakehouse: lh-bronze
""",
        )
        result = load_config(
            weave_file,
            runtime_params={"workspace_id": "ws-direct"},
            project_root=project,
        )
        assert isinstance(result, Weave)
        assert result.connections is not None
        assert result.connections["source"].workspace == "ws-direct"

    def test_weave_required_param_missing_raises(self, tmp_path):
        """Required weave-declared param missing surfaces ConfigSchemaError with file path."""
        project = write_project(tmp_path / "p.weevr")
        thread_dir = project / "threads"
        thread_dir.mkdir()
        write_document(
            thread_dir / "stub.thread",
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n",
        )
        weave_file = project / "bronze_to_silver.weave"
        write_document(
            weave_file,
            """
config_version: "2.0"

threads:
  - ref: threads/stub.thread

params:
  workspace_id:
    name: workspace_id
    type: string
    required: true

connections:
  source:
    type: onelake
    workspace: "${param.workspace_id}"
    lakehouse: lh-bronze
""",
        )
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(weave_file, runtime_params={}, project_root=project)
        msg = str(exc_info.value)
        assert "workspace_id" in msg
        assert "bronze_to_silver.weave" in msg


_VALID_THREAD = """
config_version: "2.0"
sources:
  data:
    type: delta
    alias: raw.customers
target:
  path: Tables/customers
"""


class TestLoadConfigErrorFileAttribution:
    """Errors raised while loading a file name that file in ``file_path``."""

    def test_schema_error_names_file(self, tmp_path):
        """A schema-shape error carries the loaded file's path."""
        thread = tmp_path / "shape.thread"
        write_document(
            thread,
            'config_version: "2.0"\nsources: ["not", "a", "mapping"]\ntarget:\n  path: Tables/x\n',
        )
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)
        assert f"in {thread}" in exc_info.value.plain_text()

    def test_version_error_names_file(self, tmp_path):
        """An unsupported config_version carries the loaded file's path."""
        thread = tmp_path / "ver.thread"
        write_document(thread, _VALID_THREAD.replace('"2.0"', '"9.0"'))
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)

    def test_variable_error_names_file_and_keeps_key(self, tmp_path):
        """An unresolved variable carries the file's path and its config key."""
        thread = tmp_path / "vars.thread"
        write_document(thread, _VALID_THREAD.replace("raw.customers", "${param.env.missing}"))
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)
        assert exc_info.value.config_key == "param.env.missing"

    def test_foreach_error_names_file(self, tmp_path):
        """A malformed foreach block carries the loaded file's path."""
        thread = tmp_path / "macro.thread"
        write_document(
            thread,
            _VALID_THREAD
            + "steps:\n  - foreach:\n      values: []\n      as: col\n"
            + '      steps:\n        - filter:\n            expr: "x > 0"\n',
        )
        with pytest.raises(ConfigError, match="foreach") as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)


class TestLoadConfigReferencedFileAttribution:
    """Errors in referenced files name the referenced file, not the caller."""

    @staticmethod
    def _write(root: Path, name: str, text: str) -> Path:
        path = root / name
        write_document(path, text)
        return path

    def _weave_of(self, root: Path, name: str, *entries: str) -> Path:
        body = "".join(f"  - {e}\n" for e in entries)
        return self._write(root, name, f'config_version: "2.0"\nthreads:\n{body}')

    def _loom_of(self, root: Path, *entries: str) -> Path:
        body = "".join(f"  - {e}\n" for e in entries)
        return self._write(root, "top.loom", f'config_version: "2.0"\nweaves:\n{body}')

    def test_referenced_thread_schema_error_names_thread(self, tmp_path):
        """A schema error in a ref'd thread names the thread."""
        thread = self._write(
            tmp_path,
            "shape.thread",
            'config_version: "2.0"\nsources: ["not", "a", "mapping"]\ntarget:\n  path: Tables/x\n',
        )
        weave = self._weave_of(tmp_path, "w.weave", "ref: shape.thread")
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_referenced_thread_variable_error_names_thread(self, tmp_path):
        """An unresolved variable in a ref'd thread names the thread."""
        thread = self._write(
            tmp_path, "vars.thread", _VALID_THREAD.replace("raw.customers", "${param.env.missing}")
        )
        weave = self._weave_of(tmp_path, "w.weave", "ref: vars.thread")
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_referenced_thread_version_error_names_thread(self, tmp_path):
        """An unsupported version in a ref'd thread names the thread."""
        thread = self._write(tmp_path, "ver.thread", _VALID_THREAD.replace('"2.0"', '"9.0"'))
        weave = self._weave_of(tmp_path, "w.weave", "ref: ver.thread")
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_loom_weave_thread_error_names_thread(self, tmp_path):
        """Two levels down, the thread's error still names the thread."""
        thread = self._write(
            tmp_path, "vars.thread", _VALID_THREAD.replace("raw.customers", "${param.env.missing}")
        )
        self._weave_of(tmp_path, "w.weave", "ref: vars.thread")
        loom = self._loom_of(tmp_path, "ref: w.weave")
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_loom_referenced_weave_error_names_weave(self, tmp_path):
        """An error in a ref'd weave names the weave, not the loom."""
        weave = self._write(tmp_path, "w.weave", 'config_version: "9.0"\nthreads: []\n')
        loom = self._loom_of(tmp_path, "ref: w.weave")
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_name_only_weave_is_not_loaded_by_convention(self, tmp_path):
        """A name-only entry fails even when a same-named weave file exists."""
        self._write(tmp_path, "w.weave", 'config_version: "9.0"\nthreads: []\n')
        loom = self._loom_of(tmp_path, "name: w")
        with pytest.raises(ConfigError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.diagnostics[0].code == "C313"

    def test_entry_params_error_names_weave(self, tmp_path):
        """An unresolvable entry params expression belongs to the weave."""
        self._write(tmp_path, "t.thread", _VALID_THREAD)
        weave = self._weave_of(
            tmp_path, "w.weave", "ref: t.thread\n    params:\n      x: ${param.nope}"
        )
        loom = self._loom_of(tmp_path, "ref: w.weave")
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_duplicate_effective_name_names_weave(self, tmp_path):
        """A duplicate effective thread name belongs to the weave."""
        self._write(tmp_path, "a.thread", _VALID_THREAD)
        self._write(tmp_path, "b.thread", _VALID_THREAD)
        weave = self._weave_of(
            tmp_path, "w.weave", "ref: a.thread\n    as: x", "ref: b.thread\n    as: x"
        )
        loom = self._loom_of(tmp_path, "ref: w.weave")
        with pytest.raises(ConfigError, match="Duplicate effective thread name") as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_inline_thread_missing_name_names_weave(self, tmp_path):
        """An inline thread definition without a name belongs to the weave."""
        weave = self._weave_of(tmp_path, "w.weave", "sources: {}")
        loom = self._loom_of(tmp_path, "ref: w.weave")
        with pytest.raises(ConfigError, match="requires a 'name'") as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_missing_referenced_file_keeps_target_path(self, tmp_path):
        """A missing ref still names the missing target, not the referencing file."""
        weave = self._weave_of(tmp_path, "w.weave", "ref: gone.thread")
        loom = self._loom_of(tmp_path, "ref: w.weave")
        with pytest.raises(ReferenceResolutionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(tmp_path / "gone.thread")
        assert exc_info.value.file_path != str(weave)


class TestLoadConfigChildValidation:
    """load_config validates the children of a weave or loom, not just the top level."""

    @staticmethod
    def _write(root: Path, name: str, text: str) -> Path:
        path = root / name
        write_document(path, text)
        return path

    def test_weave_with_invalid_thread_raises(self, tmp_path):
        """A referenced thread that fails hydration fails the weave, naming the thread."""
        thread = self._write(tmp_path, "bad.thread", _VALID_THREAD + "write:\n  mode: bogus\n")
        weave = self._write(
            tmp_path, "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: bad.thread\n'
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_loom_with_invalid_thread_raises(self, tmp_path):
        """Two levels down, an invalid thread still fails the loom, naming the thread."""
        thread = self._write(tmp_path, "bad.thread", _VALID_THREAD + "write:\n  mode: bogus\n")
        self._write(tmp_path, "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: bad.thread\n')
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "2.0"\nweaves:\n  - ref: w.weave\n'
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_loom_with_invalid_weave_raises(self, tmp_path):
        """A referenced weave that fails hydration fails the loom, naming the weave."""
        self._write(tmp_path, "t.thread", _VALID_THREAD)
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "2.0"\nthreads:\n  - ref: t.thread\n'
            "lookups:\n  lk:\n    not_a_field: true\n",
        )
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "2.0"\nweaves:\n  - ref: w.weave\n'
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_invalid_inline_thread_names_weave(self, tmp_path):
        """An inline thread that fails hydration names the weave that defines it."""
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "2.0"\nthreads:\n'
            '  - name: t\n    config_version: "2.0"\n'
            "    sources:\n      data:\n        type: delta\n"
            "        alias: raw.c\n    target:\n      path: T/c\n"
            "    write:\n      mode: bogus\n",
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_valid_weave_and_loom_still_load(self, tmp_path):
        """Valid children don't change what load_config returns."""
        self._write(tmp_path, "t.thread", _VALID_THREAD)
        weave = self._write(
            tmp_path, "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: t.thread\n'
        )
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "2.0"\nweaves:\n  - ref: w.weave\n'
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)


_FOREACH_THREAD = (
    _VALID_THREAD
    + "steps:\n  - foreach:\n      values: [a, b]\n      as: col\n"
    + '      steps:\n        - filter:\n            expr: "{col} > 0"\n'
)


class TestLoadConfigChildForeach:
    """foreach blocks expand in threads a weave or loom references."""

    @staticmethod
    def _write(root: Path, name: str, text: str) -> Path:
        path = root / name
        write_document(path, text)
        return path

    def test_weave_with_foreach_thread_loads(self, tmp_path):
        """A referenced thread using foreach no longer fails its weave."""
        self._write(tmp_path, "fe.thread", _FOREACH_THREAD)
        weave = self._write(
            tmp_path, "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: fe.thread\n'
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)

    def test_loom_with_foreach_thread_loads(self, tmp_path):
        """The same holds two levels down."""
        self._write(tmp_path, "fe.thread", _FOREACH_THREAD)
        self._write(tmp_path, "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: fe.thread\n')
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "2.0"\nweaves:\n  - ref: w.weave\n'
        )
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)

    def test_foreach_values_from_entry_params_load(self, tmp_path):
        """foreach values passed in through a weave entry's params expand."""
        self._write(
            tmp_path,
            "fe.thread",
            _FOREACH_THREAD.replace("values: [a, b]", "values: ${param.cols}"),
        )
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "2.0"\nthreads:\n  - ref: fe.thread\n'
            "    params:\n      cols: [x, y]\n",
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)

    def test_inline_thread_with_foreach_loads(self, tmp_path):
        """An inline thread in a weave can use foreach too."""
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "2.0"\nthreads:\n'
            '  - name: t\n    config_version: "2.0"\n'
            "    sources:\n      data:\n        type: delta\n"
            "        alias: raw.c\n    target:\n      path: T/c\n"
            "    steps:\n      - foreach:\n          values: [a, b]\n          as: col\n"
            '          steps:\n            - filter:\n                expr: "{col} > 0"\n',
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)

    def test_malformed_foreach_in_referenced_thread_names_thread(self, tmp_path):
        """A bad foreach block in a referenced thread names that thread."""
        thread = self._write(
            tmp_path, "fe.thread", _FOREACH_THREAD.replace("values: [a, b]", "values: []")
        )
        weave = self._write(
            tmp_path, "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: fe.thread\n'
        )
        with pytest.raises(ConfigError, match="foreach block at step index") as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)


_INCREMENTAL_THREAD = (
    'config_version: "2.0"\n'
    "sources:\n  s:\n    type: delta\n    alias: raw.c\n"
    "target:\n  path: Tables/c\n"
    "write:\n  mode: append\n"
)


class TestLoadConfigWatermarkTypeRequired:
    """incremental_watermark needs watermark_type alongside watermark_column."""

    @staticmethod
    def _write(root: Path, name: str, text: str) -> Path:
        path = root / name
        write_document(path, text)
        return path

    def test_missing_watermark_type_names_thread(self, tmp_path):
        """A thread missing watermark_type fails at load, naming the thread."""
        thread = self._write(
            tmp_path,
            "t.thread",
            _INCREMENTAL_THREAD + "load:\n  mode: incremental_watermark\n  watermark_column: ts\n",
        )
        with pytest.raises(ModelValidationError, match="watermark_type") as exc_info:
            load_config(thread, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_watermark_load_inherited_whole_from_defaults(self, tmp_path):
        """A thread with no load block inherits a typed defaults.load and loads."""
        self._write(tmp_path, "t.thread", _INCREMENTAL_THREAD)
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "2.0"\n'
            "defaults:\n  load:\n    mode: incremental_watermark\n"
            "    watermark_column: ts\n    watermark_type: timestamp\n"
            "threads:\n  - ref: t.thread\n",
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)

    def test_own_watermark_load_ignores_defaults_watermark_type(self, tmp_path):
        """A thread's own typeless load block fails even if defaults carry a type."""
        thread = self._write(
            tmp_path,
            "t.thread",
            _INCREMENTAL_THREAD + "load:\n  mode: incremental_watermark\n  watermark_column: ts\n",
        )
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "2.0"\n'
            "defaults:\n  load:\n    watermark_type: date\n"
            "threads:\n  - ref: t.thread\n",
        )
        with pytest.raises(ModelValidationError, match="watermark_type") as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)


_INLINE_THREAD_BODY = (
    '      - name: ops_audit\n        config_version: "2.0"\n'
    "        sources:\n          events:\n            type: delta\n"
    "            alias: raw.events\n        target:\n          alias: ops.audit\n"
)

_REF_THREAD = (
    'config_version: "2.0"\nsources:\n  data:\n    type: delta\n'
    "    alias: raw.orders\ntarget:\n  alias: stg.orders\n"
)


class TestLoadConfigInlineWeaves:
    """A loom's inline weave resolves like a weave file."""

    @staticmethod
    def _loom(tmp_path: Path, weaves: str) -> Path:
        loom = tmp_path / "x.loom"
        write_document(loom, 'config_version: "2.0"\nweaves:\n' + weaves)
        return loom

    def test_inline_weave_with_inline_thread_loads(self, tmp_path):
        """An inline weave defining its thread inline loads."""
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "2.0"\n    threads:\n' + _INLINE_THREAD_BODY,
        )
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)

    def test_inline_weave_with_referenced_thread_loads(self, tmp_path):
        """An inline weave referencing a thread file loads."""
        write_document(tmp_path / "orders.thread", _REF_THREAD)
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "2.0"\n    threads:\n'
            "      - ref: orders.thread\n",
        )
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)

    def test_inline_weave_thread_failure_names_loom(self, tmp_path):
        """An invalid inline thread in an inline weave names the loom file."""
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "2.0"\n    threads:\n'
            + _INLINE_THREAD_BODY
            + "        write:\n          mode: bogus\n",
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(loom)

    def test_inline_weave_defaults_are_not_variables(self, tmp_path):
        """A bare reference to the inline weave's own defaults fails ``P111``."""
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "2.0"\n'
            "    defaults:\n      tbl: ops.from_weave\n    threads:\n"
            '      - name: ops_audit\n        config_version: "2.0"\n'
            "        sources:\n          events:\n            type: delta\n"
            "            alias: raw.events\n        target:\n"
            '          alias: "${tbl}"\n',
        )
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert [d.code for d in exc_info.value.diagnostics] == ["P111"]
        assert exc_info.value.diagnostics[0].expected == ("${param.tbl}",)

    def test_name_only_weave_without_file_raises(self, tmp_path):
        """A name-only weave entry with no file and no body fails as name-only."""
        loom = self._loom(tmp_path, "  - name: missing\n")
        with pytest.raises(ReferenceResolutionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.diagnostics[0].code == "C313"
        assert exc_info.value.file_path == str(loom)

    def test_string_weave_entry_without_file_raises(self, tmp_path):
        """The string shorthand for a weave entry with no file also raises."""
        loom = self._loom(tmp_path, "  - missing\n")
        with pytest.raises(ReferenceResolutionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.diagnostics[0].code == "C313"


class TestLoadConfigRelativePathAttribution:
    """A relative path passed to load_config is reported as given."""

    _BAD_THREAD = (
        'config_version: "2.0"\nsources:\n  d:\n    type: delta\n    alias: raw.x\n'
        "target:\n  alias: out\nwrite:\n  mode: bogus\n"
    )

    def test_thread_failure_names_relative_path(self, tmp_path, monkeypatch):
        """A model failure in a directly loaded thread names the path as passed."""
        write_document(tmp_path / "bad.thread", self._BAD_THREAD)
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ModelValidationError) as exc_info:
            load_config("bad.thread")
        assert exc_info.value.file_path == "bad.thread"

    def test_child_failure_names_relative_path(self, tmp_path, monkeypatch):
        """A model failure in a referenced thread names that thread's relative path."""
        write_document(tmp_path / "bad.thread", self._BAD_THREAD)
        write_document(
            tmp_path / "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: bad.thread\n'
        )
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ModelValidationError) as exc_info:
            load_config("w.weave")
        assert exc_info.value.file_path == "bad.thread"


_FILE_SOURCE_THREAD = (
    'config_version: "2.0"\nsources:\n  orders:\n    type: csv\n    path: data/orders.csv\n'
    "target:\n  alias: stg.orders\n"
)

_LOOKUP_THREAD = _FILE_SOURCE_THREAD + (
    "lookups:\n  regions:\n    source:\n      type: csv\n      path: ref/regions.csv\n"
    "    materialize: false\n"
)


class TestLoadConfigMatchesContext:
    """load_config behaves like Context.load for typed configs."""

    def test_logs_execution_scope_warning(self, tmp_path, caplog):
        """A thread's own execution block is warned about on the weevr.config logger."""
        import logging

        write_document(
            tmp_path / "t.thread", _FILE_SOURCE_THREAD + "execution:\n  log_level: debug\n"
        )
        write_document(tmp_path / "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: t.thread\n')
        with caplog.at_level(logging.WARNING, logger="weevr.config"):
            load_config(tmp_path / "w.weave", project_root=tmp_path)
        messages = [r.message for r in caplog.records if r.name.startswith("weevr.config")]
        assert any("thread 't' declares an execution block" in m for m in messages)

    def test_relative_file_source_path_is_absolute(self, tmp_path):
        """A relative file-Source path resolves against the file's directory."""
        write_document(tmp_path / "t.thread", _FILE_SOURCE_THREAD)
        thread = load_config(tmp_path / "t.thread")
        assert isinstance(thread, Thread)
        assert thread.sources["orders"].path == str(tmp_path / "data" / "orders.csv")

    def test_relative_thread_lookup_path_is_absolute(self, tmp_path):
        """A thread's own lookup resolves a relative file path like its sources."""
        write_document(tmp_path / "t.thread", _LOOKUP_THREAD)
        thread = load_config(tmp_path / "t.thread", project_root=tmp_path)
        assert isinstance(thread, Thread)
        assert thread.lookups is not None
        assert thread.lookups["regions"].source.path == str(tmp_path / "ref" / "regions.csv")

    def test_child_thread_lookup_path_is_absolute(self, tmp_path):
        """A weave's child thread resolves its own lookup's relative file path."""
        write_document(tmp_path / "t.thread", _LOOKUP_THREAD)
        write_document(tmp_path / "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: t.thread\n')
        result = load_resolved(tmp_path / "w.weave", project_root=tmp_path)
        thread = result.document.weave_threads()["w"]["t"]
        assert thread.lookups is not None
        assert thread.lookups["regions"].source.path == str(tmp_path / "ref" / "regions.csv")

    def test_template_thread_hint(self, tmp_path):
        """A standalone thread failing on ${param.x} says it may be a template."""
        write_document(
            tmp_path / "t.thread", _FILE_SOURCE_THREAD.replace("stg.orders", "stg.${param.region}")
        )
        with pytest.raises(VariableResolutionError, match="may be a template") as exc_info:
            load_config(tmp_path / "t.thread")
        assert exc_info.value.config_key == "param.region"
        assert isinstance(exc_info.value.__cause__, VariableResolutionError)

    def test_qualified_key_without_project_root(self, tmp_path):
        """A typed file loaded without project_root gets a qualified_key."""
        write_document(tmp_path / "t.thread", _FILE_SOURCE_THREAD)
        thread = load_config(tmp_path / "t.thread")
        assert isinstance(thread, Thread)
        assert thread.qualified_key == "t.thread"


class TestLoadConfigFlatAuditTemplates:
    """The flat audit_templates shorthand loads like the nested columns form."""

    COLUMNS = {"_load_ts": "current_timestamp()", "_source": "'erp'"}

    @staticmethod
    def _thread(**extra: Any) -> dict[str, Any]:
        return {
            "config_version": "2.0",
            "sources": {"src": {"type": "delta", "alias": "lh.raw"}},
            "target": {"path": "/data/out", "audit_template": "lineage"},
            **extra,
        }

    def _templates(self, form: str) -> dict[str, Any]:
        if form == "flat":
            return {"lineage": dict(self.COLUMNS)}
        return {"lineage": {"columns": dict(self.COLUMNS)}}

    @pytest.mark.parametrize("level", ["loom", "weave", "thread"])
    def test_flat_template_loads_at_each_level(self, make_project, level):
        """A flat template declared at loom, weave, or thread level loads."""
        templates = {"audit_templates": self._templates("flat")}
        thread = self._thread(**templates) if level == "thread" else self._thread()
        project = make_project(
            thread,
            weave=templates if level == "weave" else None,
            loom=templates if level == "loom" else None,
        )
        files = {"loom": "l.loom", "weave": "w.weave", "thread": "fact.thread"}
        model = load_config(project / files[level], project_root=project)
        if level == "thread":
            # A thread's own template is applied at load, like a nested thread's.
            assert model.target.audit_columns == self.COLUMNS
            return
        assert model.audit_templates is not None
        assert model.audit_templates["lineage"].columns == self.COLUMNS

    @pytest.mark.parametrize("form", ["flat", "nested"])
    @pytest.mark.parametrize("level", ["loom", "weave"])
    def test_referenced_template_resolves_same_columns(self, make_project, level, form):
        """A thread referencing a loom or weave template gets its columns in either form."""
        templates = {"audit_templates": self._templates(form)}
        project = make_project(
            self._thread(),
            weave=templates if level == "weave" else None,
            loom=templates if level == "loom" else None,
        )
        loaded = load_resolved(project / "l.loom", project_root=project)
        audit = loaded.document.weave_threads()["w"]["fact"].target.audit_columns
        assert audit is not None
        assert {k: audit[k] for k in self.COLUMNS} == self.COLUMNS


class TestLoadConfigIncrementalDedupRule:
    """The incremental dedup rule surfaces at load, as weevr validate sees it."""

    def test_non_latest_dedup_fails_at_load(self, tmp_path):
        thread = tmp_path / "t.thread"
        write_document(
            thread,
            'config_version: "2.0"\n'
            "sources:\n  s:\n    type: delta\n    alias: raw.c\n"
            "    dedup:\n      keys: [id]\n      order_by: ts ASC\n"
            "target:\n  path: Tables/c\n"
            "write:\n  mode: append\n"
            "load:\n  mode: incremental_watermark\n  watermark_column: ts\n"
            "  watermark_type: timestamp\n",
        )
        with pytest.raises(ModelValidationError, match="ts DESC") as exc_info:
            load_config(thread, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)


class TestLoadConfigCdcRules:
    """CDC rules surface at load, including for a load cascaded from a parent."""

    CDC = {
        "mode": "cdc",
        "cdc": {"operation_column": "op", "insert_value": "I", "delete_value": "D"},
    }
    DIMENSION = {
        "business_key": ["id"],
        "surrogate_key": {"name": "sk", "columns": ["id"]},
        "change_detection": {"attrs": {"columns": ["name"], "on_change": "overwrite"}},
    }

    @staticmethod
    def _thread(**overrides: Any) -> dict[str, Any]:
        return {
            "config_version": "2.0",
            "sources": {"s": {"type": "delta", "alias": "raw.c"}},
            "target": {"path": "Tables/c"},
            **overrides,
        }

    def _dedup_thread(self, **overrides: Any) -> dict[str, Any]:
        source = {"type": "delta", "alias": "raw.c", "dedup": {"keys": ["id"]}}
        return self._thread(sources={"s": source}, **overrides)

    def _dimension_thread(self, **overrides: Any) -> dict[str, Any]:
        return self._thread(target={"path": "Tables/c", "dimension": self.DIMENSION}, **overrides)

    def test_dedup_fails_on_the_thread_itself(self, make_project):
        write = {"mode": "merge", "match_keys": ["id"]}
        project = make_project(self._dedup_thread(load=self.CDC, write=write))
        with pytest.raises(ConfigError, match="dedup"):
            load_config(project / "fact.thread", project_root=project)

    @pytest.mark.parametrize("level", ["weave", "loom"])
    def test_dedup_fails_with_a_cascaded_load(self, make_project, level):
        defaults = {"defaults": {"load": self.CDC}}
        project = make_project(
            self._dedup_thread(write={"mode": "merge", "match_keys": ["id"]}),
            weave=defaults if level == "weave" else None,
            loom=defaults if level == "loom" else None,
        )
        with pytest.raises(ConfigError, match="dedup"):
            load_config(
                project / f"{'w.weave' if level == 'weave' else 'l.loom'}", project_root=project
            )

    def test_dimension_fails_on_the_thread_itself(self, make_project):
        project = make_project(self._dimension_thread(load=self.CDC))
        with pytest.raises(ConfigError, match=r"target\.dimension"):
            load_config(project / "fact.thread", project_root=project)

    def test_dimension_fails_with_a_cascaded_load(self, make_project):
        project = make_project(self._dimension_thread(), weave={"defaults": {"load": self.CDC}})
        with pytest.raises(ConfigError, match=r"target\.dimension"):
            load_config(project / "w.weave", project_root=project)


class TestLoadConfigDimensionWriteMode:
    """Any write.mode on a dimension thread gets the one dimension error."""

    DIMENSION = {
        "business_key": ["id"],
        "surrogate_key": {"name": "sk", "columns": ["id"]},
        "change_detection": {"attrs": {"columns": ["name"], "on_change": "overwrite"}},
    }

    def _thread(self, write: dict[str, Any]) -> dict[str, Any]:
        return {
            "config_version": "2.0",
            "sources": {"s": {"type": "delta", "alias": "raw.c"}},
            "target": {"path": "Tables/c", "dimension": self.DIMENSION},
            "write": write,
        }

    @pytest.mark.parametrize("mode", ["merge", "overwrite", "append"])
    def test_every_mode_gets_the_dimension_error(self, make_project, mode):
        project = make_project(self._thread({"mode": mode}))
        with pytest.raises(ConfigError, match="cannot be set when target.dimension"):
            load_config(project / "fact.thread", project_root=project)

    def test_merge_overrides_without_a_mode_still_load(self, make_project):
        project = make_project(self._thread({"on_no_match_target": "insert"}))
        result = load_config(project / "fact.thread", project_root=project)
        assert isinstance(result, Thread)

    def test_plain_merge_still_needs_match_keys(self, make_project):
        thread = self._thread({"mode": "merge"})
        del thread["target"]["dimension"]
        project = make_project(thread)
        with pytest.raises(ConfigError, match="match_keys"):
            load_config(project / "fact.thread", project_root=project)


class TestLoadConfigForeachInWith:
    """foreach in a with: block fails with an error that says where and why."""

    FOREACH = {
        "foreach": {
            "values": ["a"],
            "as": "c",
            "steps": [{"derive": {"columns": {"{c}_x": "{c}"}}}],
        }
    }

    def _thread(self) -> dict[str, Any]:
        return {
            "config_version": "2.0",
            "sources": {"s": {"type": "delta", "alias": "raw.c"}},
            "target": {"path": "Tables/c"},
            "with": {"cte": {"from": "s", "steps": [self.FOREACH]}},
            "steps": [],
        }

    def test_direct_thread(self, make_project):
        project = make_project(self._thread())
        with pytest.raises(ConfigError, match=r"foreach.*with\.cte\.steps.*top-level") as exc:
            load_config(project / "fact.thread", project_root=project)
        assert exc.value.file_path == str(project / "fact.thread")
        assert not isinstance(exc.value, ModelValidationError)

    @pytest.mark.parametrize("entry", ["w.weave", "l.loom"])
    def test_through_a_parent(self, make_project, entry):
        project = make_project(self._thread())
        with pytest.raises(ConfigError, match=r"foreach.*with\.cte\.steps.*top-level"):
            load_config(project / entry, project_root=project)

    def test_top_level_foreach_still_expands(self, make_project):
        thread = self._thread()
        del thread["with"]
        thread["steps"] = [self.FOREACH]
        project = make_project(thread)
        result = load_config(project / "fact.thread", project_root=project)
        assert isinstance(result, Thread)
        assert len(result.steps) == 1


def test_pre_step_row_count_delta_fails_at_load(tmp_path: Path) -> None:
    """A row_count_delta gate in pre_steps is rejected before anything runs."""
    weave = tmp_path / "nightly.weave"
    write_document(
        weave,
        "config_version: '2.0'\n"
        "threads: [{ref: t1.thread}]\n"
        "pre_steps:\n"
        "  - type: quality_gate\n"
        "    check: row_count_delta\n"
        "    target: silver.orders\n"
        "    max_decrease_pct: 10\n",
    )
    write_document(
        tmp_path / "t1.thread",
        "config_version: '2.0'\n"
        "sources:\n  s: {type: delta, alias: raw.s}\n"
        "target: {alias: silver.orders}\n",
    )
    with pytest.raises(ModelValidationError, match=r"pre_steps\[0\].*post_steps") as exc_info:
        load_config(weave)
    assert getattr(exc_info.value, "file_path", None)


def _lookups_block(name: str, path: str, indent: str = "") -> str:
    """Return a ``lookups:`` block with one csv lookup, indented for nesting."""
    return (
        f"{indent}lookups:\n{indent}  {name}:\n{indent}    source:\n"
        f"{indent}      type: csv\n{indent}      path: {path}\n"
        f"{indent}    materialize: false\n"
    )


class TestLoadConfigLookupPaths:
    """Weave and loom lookups resolve relative file paths against the project root."""

    @staticmethod
    def _weave(tmp_path: Path) -> None:
        write_document(tmp_path / "t.thread", _FILE_SOURCE_THREAD)
        write_document(
            tmp_path / "w.weave",
            'config_version: "2.0"\n'
            + _lookups_block("countries", "ref/countries.csv")
            + "threads:\n  - ref: t.thread\n",
        )

    def test_weave_lookup_path_is_absolute(self, tmp_path):
        """A weave's lookup joins its relative path to the project root."""
        self._weave(tmp_path)
        weave = load_config(tmp_path / "w.weave", project_root=tmp_path)
        assert isinstance(weave, Weave)
        assert weave.lookups is not None
        assert weave.lookups["countries"].source.path == str(tmp_path / "ref" / "countries.csv")

    def test_loom_and_child_weave_lookup_paths_are_absolute(self, tmp_path):
        """A loom's lookups and its referenced and inline weaves' lookups are joined."""
        self._weave(tmp_path)
        write_document(
            tmp_path / "x.loom",
            'config_version: "2.0"\n'
            + _lookups_block("calendar", "ref/calendar.csv")
            + "weaves:\n  - ref: w.weave\n"
            + '  - name: inline_ops\n    config_version: "2.0"\n'
            + _lookups_block("stores", "ref/stores.csv", indent="    ")
            + "    threads:\n      - ref: t.thread\n",
        )
        result = load_resolved(tmp_path / "x.loom", project_root=tmp_path)
        loom = result.model
        assert isinstance(loom, Loom)
        assert loom.lookups is not None
        assert loom.lookups["calendar"].source.path == str(tmp_path / "ref" / "calendar.csv")
        referenced = result.document.weaves()["w"].lookups
        inline = result.document.weaves()["inline_ops"].lookups
        assert referenced is not None and inline is not None
        assert referenced["countries"].source.path == str(tmp_path / "ref" / "countries.csv")
        assert inline["stores"].source.path == str(tmp_path / "ref" / "stores.csv")

    def test_weave_lookup_joined_once_under_a_loom(self, tmp_path):
        """A weave's lookup path is the same loaded alone or as a loom's child."""
        self._weave(tmp_path)
        write_document(tmp_path / "x.loom", 'config_version: "2.0"\nweaves:\n  - ref: w.weave\n')
        alone = load_config(tmp_path / "w.weave", project_root=tmp_path)
        child = load_resolved(tmp_path / "x.loom", project_root=tmp_path).document.weaves()["w"]
        assert isinstance(alone, Weave)
        assert alone.lookups is not None and child.lookups is not None
        assert child.lookups["countries"].source.path == alone.lookups["countries"].source.path

    def test_absolute_weave_lookup_path_unchanged(self, tmp_path):
        """An absolute weave lookup path is used as written."""
        write_document(tmp_path / "t.thread", _FILE_SOURCE_THREAD)
        write_document(
            tmp_path / "w.weave",
            'config_version: "2.0"\n'
            + _lookups_block("countries", "/data/ref/countries.csv")
            + "threads:\n  - ref: t.thread\n",
        )
        weave = load_config(tmp_path / "w.weave", project_root=tmp_path)
        assert isinstance(weave, Weave)
        assert weave.lookups is not None
        assert weave.lookups["countries"].source.path == "/data/ref/countries.csv"
