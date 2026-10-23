"""Integration tests for full config loading pipeline."""

from pathlib import Path

import pytest

from weevr.config import load_config
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
        assert result.config_version == "1.0"
        assert "customers" in result.sources
        assert result.target is not None

    def test_load_thread_from_project(self):
        """Load a thread from project fixtures."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "dimensions" / "dim_customer.thread", project_root=project)
        assert isinstance(result, Thread)
        assert result.config_version == "1.0"
        assert "customers" in result.sources

    def test_load_thread_with_variable_resolution(self, tmp_path):
        """load_config resolves variables and returns a typed Thread."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${lakehouse}.customers
target:
  path: /data/${env}_output
"""
        )

        result = load_config(
            thread_file,
            runtime_params={"lakehouse": "bronze", "env": "dev"},
        )

        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert result.target.path == "/data/dev_output"

    def test_load_thread_with_runtime_params(self, tmp_path):
        """load_config resolves variables from runtime params."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${lakehouse}.customers
target:
  path: /data/${env}_output
"""
        )

        result = load_config(thread_file, runtime_params={"lakehouse": "bronze", "env": "dev"})

        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert result.target.path == "/data/dev_output"

    def test_runtime_params_override_defaults(self, tmp_path):
        """Runtime params override config default values."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
defaults:
  env: dev
sources:
  data:
    type: delta
    alias: lakehouse
target:
  path: /data/output_${env}
"""
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
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  path: ${output_table:-default_output}
"""
        )

        result = load_config(thread_file)

        assert isinstance(result, Thread)
        assert result.target.path == "default_output"

    def test_load_weave_returns_weave_model(self):
        """load_config returns a Weave model for a weave config."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "dimensions.weave", project_root=project)
        assert isinstance(result, Weave)
        assert result.config_version == "1.0"
        assert any(e.ref == "dimensions/dim_customer.thread" for e in result.threads)

    def test_load_loom_returns_loom_model(self):
        """load_config returns a Loom model for a loom config."""
        project = FIXTURES / "test_project.weevr"
        result = load_config(project / "nightly.loom", project_root=project)
        assert isinstance(result, Loom)
        assert result.config_version == "1.0"
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
        thread_file.write_text(
            """
config_version: "1.0"
name: custom.name
sources:
  data:
    type: delta
    alias: lakehouse
target:
  alias: test
"""
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
        assert "Missing required field 'config_version'" in str(exc_info.value)

    def test_unsupported_version(self):
        """Raise ConfigVersionError for unsupported version."""
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(FIXTURES / "bad_version.yaml")
        assert "Unsupported" in str(exc_info.value)
        assert "99.0" in str(exc_info.value)

    def test_schema_validation_failure(self, tmp_path):
        """Raise ConfigSchemaError for structurally invalid schema."""
        bad_file = tmp_path / "bad_schema.yaml"
        bad_file.write_text(
            """
config_version: "1.0"
threads: not_a_list
"""
        )

        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(bad_file)
        assert "Schema validation failed" in str(exc_info.value)

    def test_unresolved_variable(self, tmp_path):
        """Raise VariableResolutionError for missing variable."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${missing_var}.customers
target:
  alias: test
"""
        )

        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(thread_file)
        assert "Unresolved variable" in str(exc_info.value)
        assert "missing_var" in str(exc_info.value)

    def test_model_validation_error_on_bad_thread(self, tmp_path):
        """Raise ModelValidationError when hydration fails (e.g., invalid step type)."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  alias: test
steps:
  - pivot:
      columns: [x]
"""
        )

        with pytest.raises(ModelValidationError):
            load_config(thread_file)


class TestLoadConfigForeach:
    """Test foreach macro expansion in the load_config pipeline."""

    def test_foreach_expands_steps(self, tmp_path):
        """Foreach block in steps is expanded before hydration."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
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
"""
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
        thread_file.write_text(
            """
config_version: "1.0"
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
"""
        )

        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert len(result.steps) == 4

    def test_foreach_with_variables(self, tmp_path):
        """Foreach works alongside variable resolution."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${schema}.customers
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
"""
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
        thread_file.write_text(
            'config_version: "1.0"\nsources:\n  data:\n    type: delta\n'
            "    alias: raw.customers\ntarget:\n  alias: test\n"
        )
        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert result.name == "dim_customer"

    def test_declared_name_matches_stem(self, tmp_path):
        """Declared name matching stem passes validation."""
        thread_file = tmp_path / "dim_customer.thread"
        thread_file.write_text(
            'config_version: "1.0"\nname: dim_customer\nsources:\n  data:\n'
            "    type: delta\n    alias: raw.customers\ntarget:\n  alias: test\n"
        )
        result = load_config(thread_file)
        assert isinstance(result, Thread)
        assert result.name == "dim_customer"

    def test_declared_name_mismatch_raises(self, tmp_path):
        """Declared name not matching stem raises ConfigError."""
        thread_file = tmp_path / "dim_customer.thread"
        thread_file.write_text(
            'config_version: "1.0"\nname: wrong_name\nsources:\n  data:\n'
            "    type: delta\n    alias: raw.customers\ntarget:\n  alias: test\n"
        )
        with pytest.raises(ConfigError, match="does not match filename stem"):
            load_config(thread_file)

    def test_qualified_key_set_with_project_root(self, tmp_path):
        """Qualified key is set relative to project root."""
        project = tmp_path / "test.weevr"
        thread_dir = project / "dims"
        thread_dir.mkdir(parents=True)
        thread_file = thread_dir / "dim_customer.thread"
        thread_file.write_text(
            'config_version: "1.0"\nsources:\n  data:\n    type: delta\n'
            "    alias: raw.customers\ntarget:\n  alias: test\n"
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


class TestLoadConfigLoomDeclaredParams:
    """Loom-level ``params:`` block is reachable as ``${param.x}`` within the same file."""

    def _make_stub_weave(self, project: Path, name: str = "stub") -> None:
        """Write a minimal ``<name>.weave`` plus the thread it references."""
        thread_dir = project / "threads"
        thread_dir.mkdir(exist_ok=True)
        (thread_dir / "stub.thread").write_text(
            'config_version: "1.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n"
        )
        (project / f"{name}.weave").write_text(
            'config_version: "1.0"\nthreads:\n  - ref: threads/stub.thread\n'
        )

    def _bug_report_loom(self, tmp_path: Path) -> Path:
        project = tmp_path / "silver.weevr"
        project.mkdir()
        self._make_stub_weave(project)
        loom_file = project / "spartech_silver.loom"
        loom_file.write_text(
            """
config_version: "1.0"

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
"""
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
        project = tmp_path / "p.weevr"
        project.mkdir()
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        loom_file.write_text(
            """
config_version: "1.0"

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
"""
        )
        result = load_config(loom_file, project_root=project)
        assert isinstance(result, Loom)
        assert result.connections is not None
        assert result.connections["bronze"].workspace == "eastus"

    def test_defaults_and_params_coexist(self, tmp_path):
        """``defaults:`` (top-level) and ``params:`` (declared) both honored."""
        project = tmp_path / "p.weevr"
        project.mkdir()
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        loom_file.write_text(
            """
config_version: "1.0"

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
"""
        )
        result = load_config(
            loom_file,
            runtime_params={"workspace_id": "ws-1"},
            project_root=project,
        )
        assert isinstance(result, Loom)
        assert result.connections is not None
        bronze = result.connections["bronze"]
        assert bronze.workspace == "ws-1"
        assert bronze.lakehouse == "dev"

    def test_param_typo_lists_available_names(self, tmp_path):
        """Typo on ``${param.x}`` raises with available declared param names."""
        project = tmp_path / "p.weevr"
        project.mkdir()
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        loom_file.write_text(
            """
config_version: "1.0"

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
"""
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
        project = tmp_path / "p.weevr"
        project.mkdir()
        self._make_stub_weave(project)
        loom_file = project / "demo.loom"
        loom_file.write_text(
            """
config_version: "1.0"

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
"""
        )
        with pytest.raises(ConfigSchemaError, match="expected int"):
            load_config(loom_file, runtime_params={"max_rows": "abc"}, project_root=project)


class TestLoadConfigWeaveDeclaredParams:
    """Weave-level ``params:`` block is reachable as ``${param.x}`` within the same file."""

    def test_weave_loaded_directly_resolves_declared_params(self, tmp_path):
        """A standalone .weave file with declared params: honors ${param.x} from runtime."""
        project = tmp_path / "p.weevr"
        project.mkdir()
        thread_dir = project / "threads"
        thread_dir.mkdir()
        (thread_dir / "stub.thread").write_text(
            'config_version: "1.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n"
        )
        weave_file = project / "bronze_to_silver.weave"
        weave_file.write_text(
            """
config_version: "1.0"

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
"""
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
        project = tmp_path / "p.weevr"
        project.mkdir()
        thread_dir = project / "threads"
        thread_dir.mkdir()
        (thread_dir / "stub.thread").write_text(
            'config_version: "1.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target:\n  alias: out\n"
        )
        weave_file = project / "bronze_to_silver.weave"
        weave_file.write_text(
            """
config_version: "1.0"

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
"""
        )
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(weave_file, runtime_params={}, project_root=project)
        msg = str(exc_info.value)
        assert "workspace_id" in msg
        assert "bronze_to_silver.weave" in msg


_VALID_THREAD = """
config_version: "1.0"
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
        thread.write_text(
            'config_version: "1.0"\nsources: ["not", "a", "mapping"]\ntarget:\n  path: Tables/x\n'
        )
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)
        assert f"in {thread}" in str(exc_info.value)

    def test_version_error_names_file(self, tmp_path):
        """An unsupported config_version carries the loaded file's path."""
        thread = tmp_path / "ver.thread"
        thread.write_text(_VALID_THREAD.replace('"1.0"', '"9.0"'))
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)

    def test_variable_error_names_file_and_keeps_key(self, tmp_path):
        """An unresolved variable carries the file's path and its config key."""
        thread = tmp_path / "vars.thread"
        thread.write_text(_VALID_THREAD.replace("raw.customers", "${env.missing}"))
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)
        assert exc_info.value.config_key == "env.missing"

    def test_foreach_error_names_file(self, tmp_path):
        """A malformed foreach block carries the loaded file's path."""
        thread = tmp_path / "macro.thread"
        thread.write_text(
            _VALID_THREAD
            + "steps:\n  - foreach:\n      values: []\n      as: col\n"
            + '      steps:\n        - filter:\n            expr: "x > 0"\n'
        )
        with pytest.raises(ConfigError, match="foreach") as exc_info:
            load_config(thread)
        assert exc_info.value.file_path == str(thread)


class TestLoadConfigReferencedFileAttribution:
    """Errors in referenced files name the referenced file, not the caller."""

    @staticmethod
    def _write(root: Path, name: str, text: str) -> Path:
        path = root / name
        path.write_text(text)
        return path

    def _weave_of(self, root: Path, name: str, *entries: str) -> Path:
        body = "".join(f"  - {e}\n" for e in entries)
        return self._write(root, name, f'config_version: "1.0"\nthreads:\n{body}')

    def _loom_of(self, root: Path, *entries: str) -> Path:
        body = "".join(f"  - {e}\n" for e in entries)
        return self._write(root, "top.loom", f'config_version: "1.0"\nweaves:\n{body}')

    def test_referenced_thread_schema_error_names_thread(self, tmp_path):
        """A schema error in a ref'd thread names the thread."""
        thread = self._write(
            tmp_path,
            "shape.thread",
            'config_version: "1.0"\nsources: ["not", "a", "mapping"]\ntarget:\n  path: Tables/x\n',
        )
        weave = self._weave_of(tmp_path, "w.weave", "ref: shape.thread")
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_referenced_thread_variable_error_names_thread(self, tmp_path):
        """An unresolved variable in a ref'd thread names the thread."""
        thread = self._write(
            tmp_path, "vars.thread", _VALID_THREAD.replace("raw.customers", "${env.missing}")
        )
        weave = self._weave_of(tmp_path, "w.weave", "ref: vars.thread")
        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_referenced_thread_version_error_names_thread(self, tmp_path):
        """An unsupported version in a ref'd thread names the thread."""
        thread = self._write(tmp_path, "ver.thread", _VALID_THREAD.replace('"1.0"', '"9.0"'))
        weave = self._weave_of(tmp_path, "w.weave", "ref: ver.thread")
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_loom_weave_thread_error_names_thread(self, tmp_path):
        """Two levels down, the thread's error still names the thread."""
        thread = self._write(
            tmp_path, "vars.thread", _VALID_THREAD.replace("raw.customers", "${env.missing}")
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

    def test_convention_weave_error_names_weave(self, tmp_path):
        """A weave found by name convention names its own file."""
        weave = self._write(tmp_path, "w.weave", 'config_version: "9.0"\nthreads: []\n')
        loom = self._loom_of(tmp_path, "w")
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_entry_params_error_names_weave(self, tmp_path):
        """An unresolvable entry params expression belongs to the weave."""
        self._write(tmp_path, "t.thread", _VALID_THREAD)
        weave = self._weave_of(tmp_path, "w.weave", "ref: t.thread\n    params:\n      x: ${nope}")
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
        path.write_text(text)
        return path

    def test_weave_with_invalid_thread_raises(self, tmp_path):
        """A referenced thread that fails hydration fails the weave, naming the thread."""
        thread = self._write(tmp_path, "bad.thread", _VALID_THREAD + "write:\n  mode: bogus\n")
        weave = self._write(
            tmp_path, "w.weave", 'config_version: "1.0"\nthreads:\n  - ref: bad.thread\n'
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)

    def test_loom_with_invalid_thread_raises(self, tmp_path):
        """Two levels down, an invalid thread still fails the loom, naming the thread."""
        thread = self._write(tmp_path, "bad.thread", _VALID_THREAD + "write:\n  mode: bogus\n")
        self._write(tmp_path, "w.weave", 'config_version: "1.0"\nthreads:\n  - ref: bad.thread\n')
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "1.0"\nweaves:\n  - ref: w.weave\n'
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
            'config_version: "1.0"\nthreads:\n  - ref: t.thread\n'
            "lookups:\n  lk:\n    not_a_field: true\n",
        )
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "1.0"\nweaves:\n  - ref: w.weave\n'
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(weave)

    def test_invalid_inline_thread_names_weave(self, tmp_path):
        """An inline thread that fails hydration names the weave that defines it."""
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "1.0"\nthreads:\n'
            '  - name: t\n    config_version: "1.0"\n'
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
            tmp_path, "w.weave", 'config_version: "1.0"\nthreads:\n  - ref: t.thread\n'
        )
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "1.0"\nweaves:\n  - ref: w.weave\n'
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
        path.write_text(text)
        return path

    def test_weave_with_foreach_thread_loads(self, tmp_path):
        """A referenced thread using foreach no longer fails its weave."""
        self._write(tmp_path, "fe.thread", _FOREACH_THREAD)
        weave = self._write(
            tmp_path, "w.weave", 'config_version: "1.0"\nthreads:\n  - ref: fe.thread\n'
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)

    def test_loom_with_foreach_thread_loads(self, tmp_path):
        """The same holds two levels down."""
        self._write(tmp_path, "fe.thread", _FOREACH_THREAD)
        self._write(tmp_path, "w.weave", 'config_version: "1.0"\nthreads:\n  - ref: fe.thread\n')
        loom = self._write(
            tmp_path, "top.loom", 'config_version: "1.0"\nweaves:\n  - ref: w.weave\n'
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
            'config_version: "1.0"\nthreads:\n  - ref: fe.thread\n'
            "    params:\n      cols: [x, y]\n",
        )
        assert isinstance(load_config(weave, project_root=tmp_path), Weave)

    def test_inline_thread_with_foreach_loads(self, tmp_path):
        """An inline thread in a weave can use foreach too."""
        weave = self._write(
            tmp_path,
            "w.weave",
            'config_version: "1.0"\nthreads:\n'
            '  - name: t\n    config_version: "1.0"\n'
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
            tmp_path, "w.weave", 'config_version: "1.0"\nthreads:\n  - ref: fe.thread\n'
        )
        with pytest.raises(ConfigError, match="foreach block at step index") as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)


_INCREMENTAL_THREAD = (
    'config_version: "1.0"\n'
    "sources:\n  s:\n    type: delta\n    alias: raw.c\n"
    "target:\n  path: Tables/c\n"
    "write:\n  mode: append\n"
)


class TestLoadConfigWatermarkTypeRequired:
    """incremental_watermark needs watermark_type alongside watermark_column."""

    @staticmethod
    def _write(root: Path, name: str, text: str) -> Path:
        path = root / name
        path.write_text(text)
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
            'config_version: "1.0"\n'
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
            'config_version: "1.0"\n'
            "defaults:\n  load:\n    watermark_type: date\n"
            "threads:\n  - ref: t.thread\n",
        )
        with pytest.raises(ModelValidationError, match="watermark_type") as exc_info:
            load_config(weave, project_root=tmp_path)
        assert exc_info.value.file_path == str(thread)


_INLINE_THREAD_BODY = (
    '      - name: ops_audit\n        config_version: "1.0"\n'
    "        sources:\n          events:\n            type: delta\n"
    "            alias: raw.events\n        target:\n          alias: ops.audit\n"
)

_REF_THREAD = (
    'config_version: "1.0"\nsources:\n  data:\n    type: delta\n'
    "    alias: raw.orders\ntarget:\n  alias: stg.orders\n"
)


class TestLoadConfigInlineWeaves:
    """A loom's inline weave resolves like a weave file."""

    @staticmethod
    def _loom(tmp_path: Path, weaves: str) -> Path:
        loom = tmp_path / "x.loom"
        loom.write_text('config_version: "1.0"\nweaves:\n' + weaves)
        return loom

    def test_inline_weave_with_inline_thread_loads(self, tmp_path):
        """An inline weave defining its thread inline loads."""
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "1.0"\n    threads:\n' + _INLINE_THREAD_BODY,
        )
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)

    def test_inline_weave_with_referenced_thread_loads(self, tmp_path):
        """An inline weave referencing a thread file loads."""
        (tmp_path / "orders.thread").write_text(_REF_THREAD)
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "1.0"\n    threads:\n'
            "      - ref: orders.thread\n",
        )
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)

    def test_inline_weave_thread_failure_names_loom(self, tmp_path):
        """An invalid inline thread in an inline weave names the loom file."""
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "1.0"\n    threads:\n'
            + _INLINE_THREAD_BODY
            + "        write:\n          mode: bogus\n",
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(loom)

    def test_inline_weave_defaults_resolve_variables(self, tmp_path):
        """A ${var} defined in the inline weave's own defaults resolves."""
        loom = self._loom(
            tmp_path,
            '  - name: inline_ops\n    config_version: "1.0"\n'
            "    defaults:\n      tbl: ops.from_weave\n    threads:\n"
            '      - name: ops_audit\n        config_version: "1.0"\n'
            "        sources:\n          events:\n            type: delta\n"
            "            alias: raw.events\n        target:\n"
            '          alias: "${tbl}"\n',
        )
        assert isinstance(load_config(loom, project_root=tmp_path), Loom)

    def test_name_only_weave_without_file_raises(self, tmp_path):
        """A name-only weave entry with no file and no body names the missing file."""
        loom = self._loom(tmp_path, "  - name: missing\n")
        with pytest.raises(ReferenceResolutionError, match=r"missing\.weave") as exc_info:
            load_config(loom, project_root=tmp_path)
        assert exc_info.value.file_path == str(loom)

    def test_string_weave_entry_without_file_raises(self, tmp_path):
        """The string shorthand for a weave entry with no file also raises."""
        loom = self._loom(tmp_path, "  - missing\n")
        with pytest.raises(ReferenceResolutionError, match=r"missing\.weave"):
            load_config(loom, project_root=tmp_path)
