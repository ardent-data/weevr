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
