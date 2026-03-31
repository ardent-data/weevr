"""Tests for variable resolver."""

from pathlib import Path

import pytest

from weevr.config.resolver import build_param_context, resolve_variables, validate_params
from weevr.errors import ConfigSchemaError, VariableResolutionError

FIXTURES = Path(__file__).parent / "fixtures"


class TestBuildParamContext:
    """Test build_param_context function."""

    def test_runtime_params_only(self):
        """Build context with only runtime params."""
        runtime = {"env": "prod", "lakehouse": "gold"}
        context = build_param_context(runtime_params=runtime)
        assert context["env"] == "prod"
        assert context["lakehouse"] == "gold"

    def test_config_defaults_only(self):
        """Build context with only config defaults."""
        defaults = {"env": "dev", "mode": "test"}
        context = build_param_context(config_defaults=defaults)
        assert context["env"] == "dev"
        assert context["mode"] == "test"

    def test_runtime_overrides_defaults(self):
        """Runtime params should override config defaults."""
        runtime = {"env": "prod"}
        defaults = {"env": "dev", "lakehouse": "bronze"}
        context = build_param_context(runtime, defaults)
        assert context["env"] == "prod"  # Runtime wins
        assert context["lakehouse"] == "bronze"  # From defaults

    def test_full_priority_chain(self):
        """Test full priority: runtime > defaults."""
        runtime = {"env": "prod"}
        defaults = {"env": "dev", "lakehouse": "bronze", "mode": "test"}
        context = build_param_context(runtime, defaults)
        assert context["env"] == "prod"  # Runtime wins
        assert context["lakehouse"] == "bronze"  # Defaults (no override)
        assert context["mode"] == "test"  # Defaults (no override)

    def test_fabric_context_workspace_id(self):
        """Fabric context provides workspace_id as nested dict under 'fabric'."""
        fabric = {"fabric.workspace_id": "ws-abc-123"}
        context = build_param_context(fabric_context=fabric)
        assert context["fabric"]["workspace_id"] == "ws-abc-123"

    def test_fabric_context_lakehouse_id(self):
        """Fabric context provides lakehouse_id as nested dict under 'fabric'."""
        fabric = {"fabric.lakehouse_id": "lh-xyz-456"}
        context = build_param_context(fabric_context=fabric)
        assert context["fabric"]["lakehouse_id"] == "lh-xyz-456"

    def test_fabric_context_none_values_excluded(self):
        """Fabric context keys with None values are excluded from context."""
        fabric = {"fabric.workspace_id": "ws-abc-123", "fabric.lakehouse_id": None}
        context = build_param_context(fabric_context=fabric)
        assert context["fabric"]["workspace_id"] == "ws-abc-123"
        assert "lakehouse_id" not in context["fabric"]

    def test_fabric_context_all_none_excluded(self):
        """Fabric context with all None values produces no 'fabric' key."""
        fabric = {"fabric.workspace_id": None, "fabric.lakehouse_id": None}
        context = build_param_context(fabric_context=fabric)
        assert "fabric" not in context

    def test_runtime_param_overrides_fabric_context(self):
        """Explicit runtime param overrides fabric context value."""
        fabric = {"fabric.workspace_id": "ws-from-spark"}
        runtime = {"fabric": {"workspace_id": "ws-override"}}
        context = build_param_context(runtime_params=runtime, fabric_context=fabric)
        assert context["fabric"]["workspace_id"] == "ws-override"

    def test_config_default_overrides_fabric_context(self):
        """Config default overrides fabric context value."""
        fabric = {"fabric.workspace_id": "ws-from-spark"}
        defaults = {"fabric": {"workspace_id": "ws-from-config"}}
        context = build_param_context(config_defaults=defaults, fabric_context=fabric)
        assert context["fabric"]["workspace_id"] == "ws-from-config"

    def test_full_priority_chain_with_fabric(self):
        """Full priority: runtime > config_defaults > fabric_context."""
        fabric = {
            "fabric.workspace_id": "ws-spark",
            "fabric.lakehouse_id": "lh-spark",
            "fabric.workspace_name": "wks-spark",
        }
        defaults = {"fabric": {"workspace_id": "ws-config"}, "env": "dev"}
        runtime = {"fabric": {"workspace_id": "ws-runtime"}, "env": "prod"}
        context = build_param_context(
            runtime_params=runtime,
            config_defaults=defaults,
            fabric_context=fabric,
        )
        assert context["fabric"]["workspace_id"] == "ws-runtime"  # Runtime wins
        assert context["env"] == "prod"  # Runtime wins

    def test_fabric_context_none_param_no_error(self):
        """Passing None for fabric_context is valid and produces no fabric key."""
        context = build_param_context(fabric_context=None)
        assert "fabric" not in context


class TestFabricVariableResolution:
    """Test that ${fabric.*} variables resolve from fabric context."""

    def test_fabric_workspace_id_resolves(self):
        """${fabric.workspace_id} resolves from fabric context."""
        fabric = {"fabric.workspace_id": "ws-abc-123"}
        context = build_param_context(fabric_context=fabric)
        config = {"workspace": "${fabric.workspace_id}"}
        result = resolve_variables(config, context)
        assert result["workspace"] == "ws-abc-123"

    def test_fabric_lakehouse_id_resolves(self):
        """${fabric.lakehouse_id} resolves from fabric context."""
        fabric = {"fabric.lakehouse_id": "lh-xyz-456"}
        context = build_param_context(fabric_context=fabric)
        config = {"lakehouse": "${fabric.lakehouse_id}"}
        result = resolve_variables(config, context)
        assert result["lakehouse"] == "lh-xyz-456"

    def test_fabric_workspace_id_missing_raises(self):
        """${fabric.workspace_id} with no fabric context raises VariableResolutionError."""
        context = build_param_context()
        config = {"workspace": "${fabric.workspace_id}"}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        assert "fabric.workspace_id" in str(exc_info.value)

    def test_fabric_workspace_id_with_default_fallback(self):
        """${fabric.workspace_id:-fallback} uses default when fabric context absent."""
        context = build_param_context()
        config = {"workspace": "${fabric.workspace_id:-local-ws}"}
        result = resolve_variables(config, context)
        assert result["workspace"] == "local-ws"

    def test_fabric_context_in_embedded_string(self):
        """${fabric.workspace_id} embedded in longer string resolves correctly."""
        fabric = {"fabric.workspace_id": "ws-abc-123"}
        context = build_param_context(fabric_context=fabric)
        config = {"path": "abfss://data@storage/${fabric.workspace_id}/files"}
        result = resolve_variables(config, context)
        assert result["path"] == "abfss://data@storage/ws-abc-123/files"


class TestResolveVariables:
    """Test resolve_variables function."""

    def test_simple_variable(self):
        """Resolve simple variable reference."""
        config = {"path": "${base_path}/data"}
        context = {"base_path": "/mnt/storage"}
        result = resolve_variables(config, context)
        assert result["path"] == "/mnt/storage/data"

    def test_variable_with_default_not_used(self):
        """Variable with default should use context value when present."""
        config = {"env": "${environment:-dev}"}
        context = {"environment": "prod"}
        result = resolve_variables(config, context)
        assert result["env"] == "prod"  # Context value used

    def test_variable_with_default_used(self):
        """Variable with default should use default when not in context."""
        config = {"env": "${missing_var:-dev}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["env"] == "dev"  # Default used

    def test_dotted_key_access(self):
        """Resolve variable with dotted key access."""
        config = {"host": "${database.host}"}
        context = {"database": {"host": "localhost", "port": 5432}}
        result = resolve_variables(config, context)
        assert result["host"] == "localhost"

    def test_nested_dict_resolution(self):
        """Recursively resolve variables in nested dict."""
        config = {
            "source": {"path": "${base_path}/input"},
            "target": {"path": "${base_path}/output"},
        }
        context = {"base_path": "/data"}
        result = resolve_variables(config, context)
        assert result["source"]["path"] == "/data/input"
        assert result["target"]["path"] == "/data/output"

    def test_list_resolution(self):
        """Recursively resolve variables in list."""
        config = {"paths": ["${base}/input", "${base}/output"]}
        context = {"base": "/data"}
        result = resolve_variables(config, context)
        assert result["paths"][0] == "/data/input"
        assert result["paths"][1] == "/data/output"

    def test_multiple_variables_in_string(self):
        """Resolve multiple variables in one string."""
        config = {"url": "${protocol}://${host}:${port}"}
        context = {"protocol": "https", "host": "api.example.com", "port": "443"}
        result = resolve_variables(config, context)
        assert result["url"] == "https://api.example.com:443"

    def test_unresolved_variable_no_default(self):
        """Raise VariableResolutionError for missing variable without default."""
        config = {"path": "${missing_var}"}
        context = {}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        assert "Unresolved variable" in str(exc_info.value)
        assert "missing_var" in str(exc_info.value)

    def test_non_string_values_passthrough(self):
        """Non-string values should pass through unchanged."""
        config = {
            "count": 42,
            "enabled": True,
            "ratio": 3.14,
            "nullable": None,
        }
        context = {}
        result = resolve_variables(config, context)
        assert result["count"] == 42
        assert result["enabled"] is True
        assert result["ratio"] == 3.14
        assert result["nullable"] is None

    def test_empty_config(self):
        """Empty config should return empty."""
        config = {}
        context = {"var": "value"}
        result = resolve_variables(config, context)
        assert result == {}

    def test_empty_context(self):
        """Variables with defaults should work with empty context."""
        config = {"env": "${missing:-dev}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["env"] == "dev"


class TestValidateParams:
    """Test validate_params function."""

    def test_string_type_valid(self):
        """Validate string parameter."""
        specs = {"env": {"type": "string", "required": True}}
        context = {"env": "dev"}
        validate_params(specs, context)  # Should not raise

    def test_string_type_invalid(self):
        """Reject non-string for string type."""
        specs = {"env": {"type": "string", "required": True}}
        context = {"env": 123}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected string" in str(exc_info.value).lower()

    def test_int_type_valid(self):
        """Validate int parameter."""
        specs = {"port": {"type": "int", "required": True}}
        context = {"port": 5432}
        validate_params(specs, context)  # Should not raise

    def test_int_type_invalid(self):
        """Reject non-int for int type."""
        specs = {"port": {"type": "int", "required": True}}
        context = {"port": "5432"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected int" in str(exc_info.value).lower()

    def test_float_type_valid(self):
        """Validate float parameter."""
        specs = {"ratio": {"type": "float", "required": True}}
        context = {"ratio": 3.14}
        validate_params(specs, context)  # Should not raise

    def test_float_accepts_int(self):
        """Float type should accept int values."""
        specs = {"ratio": {"type": "float", "required": True}}
        context = {"ratio": 3}
        validate_params(specs, context)  # Should not raise

    def test_bool_type_valid(self):
        """Validate bool parameter."""
        specs = {"enabled": {"type": "bool", "required": True}}
        context = {"enabled": True}
        validate_params(specs, context)  # Should not raise

    def test_bool_type_invalid(self):
        """Reject non-bool for bool type."""
        specs = {"enabled": {"type": "bool", "required": True}}
        context = {"enabled": "true"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected bool" in str(exc_info.value).lower()

    def test_date_type_valid_string(self):
        """Validate date parameter as ISO string."""
        specs = {"start_date": {"type": "date", "required": True}}
        context = {"start_date": "2024-01-15"}
        validate_params(specs, context)  # Should not raise

    def test_date_type_invalid_format(self):
        """Reject invalid date format."""
        specs = {"start_date": {"type": "date", "required": True}}
        context = {"start_date": "01/15/2024"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "date format" in str(exc_info.value).lower()

    def test_timestamp_type_valid_string(self):
        """Validate timestamp parameter as ISO string."""
        specs = {"created_at": {"type": "timestamp", "required": True}}
        context = {"created_at": "2024-01-15T10:30:00"}
        validate_params(specs, context)  # Should not raise

    def test_timestamp_type_invalid_format(self):
        """Reject invalid timestamp format."""
        specs = {"created_at": {"type": "timestamp", "required": True}}
        context = {"created_at": "invalid-timestamp"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "timestamp format" in str(exc_info.value).lower()

    def test_list_string_type_valid(self):
        """Validate list[string] parameter."""
        specs = {"tags": {"type": "list[string]", "required": True}}
        context = {"tags": ["dev", "critical"]}
        validate_params(specs, context)  # Should not raise

    def test_list_string_type_not_list(self):
        """Reject non-list for list[string] type."""
        specs = {"tags": {"type": "list[string]", "required": True}}
        context = {"tags": "dev"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected list" in str(exc_info.value).lower()

    def test_list_string_type_non_string_items(self):
        """Reject list with non-string items."""
        specs = {"tags": {"type": "list[string]", "required": True}}
        context = {"tags": ["dev", 123]}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "list of strings" in str(exc_info.value).lower()

    def test_required_param_missing(self):
        """Raise error for missing required parameter."""
        specs = {"env": {"type": "string", "required": True}}
        context = {}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "Required parameter" in str(exc_info.value)
        assert "env" in str(exc_info.value)

    def test_required_param_with_default(self):
        """Use default for required param when missing."""
        specs = {"env": {"type": "string", "required": True, "default": "dev"}}
        context = {}
        validate_params(specs, context)
        assert context["env"] == "dev"  # Default applied

    def test_optional_param_missing(self):
        """Optional param missing should not raise."""
        specs = {"optional": {"type": "string", "required": False}}
        context = {}
        validate_params(specs, context)  # Should not raise

    def test_none_param_specs(self):
        """None param specs should not raise."""
        validate_params(None, {"env": "dev"})  # Should not raise

    def test_empty_param_specs(self):
        """Empty param specs should not raise."""
        validate_params({}, {"env": "dev"})  # Should not raise


class TestReferenceResolution:
    """Test reference resolution functions."""

    def test_resolve_ref_path_thread(self, tmp_path):
        """Resolve path-based thread reference."""
        from weevr.config.resolver import resolve_ref_path

        thread_file = tmp_path / "dimensions" / "dim_customer.thread"
        thread_file.parent.mkdir(parents=True)
        thread_file.write_text('config_version: "1.0"\nsources:\n  data:\n    type: delta\n')
        result = resolve_ref_path("dimensions/dim_customer.thread", tmp_path)
        assert result == thread_file.resolve()

    def test_resolve_ref_path_weave(self, tmp_path):
        """Resolve path-based weave reference."""
        from weevr.config.resolver import resolve_ref_path

        weave_file = tmp_path / "dimensions.weave"
        weave_file.write_text('config_version: "1.0"\nthreads: []\n')
        result = resolve_ref_path("dimensions.weave", tmp_path)
        assert result == weave_file.resolve()

    def test_resolve_ref_path_missing(self, tmp_path):
        """Raise ReferenceResolutionError for missing file."""
        from weevr.config.resolver import resolve_ref_path
        from weevr.errors import ReferenceResolutionError

        with pytest.raises(ReferenceResolutionError, match="not found"):
            resolve_ref_path("missing.thread", tmp_path)

    def test_resolve_ref_path_bad_extension(self, tmp_path):
        """Raise ConfigError for unsupported extension."""
        from weevr.config.resolver import resolve_ref_path
        from weevr.errors import ConfigError

        with pytest.raises(ConfigError, match="Unsupported extension"):
            resolve_ref_path("bad.yaml", tmp_path)

    def test_resolve_ref_path_traversal_rejected(self, tmp_path):
        """Raise ReferenceResolutionError when ref escapes the project root."""
        from weevr.config.resolver import resolve_ref_path
        from weevr.errors import ReferenceResolutionError

        project_root = tmp_path / "project.weevr"
        project_root.mkdir()

        with pytest.raises(ReferenceResolutionError, match="resolves outside project root"):
            resolve_ref_path("../escape.thread", project_root)

    def test_resolve_references_loom_to_weave_to_thread(self, tmp_path):
        """Resolve full hierarchy via ref entries: loom -> weave -> thread."""
        from weevr.config.resolver import resolve_references

        # Create thread file
        thread_file = tmp_path / "dim_customer.thread"
        thread_file.write_text(
            'config_version: "1.0"\nsources:\n  customers:\n    type: delta\n'
            "    alias: raw_customers\ntarget: {}\n"
        )

        # Create weave file referencing the thread
        weave_file = tmp_path / "dimensions.weave"
        weave_file.write_text('config_version: "1.0"\nthreads:\n  - ref: dim_customer.thread\n')

        loom_config = {
            "config_version": "1.0",
            "weaves": [{"ref": "dimensions.weave"}],
        }

        result = resolve_references(loom_config, "loom", tmp_path)

        assert "_resolved_weaves" in result
        assert len(result["_resolved_weaves"]) == 1
        weave = result["_resolved_weaves"][0]
        assert "_resolved_threads" in weave
        assert len(weave["_resolved_threads"]) == 1
        thread = weave["_resolved_threads"][0]
        assert "customers" in thread["sources"]

    def test_resolve_references_missing_file(self, tmp_path):
        """Raise ReferenceResolutionError for missing referenced file."""
        from weevr.config.resolver import resolve_references
        from weevr.errors import ReferenceResolutionError

        config = {
            "config_version": "1.0",
            "weaves": [{"ref": "nonexistent.weave"}],
        }

        with pytest.raises(ReferenceResolutionError) as exc_info:
            resolve_references(config, "loom", tmp_path)
        assert "not found" in str(exc_info.value)

    def test_resolve_variables_var_namespace_passthrough(self):
        """var.* references pass through resolve_variables unchanged."""
        config = {"alias": "${var.target_schema}.customers"}
        context = {}
        result = resolve_variables(config, context)
        assert result["alias"] == "${var.target_schema}.customers"

    def test_resolve_variables_var_namespace_mixed(self):
        """var.* references coexist with resolved variables."""
        config = {"path": "${base_path}/${var.filename}"}
        context = {"base_path": "/data"}
        result = resolve_variables(config, context)
        assert result["path"] == "/data/${var.filename}"

    def test_convention_based_loom_weave_resolution(self, tmp_path):
        """Loom name-only weave entries resolve via {name}.weave convention."""
        from weevr.config.resolver import resolve_references

        # Create a weave file at the project root
        weave_file = tmp_path / "dimensions.weave"
        weave_file.write_text('config_version: "1.0"\nthreads: []\n')

        loom_config = {
            "config_version": "1.0",
            "weaves": [{"name": "dimensions"}],
        }

        result = resolve_references(loom_config, "loom", tmp_path)

        assert "_resolved_weaves" in result
        assert len(result["_resolved_weaves"]) == 1
        assert result["_resolved_weaves"][0]["name"] == "dimensions"
        assert result["_resolved_weaves"][0]["qualified_key"] == "dimensions.weave"


class TestWholeValueResolution:
    """Test that whole-value ${param} references return native Python types."""

    def test_whole_value_list(self):
        """${param} resolving to list returns list, not string."""
        config = {"match_keys": "${pk_columns}"}
        context = {"pk_columns": ["mandt", "color"]}
        result = resolve_variables(config, context)
        assert result["match_keys"] == ["mandt", "color"]
        assert isinstance(result["match_keys"], list)

    def test_whole_value_int(self):
        """${param} resolving to int returns int."""
        config = {"page_size": "${batch_size}"}
        context = {"batch_size": 1000}
        result = resolve_variables(config, context)
        assert result["page_size"] == 1000
        assert isinstance(result["page_size"], int)

    def test_whole_value_bool_true(self):
        """${param} resolving to bool True returns bool."""
        config = {"enabled": "${flag_enabled}"}
        context = {"flag_enabled": True}
        result = resolve_variables(config, context)
        assert result["enabled"] is True
        assert isinstance(result["enabled"], bool)

    def test_whole_value_bool_false(self):
        """${param} resolving to bool False returns bool."""
        config = {"debug": "${flag_debug}"}
        context = {"flag_debug": False}
        result = resolve_variables(config, context)
        assert result["debug"] is False
        assert isinstance(result["debug"], bool)

    def test_whole_value_none(self):
        """${param} resolving to None returns None, not string 'None'."""
        config = {"filter": "${optional_filter}"}
        context = {"optional_filter": None}
        result = resolve_variables(config, context)
        assert result["filter"] is None

    def test_whole_value_with_default_returns_string(self):
        """${param:-default} where param missing returns default as string."""
        config = {"mode": "${missing_param:-batch}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["mode"] == "batch"
        assert isinstance(result["mode"], str)

    def test_embedded_ref_still_returns_string(self):
        """Embedded ${param} in a longer string always returns string."""
        config = {"alias": "SAP.${table_name}"}
        context = {"table_name": "MARA"}
        result = resolve_variables(config, context)
        assert result["alias"] == "SAP.MARA"
        assert isinstance(result["alias"], str)

    def test_whole_value_var_namespace_passthrough(self):
        """${var.x} whole-value reference still passes through unchanged."""
        config = {"schema": "${var.target_schema}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["schema"] == "${var.target_schema}"

    def test_whole_value_run_namespace_passthrough(self):
        """${run.x} whole-value reference still passes through unchanged."""
        config = {"ts": "${run.timestamp}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["ts"] == "${run.timestamp}"

    def test_whole_value_missing_no_default_raises(self):
        """${param} where param missing and no default raises VariableResolutionError."""
        config = {"keys": "${undefined_param}"}
        context = {}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        assert "Unresolved variable" in str(exc_info.value)
        assert "undefined_param" in str(exc_info.value)


_MINIMAL_THREAD = {
    "config_version": "1.0",
    "sources": {"data": {"type": "delta", "alias": "raw.data"}},
    "target": {"alias": "test"},
}


class TestStandaloneThreadResources:
    """Test that standalone .thread files with inline resources parse correctly."""

    def test_standalone_thread_with_inline_lookups(self):
        """Standalone thread with inline lookups field parses correctly."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "lookups": {
                "country_ref": {
                    "source": {"type": "delta", "alias": "dim.country"},
                    "materialize": True,
                    "key": ["country_code"],
                }
            },
        }
        thread = Thread.model_validate(data)

        assert thread.lookups is not None
        assert "country_ref" in thread.lookups
        lookup = thread.lookups["country_ref"]
        assert lookup.materialize is True
        assert lookup.key == ["country_code"]
        assert lookup.source.alias == "dim.country"

    def test_standalone_thread_with_inline_column_sets(self):
        """Standalone thread with inline column_sets field parses correctly."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "column_sets": {
                "rename_map": {
                    "source": {
                        "type": "delta",
                        "alias": "config.column_mapping",
                    }
                }
            },
        }
        thread = Thread.model_validate(data)

        assert thread.column_sets is not None
        assert "rename_map" in thread.column_sets
        cs = thread.column_sets["rename_map"]
        assert cs.source is not None
        assert cs.source.alias == "config.column_mapping"

    def test_standalone_thread_with_inline_params(self):
        """Standalone thread with inline params field parses correctly."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {
                "env": {"name": "env", "type": "string", "required": True},
                "batch_size": {
                    "name": "batch_size",
                    "type": "int",
                    "required": False,
                    "default": 1000,
                },
            },
        }
        thread = Thread.model_validate(data)

        assert thread.params is not None
        assert "env" in thread.params
        assert "batch_size" in thread.params
        assert thread.params["env"].type == "string"
        assert thread.params["env"].required is True
        assert thread.params["batch_size"].default == 1000

    def test_standalone_thread_with_inline_variables(self):
        """Standalone thread with inline variables field parses correctly."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "variables": {
                "row_count": {"type": "int"},
                "run_date": {"type": "date", "default": "2024-01-01"},
            },
        }
        thread = Thread.model_validate(data)

        assert thread.variables is not None
        assert "row_count" in thread.variables
        assert "run_date" in thread.variables
        assert thread.variables["row_count"].type == "int"
        assert thread.variables["run_date"].default == "2024-01-01"

    def test_standalone_thread_with_inline_pre_and_post_steps(self):
        """Standalone thread with inline pre_steps and post_steps parses correctly."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "pre_steps": [{"type": "log_message", "message": "Starting thread execution"}],
            "post_steps": [{"type": "sql_statement", "sql": "OPTIMIZE silver.data"}],
        }
        thread = Thread.model_validate(data)

        assert thread.pre_steps is not None
        assert len(thread.pre_steps) == 1
        pre = thread.pre_steps[0]
        assert pre.type == "log_message"
        assert pre.message == "Starting thread execution"

        assert thread.post_steps is not None
        assert len(thread.post_steps) == 1
        post = thread.post_steps[0]
        assert post.type == "sql_statement"
        assert post.sql == "OPTIMIZE silver.data"

    def test_standalone_thread_with_all_resources(self):
        """Standalone thread can carry all inline resource types simultaneously."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {"env": {"name": "env", "type": "string", "required": True}},
            "lookups": {
                "ref_data": {
                    "source": {"type": "delta", "alias": "dim.ref"},
                    "materialize": False,
                }
            },
            "column_sets": {"col_map": {"source": {"type": "delta", "alias": "cfg.cols"}}},
            "variables": {"status": {"type": "string"}},
            "pre_steps": [{"type": "log_message", "message": "pre"}],
            "post_steps": [{"type": "log_message", "message": "post"}],
        }
        thread = Thread.model_validate(data)

        assert thread.params is not None and "env" in thread.params
        assert thread.lookups is not None and "ref_data" in thread.lookups
        assert thread.column_sets is not None and "col_map" in thread.column_sets
        assert thread.variables is not None and "status" in thread.variables
        assert thread.pre_steps is not None and len(thread.pre_steps) == 1
        assert thread.post_steps is not None and len(thread.post_steps) == 1

    def test_param_validation_required_param_missing_raises(self):
        """validate_params raises ConfigSchemaError when a required thread param is absent."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {"env": {"name": "env", "type": "string", "required": True}},
        }
        thread = Thread.model_validate(data)

        # Build param specs dict from thread.params (as validate_params expects)
        assert thread.params is not None
        param_specs = {k: v for k, v in thread.params.items()}
        context: dict = {}

        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(param_specs, context)
        assert "Required parameter" in str(exc_info.value)
        assert "env" in str(exc_info.value)

    def test_param_validation_type_mismatch_raises(self):
        """validate_params raises ConfigSchemaError when a thread param has wrong type."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {"count": {"name": "count", "type": "int", "required": True}},
        }
        thread = Thread.model_validate(data)

        assert thread.params is not None
        param_specs = {k: v for k, v in thread.params.items()}
        context = {"count": "not_an_int"}

        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(param_specs, context)
        assert "expected int" in str(exc_info.value).lower()

    def test_param_validation_with_defaults_passes(self):
        """validate_params uses default when required param is absent but has a default."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {
                "mode": {
                    "name": "mode",
                    "type": "string",
                    "required": True,
                    "default": "batch",
                }
            },
        }
        thread = Thread.model_validate(data)

        assert thread.params is not None
        param_specs = {k: v for k, v in thread.params.items()}
        context: dict = {}

        validate_params(param_specs, context)  # Should not raise
        assert context["mode"] == "batch"

    def test_variable_resolution_within_standalone_thread(self):
        """${param} references inside a standalone thread config resolve correctly."""
        config = {
            "sources": {
                "data": {
                    "type": "delta",
                    "alias": "${lakehouse}.customers",
                }
            },
            "target": {"alias": "${lakehouse}.output"},
        }
        context = {"lakehouse": "silver"}
        resolved = resolve_variables(config, context)

        assert resolved["sources"]["data"]["alias"] == "silver.customers"
        assert resolved["target"]["alias"] == "silver.output"

    def test_whole_value_whitespace_stripped(self):
        """Whole-value match works even when value has surrounding whitespace."""
        config = {"keys": "  ${pk_columns}  "}
        context = {"pk_columns": ["id", "code"]}
        result = resolve_variables(config, context)
        assert result["keys"] == ["id", "code"]
        assert isinstance(result["keys"], list)


class TestRunVariableSkip:
    """Test that ${run.*} variables are preserved at config-load time."""

    def test_run_timestamp_preserved(self):
        """${run.timestamp} is not resolved at config-load time."""
        config = {"path": "/archive/${run.timestamp}/data"}
        result = resolve_variables(config, {})
        assert result["path"] == "/archive/${run.timestamp}/data"

    def test_run_id_preserved(self):
        """${run.id} is not resolved at config-load time."""
        config = {"path": "/export/${run.id}"}
        result = resolve_variables(config, {})
        assert result["path"] == "/export/${run.id}"

    def test_run_vars_in_nested_config(self):
        """${run.*} preserved in nested structures."""
        config = {
            "exports": [
                {"name": "archive", "path": "/data/${run.timestamp}/${run.id}"},
            ],
        }
        result = resolve_variables(config, {})
        assert "${run.timestamp}" in result["exports"][0]["path"]
        assert "${run.id}" in result["exports"][0]["path"]

    def test_run_and_regular_vars_mixed(self):
        """${run.*} preserved while regular vars are resolved."""
        config = {"path": "/lakehouse/${env}/${run.timestamp}"}
        result = resolve_variables(config, {"env": "prod"})
        assert result["path"] == "/lakehouse/prod/${run.timestamp}"
