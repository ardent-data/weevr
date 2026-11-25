"""Tests for variable resolver."""

import dataclasses
from pathlib import Path

import pytest

from documents import write_document, write_project
from weevr.config.audit_templates import (
    CONTEXT_VARIABLE_FIELDS,
    HIERARCHY_CONTEXT_KEYS,
    AuditContext,
    resolve_context_variables,
)
from weevr.config.interpolation import resolve_variables
from weevr.config.interpolation_errors import ReferenceScope
from weevr.config.params import ParamScope
from weevr.config.scoping import scope_context
from weevr.diagnostics import Diagnostic
from weevr.errors import ConfigSchemaError, VariableResolutionError

FIXTURES = Path(__file__).parent / "fixtures"


class TestScopeContext:
    """The load-time namespaces: the scope's bound params and the Fabric context."""

    def test_runtime_params_bind_under_param(self):
        """Runtime params reach ``${param.x}`` as the root scope's bindings, not flat."""
        runtime = {"env": "prod", "lakehouse": "gold"}
        context = scope_context(ParamScope.root(runtime))
        assert context == {"param": runtime}

    def test_fabric_context_workspace_id(self):
        """Fabric context provides workspace_id as nested dict under 'fabric'."""
        fabric = {"fabric.workspace_id": "ws-abc-123"}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        assert context["fabric"]["workspace_id"] == "ws-abc-123"

    def test_fabric_context_lakehouse_id(self):
        """Fabric context provides lakehouse_id as nested dict under 'fabric'."""
        fabric = {"fabric.lakehouse_id": "lh-xyz-456"}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        assert context["fabric"]["lakehouse_id"] == "lh-xyz-456"

    def test_fabric_context_none_values_excluded(self):
        """Fabric context keys with None values are excluded from context."""
        fabric = {"fabric.workspace_id": "ws-abc-123", "fabric.lakehouse_id": None}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        assert context["fabric"]["workspace_id"] == "ws-abc-123"
        assert "lakehouse_id" not in context["fabric"]

    def test_fabric_context_all_none_is_an_empty_namespace(self):
        """A context with only None values is still a context, with no values."""
        fabric = {"fabric.workspace_id": None, "fabric.lakehouse_id": None}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        assert context["fabric"] == {}

    def test_runtime_fabric_key_binds_as_a_param(self):
        """A runtime ``fabric`` key is a param binding, never the fabric namespace."""
        fabric = {"fabric.workspace_id": "ws-from-spark"}
        runtime = {"fabric": {"workspace_id": "ws-override"}}
        context = scope_context(ParamScope.root(runtime), fabric_context=fabric)
        assert context["fabric"] == {"workspace_id": "ws-from-spark"}
        assert context["param"] == runtime

    def test_fabric_context_none_param_no_error(self):
        """Passing None for fabric_context is valid and produces no fabric key."""
        context = scope_context(ParamScope.root(), fabric_context=None)
        assert "fabric" not in context

    def test_scope_bindings_nest_under_param_key(self):
        """A scope's bound values nest under context['param'] and resolve dotted."""
        scope = ParamScope.root().child(None, pins={"table": "ADR6"})
        context = scope_context(scope)
        assert context == {"param": {"table": "ADR6"}}
        assert resolve_variables({"x": "${param.table}"}, context) == {"x": "ADR6"}

    def test_empty_scope_adds_no_param_key(self):
        """A scope with no bindings adds no 'param' key."""
        assert "param" not in scope_context(ParamScope.root())

    def test_runtime_param_named_param_does_not_replace_namespace(self):
        """A runtime param named 'param' is one binding, not the namespace."""
        runtime = {"param": {"table": "OVERRIDE"}}
        scope = ParamScope.root(runtime).child(None, pins={"table": "ADR6"})
        context = scope_context(scope)
        assert context["param"]["table"] == "ADR6"


class TestConsumedKeysTracking:
    """Test consumed_keys param tracking in resolve_variables (M121 Task 12)."""

    def test_param_keys_tracked(self):
        """${param.x} and ${param.y} usage tracked in consumed_keys."""
        context = {"param": {"x": 1, "y": 2}}
        consumed: set[str] = set()
        resolve_variables(
            {"a": "${param.x}", "b": "${param.y}"},
            context,
            consumed_keys=consumed,
        )
        assert consumed == {"param.x", "param.y"}

    def test_duplicate_usage_idempotent(self):
        """Same param used twice produces single entry in consumed_keys."""
        context = {"param": {"x": 1}}
        consumed: set[str] = set()
        resolve_variables(
            {"a": "${param.x}", "b": "${param.x}"},
            context,
            consumed_keys=consumed,
        )
        assert consumed == {"param.x"}

    def test_no_param_refs_empty_set(self):
        """Config with no param references yields empty consumed_keys."""
        context = {"param": {"env": "prod"}}
        consumed: set[str] = set()
        resolve_variables({"a": "${param.env}"}, context, consumed_keys=consumed)
        assert consumed == {"param.env"}

    def test_var_namespace_not_tracked(self):
        """${var.x} references are runtime-deferred, not tracked."""
        context = {"param": {"env": "prod"}}
        consumed: set[str] = set()
        resolve_variables(
            {"a": "${param.env}", "b": "${var.x}"},
            context,
            consumed_keys=consumed,
        )
        assert consumed == {"param.env"}

    def test_consumed_keys_none_no_error(self):
        """consumed_keys=None (default) causes no tracking or error."""
        context = {"param": {"env": "prod"}}
        result = resolve_variables({"a": "${param.env}"}, context)
        assert result == {"a": "prod"}


class TestUnusedParamWarnings:
    """An entry pin the referenced thread never reads warns ``P123``."""

    def test_unused_entry_param_warns(self, tmp_path):
        """Entry param not consumed by thread emits P123 at the entry."""
        from resolving import resolve_tree

        thread_file = tmp_path / "simple.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "simple.thread",
                    "as": "inst",
                    "params": {"unused_key": "val"},
                },
            ],
        }
        warnings: list = []
        resolve_tree(weave_config, "weave", tmp_path, warnings=warnings)
        assert [(d.code, d.pointer) for d in warnings] == [("P123", "/threads/0/params/unused_key")]

    def test_all_params_consumed_no_warning(self, tmp_path):
        """All entry params consumed by thread emits no warning."""
        from resolving import resolve_tree

        thread_file = tmp_path / "parameterized.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.table}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "parameterized.thread",
                    "as": "inst",
                    "params": {"table": "ADR6"},
                },
            ],
        }
        warnings: list = []
        resolve_tree(weave_config, "weave", tmp_path, warnings=warnings)
        assert warnings == []

    def test_shadowed_default_no_warning(self, tmp_path):
        """A thread with no entry pins emits no warning."""
        from resolving import resolve_tree

        thread_file = tmp_path / "with_default.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.env:-dev}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {"ref": "with_default.thread"},
            ],
        }
        warnings: list = []
        resolve_tree(weave_config, "weave", tmp_path, warnings=warnings)
        assert warnings == []


class TestStandaloneTemplateErrors:
    """Test enhanced error messages for standalone templates (M121 Task 14)."""

    def test_standalone_unresolved_param_hints_template(self):
        """Standalone thread with ${param.x} raises with param.table in msg."""
        context = {}
        config = {"alias": "${param.table}"}
        with pytest.raises(VariableResolutionError, match="param.table"):
            resolve_variables(config, context)

    def test_ref_loaded_thread_unresolved_param_shows_ref(self, tmp_path):
        """Thread loaded via ref with unresolved ${param.x} raises with ref."""
        from resolving import resolve_tree

        thread_file = tmp_path / "template.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.table}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [{"ref": "template.thread"}],
        }
        with pytest.raises(VariableResolutionError, match="param.table"):
            resolve_tree(weave_config, "weave", tmp_path)


class TestFabricVariableResolution:
    """Test that ${fabric.*} variables resolve from fabric context."""

    def test_fabric_workspace_id_resolves(self):
        """${fabric.workspace_id} resolves from fabric context."""
        fabric = {"fabric.workspace_id": "ws-abc-123"}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        config = {"workspace": "${fabric.workspace_id}"}
        result = resolve_variables(config, context)
        assert result["workspace"] == "ws-abc-123"

    def test_fabric_lakehouse_id_resolves(self):
        """${fabric.lakehouse_id} resolves from fabric context."""
        fabric = {"fabric.lakehouse_id": "lh-xyz-456"}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        config = {"lakehouse": "${fabric.lakehouse_id}"}
        result = resolve_variables(config, context)
        assert result["lakehouse"] == "lh-xyz-456"

    def test_fabric_workspace_id_missing_raises(self):
        """${fabric.workspace_id} a supplied fabric context lacks raises ``P110``."""
        context = {"fabric": {}}
        config = {"workspace": "${fabric.workspace_id}"}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        assert [d.code for d in exc_info.value.diagnostics] == ["P110"]

    def test_fabric_reference_without_context_is_deferred(self):
        """With no fabric namespace the reference stays in place, noted as ``P113``."""
        deferred: list[Diagnostic] = []
        config = {"workspace": "${fabric.workspace_id}"}
        assert resolve_variables(config, {}, deferred=deferred) == config
        assert [(d.code, d.pointer) for d in deferred] == [("P113", "/workspace")]

    def test_fabric_workspace_id_with_default_fallback(self):
        """${fabric.workspace_id:-fallback} uses default when fabric context absent."""
        context = {}
        config = {"workspace": "${fabric.workspace_id:-local-ws}"}
        result = resolve_variables(config, context)
        assert result["workspace"] == "local-ws"

    def test_fabric_context_in_embedded_string(self):
        """${fabric.workspace_id} embedded in longer string resolves correctly."""
        fabric = {"fabric.workspace_id": "ws-abc-123"}
        context = scope_context(ParamScope.root(), fabric_context=fabric)
        config = {"path": "abfss://data@storage/${fabric.workspace_id}/files"}
        result = resolve_variables(config, context)
        assert result["path"] == "abfss://data@storage/ws-abc-123/files"


class TestResolveVariables:
    """Test resolve_variables function."""

    def test_simple_variable(self):
        """Resolve simple variable reference."""
        config = {"path": "${param.base_path}/data"}
        context = {"param": {"base_path": "/mnt/storage"}}
        result = resolve_variables(config, context)
        assert result["path"] == "/mnt/storage/data"

    def test_variable_with_default_not_used(self):
        """Variable with default should use context value when present."""
        config = {"env": "${param.environment:-dev}"}
        context = {"param": {"environment": "prod"}}
        result = resolve_variables(config, context)
        assert result["env"] == "prod"  # Context value used

    def test_variable_with_default_used(self):
        """Variable with default should use default when not in context."""
        config = {"env": "${param.missing_var:-dev}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["env"] == "dev"  # Default used

    def test_dotted_key_access(self):
        """Resolve variable with dotted key access."""
        config = {"host": "${param.database.host}"}
        context = {"param": {"database": {"host": "localhost", "port": 5432}}}
        result = resolve_variables(config, context)
        assert result["host"] == "localhost"

    def test_nested_dict_resolution(self):
        """Recursively resolve variables in nested dict."""
        config = {
            "source": {"path": "${param.base_path}/input"},
            "target": {"path": "${param.base_path}/output"},
        }
        context = {"param": {"base_path": "/data"}}
        result = resolve_variables(config, context)
        assert result["source"]["path"] == "/data/input"
        assert result["target"]["path"] == "/data/output"

    def test_list_resolution(self):
        """Recursively resolve variables in list."""
        config = {"paths": ["${param.base}/input", "${param.base}/output"]}
        context = {"param": {"base": "/data"}}
        result = resolve_variables(config, context)
        assert result["paths"][0] == "/data/input"
        assert result["paths"][1] == "/data/output"

    def test_multiple_variables_in_string(self):
        """Resolve multiple variables in one string."""
        config = {"url": "${param.protocol}://${param.host}:${param.port}"}
        context = {"param": {"protocol": "https", "host": "api.example.com", "port": "443"}}
        result = resolve_variables(config, context)
        assert result["url"] == "https://api.example.com:443"

    def test_unresolved_variable_no_default(self):
        """Raise VariableResolutionError for missing variable without default."""
        config = {"path": "${param.missing_var}"}
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
        config = {"env": "${param.missing:-dev}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["env"] == "dev"


class TestDeclaredParamsAndDefaults:
    """Declared params bind in the scope; ``defaults`` stay out of the variables."""

    def test_defaults_and_declared_params_independent(self):
        """``defaults`` are not variables: a bare reference to one fails ``P111``."""
        param_specs = {
            "workspace_id": {
                "name": "workspace_id",
                "type": "string",
                "required": True,
            }
        }
        config_defaults = {"env": "dev"}
        runtime = {"workspace_id": "ws-1"}
        scope = ParamScope.root(runtime).child(param_specs)
        context = scope_context(scope)
        assert resolve_variables({"w": "${param.workspace_id}"}, context) == {"w": "ws-1"}
        ref_scope = ReferenceScope(
            declared={n: s.declaration(n) for n, s in scope.specs.items()},
            defaults=config_defaults,
        )
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables({"lakehouse": "${env}"}, context, scope=ref_scope)
        assert [d.code for d in exc_info.value.diagnostics] == ["P111"]
        assert exc_info.value.diagnostics[0].expected == ("${param.env}",)


class TestUnresolvedParamHints:
    """Improved error message for unresolved ${param.x} in same scope."""

    def test_unresolved_param_lists_available_names(self):
        """Error message lists declared param names for typo detection."""
        # Loom declares `workspace_id`; YAML mistypes as `workspaceid`.
        context = {"param": {"workspace_id": "ws-1"}}
        config = {"workspace": "${param.workspaceid}"}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        msg = str(exc_info.value)
        assert "param.workspaceid" in msg
        assert "workspace_id" in msg  # the available declared name


class TestReferenceResolution:
    """Test reference resolution functions."""

    def test_resolve_ref_path_thread(self, tmp_path):
        """Resolve path-based thread reference."""
        from weevr.config.resolver import resolve_ref_path

        thread_file = tmp_path / "dimensions" / "dim_customer.thread"
        thread_file.parent.mkdir(parents=True)
        write_document(thread_file, 'config_version: "2.0"\nsources:\n  data:\n    type: delta\n')
        result = resolve_ref_path("dimensions/dim_customer.thread", tmp_path)
        assert str(result) == str(tmp_path / "dimensions" / "dim_customer.thread")

    def test_resolve_ref_path_weave(self, tmp_path):
        """Resolve path-based weave reference."""
        from weevr.config.resolver import resolve_ref_path

        weave_file = tmp_path / "dimensions.weave"
        write_document(weave_file, 'config_version: "2.0"\nthreads: []\n')
        result = resolve_ref_path("dimensions.weave", tmp_path)
        assert str(result) == str(weave_file)

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

    @pytest.mark.parametrize("ref", [5, None, ["a.thread"], {"path": "a.thread"}])
    def test_resolve_ref_path_rejects_non_string(self, tmp_path, ref):
        """A ref that isn't a string fails as a reference error naming no file of its own."""
        from weevr.config.resolver import resolve_ref_path
        from weevr.errors import ReferenceResolutionError

        with pytest.raises(ReferenceResolutionError, match="must be a path string") as caught:
            resolve_ref_path(ref, tmp_path)
        # The enclosing config's file is attached by attribute_errors.
        assert caught.value.file_path is None

    def test_resolve_ref_path_traversal_rejected(self, tmp_path):
        """Raise ReferenceResolutionError when ref escapes the project root."""
        from weevr.config.resolver import resolve_ref_path
        from weevr.errors import ReferenceResolutionError

        project_root = write_project(tmp_path / "project.weevr")

        with pytest.raises(ReferenceResolutionError, match="resolves outside project root"):
            resolve_ref_path("../escape.thread", project_root)

    def test_resolve_ref_path_accepts_config_location(self, tmp_path):
        """resolve_ref_path accepts a LocalConfigLocation directly."""
        from weevr.config.locations import LocalConfigLocation
        from weevr.config.resolver import resolve_ref_path

        thread_file = tmp_path / "dim.thread"
        write_document(thread_file, 'config_version: "2.0"\n')
        loc = LocalConfigLocation(tmp_path)
        result = resolve_ref_path("dim.thread", loc)
        assert isinstance(result, LocalConfigLocation)
        assert str(result) == str(thread_file)

    def test_resolve_references_loom_to_weave_to_thread(self, tmp_path):
        """Resolve full hierarchy via ref entries: loom -> weave -> thread."""
        from resolving import resolve_tree

        # Create thread file
        thread_file = tmp_path / "dim_customer.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\nsources:\n  customers:\n    type: delta\n'
            "    alias: raw_customers\ntarget: {}\n",
        )

        # Create weave file referencing the thread
        weave_file = tmp_path / "dimensions.weave"
        write_document(
            weave_file, 'config_version: "2.0"\nthreads:\n  - ref: dim_customer.thread\n'
        )

        loom_config = {
            "config_version": "2.0",
            "weaves": [{"ref": "dimensions.weave"}],
        }

        result = resolve_tree(loom_config, "loom", tmp_path)

        assert len(result) == 1
        weave = result[0]
        assert weave.source_path == str(tmp_path / "dimensions.weave")
        assert len(weave.children) == 1
        thread = weave.children[0].data
        assert "customers" in thread["sources"]

    def test_resolve_references_missing_file(self, tmp_path):
        """Raise ReferenceResolutionError for missing referenced file."""
        from resolving import resolve_tree
        from weevr.errors import ReferenceResolutionError

        config = {
            "config_version": "2.0",
            "weaves": [{"ref": "nonexistent.weave"}],
        }

        with pytest.raises(ReferenceResolutionError) as exc_info:
            resolve_tree(config, "loom", tmp_path)
        assert "not found" in str(exc_info.value)

    def test_resolve_variables_var_namespace_passthrough(self):
        """var.* references pass through resolve_variables unchanged."""
        config = {"alias": "${var.target_schema}.customers"}
        context = {}
        result = resolve_variables(config, context)
        assert result["alias"] == "${var.target_schema}.customers"

    def test_resolve_variables_var_namespace_mixed(self):
        """var.* references coexist with resolved variables."""
        config = {"path": "${param.base_path}/${var.filename}"}
        context = {"param": {"base_path": "/data"}}
        result = resolve_variables(config, context)
        assert result["path"] == "/data/${var.filename}"

    def test_ref_loom_weave_resolution(self, tmp_path):
        """Loom ref weave entries resolve the referenced file."""
        from resolving import resolve_tree

        # Create a weave file at the project root
        weave_file = tmp_path / "dimensions.weave"
        write_document(weave_file, 'config_version: "2.0"\nthreads: []\n')

        loom_config = {
            "config_version": "2.0",
            "weaves": [{"ref": "dimensions.weave"}],
        }

        result = resolve_tree(loom_config, "loom", tmp_path)

        assert len(result) == 1
        assert result[0].data["name"] == "dimensions"
        assert result[0].data["qualified_key"] == "dimensions.weave"


class TestEntryStashPattern:
    """Entry fields (``as``, ``params``) travel on the resolved child, not in its dict."""

    def test_ref_entry_with_as_stashed(self, tmp_path):
        """A ref entry's 'as' is on the child's entry; the dict carries no stash key."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [{"ref": "sap_table.thread", "as": "sap_adr6"}],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        assert result[0].entry["as"] == "sap_adr6"
        assert result[0].source_path == str(thread_file)
        assert not any(key.startswith("_") for key in result[0].data)

    def test_ref_entry_with_params_stashed(self, tmp_path):
        """A ref entry's 'params' are on the child's entry."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {"ref": "sap_table.thread", "params": {"table": "ADR6"}},
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        assert result[0].entry["params"] == {"table": "ADR6"}

    def test_ref_entry_without_as_no_stash(self, tmp_path):
        """A ref entry without 'as' or 'params' has neither on its entry."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [{"ref": "sap_table.thread"}],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        assert "as" not in result[0].entry
        assert "params" not in result[0].entry

    def test_ref_entry_params_resolve_in_thread(self, tmp_path):
        """ThreadEntry params are injected and resolve ${param.x} in thread."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.table}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "sap_table.thread",
                    "as": "sap_adr6",
                    "params": {"table": "ADR6"},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        resolved = result[0].data
        assert resolved["sources"]["data"]["alias"] == "ADR6"

    def test_two_phase_param_expression_resolution(self, tmp_path):
        """ThreadEntry param values with ${param.x} resolve against parent."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.target_schema}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "sap_table.thread",
                    "as": "sap_adr6",
                    "params": {"target_schema": "${param.env}_silver"},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path, runtime_params={"env": "prod"})
        resolved = result[0].data
        assert resolved["sources"]["data"]["alias"] == "prod_silver"

    def test_literal_params_pass_through(self, tmp_path):
        """ThreadEntry params with literal values pass through unchanged."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.table}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "sap_table.thread",
                    "as": "sap_adr6",
                    "params": {"table": "ADR6"},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        resolved = result[0].data
        assert resolved["sources"]["data"]["alias"] == "ADR6"

    def test_sibling_refs_same_file_no_cycle_error(self, tmp_path):
        """Two ThreadEntry objects with same ref and different aliases resolve."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n"
            "    alias: ${param.table}\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "sap_table.thread",
                    "as": "sap_adr6",
                    "params": {"table": "ADR6"},
                },
                {
                    "ref": "sap_table.thread",
                    "as": "sap_adr2",
                    "params": {"table": "ADR2"},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        resolved = [child.data for child in result]
        assert len(resolved) == 2
        assert resolved[0]["sources"]["data"]["alias"] == "ADR6"
        assert resolved[1]["sources"]["data"]["alias"] == "ADR2"

    def test_duplicate_ref_without_alias_raises(self, tmp_path):
        """Same ref twice without 'as' raises ConfigError."""
        from resolving import resolve_tree
        from weevr.errors import ConfigError

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {"ref": "sap_table.thread"},
                {"ref": "sap_table.thread"},
            ],
        }
        with pytest.raises(ConfigError, match="appears multiple times"):
            resolve_tree(weave_config, "weave", tmp_path)

    def test_duplicate_ref_with_aliases_succeeds(self, tmp_path):
        """Same ref twice with different aliases resolves successfully."""
        from resolving import resolve_tree

        thread_file = tmp_path / "sap_table.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {"ref": "sap_table.thread", "as": "instance_a"},
                {"ref": "sap_table.thread", "as": "instance_b"},
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        assert len(result) == 2

    def test_duplicate_effective_name_raises(self, tmp_path):
        """Two entries with same effective name raise ConfigError."""
        from resolving import resolve_tree
        from weevr.errors import ConfigError

        thread_a = tmp_path / "a.thread"
        write_document(
            thread_a,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )
        thread_b = tmp_path / "b.thread"
        write_document(
            thread_b,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {"ref": "a.thread", "as": "same_name"},
                {"ref": "b.thread", "as": "same_name"},
            ],
        }
        with pytest.raises(ConfigError, match="same_name"):
            resolve_tree(weave_config, "weave", tmp_path)

    def test_inline_entry_with_as_stashed(self):
        """An inline entry's 'as' is on the child's entry, which has no own file."""
        from resolving import resolve_tree

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "name": "inline_thread",
                    "as": "my_alias",
                    "config_version": "2.0",
                    "sources": {"d": {"type": "delta", "alias": "r"}},
                    "target": {},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", Path("/tmp"))
        assert result[0].entry["as"] == "my_alias"
        assert result[0].source_path is None


class TestWholeValueResolution:
    """Test that whole-value ${param.x} references return native Python types."""

    def test_whole_value_list(self):
        """${param.param} resolving to list returns list, not string."""
        config = {"match_keys": "${param.pk_columns}"}
        context = {"param": {"pk_columns": ["mandt", "color"]}}
        result = resolve_variables(config, context)
        assert result["match_keys"] == ["mandt", "color"]
        assert isinstance(result["match_keys"], list)

    def test_whole_value_int(self):
        """${param.param} resolving to int returns int."""
        config = {"page_size": "${param.batch_size}"}
        context = {"param": {"batch_size": 1000}}
        result = resolve_variables(config, context)
        assert result["page_size"] == 1000
        assert isinstance(result["page_size"], int)

    def test_whole_value_bool_true(self):
        """${param.param} resolving to bool True returns bool."""
        config = {"enabled": "${param.flag_enabled}"}
        context = {"param": {"flag_enabled": True}}
        result = resolve_variables(config, context)
        assert result["enabled"] is True
        assert isinstance(result["enabled"], bool)

    def test_whole_value_bool_false(self):
        """${param.param} resolving to bool False returns bool."""
        config = {"debug": "${param.flag_debug}"}
        context = {"param": {"flag_debug": False}}
        result = resolve_variables(config, context)
        assert result["debug"] is False
        assert isinstance(result["debug"], bool)

    def test_whole_value_none(self):
        """${param.param} resolving to None returns None, not string 'None'."""
        config = {"filter": "${param.optional_filter}"}
        context = {"param": {"optional_filter": None}}
        result = resolve_variables(config, context)
        assert result["filter"] is None

    def test_whole_value_with_default_returns_string(self):
        """${param.param:-default} where param missing returns default as string."""
        config = {"mode": "${param.missing_param:-batch}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["mode"] == "batch"
        assert isinstance(result["mode"], str)

    def test_embedded_ref_still_returns_string(self):
        """Embedded ${param.param} in a longer string always returns string."""
        config = {"alias": "SAP.${param.table_name}"}
        context = {"param": {"table_name": "MARA"}}
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
        """${param.param} where param missing and no default raises VariableResolutionError."""
        config = {"keys": "${param.undefined_param}"}
        context = {}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        assert "Unresolved variable" in str(exc_info.value)
        assert "undefined_param" in str(exc_info.value)


_MINIMAL_THREAD = {
    "config_version": "2.0",
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
        """A required thread param with no value fails P101 when its scope binds."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {"env": {"name": "env", "type": "string", "required": True}},
        }
        thread = Thread.model_validate(data)

        with pytest.raises(ConfigSchemaError) as exc_info:
            ParamScope.root().child(thread.params)
        assert [d.code for d in exc_info.value.diagnostics] == ["P101"]

    def test_param_validation_type_mismatch_raises(self):
        """A thread param value its type rejects fails P102 when its scope binds."""
        from weevr.model.thread import Thread

        data = {
            **_MINIMAL_THREAD,
            "params": {"count": {"name": "count", "type": "int", "required": True}},
        }
        thread = Thread.model_validate(data)

        with pytest.raises(ConfigSchemaError) as exc_info:
            ParamScope.root({"count": "not_an_int"}).child(thread.params)
        assert [d.code for d in exc_info.value.diagnostics] == ["P102"]

    def test_param_validation_with_defaults_passes(self):
        """A required thread param with a default binds the default."""
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

        assert ParamScope.root().child(thread.params).values() == {"mode": "batch"}

    def test_variable_resolution_within_standalone_thread(self):
        """${param.param} references inside a standalone thread config resolve correctly."""
        config = {
            "sources": {
                "data": {
                    "type": "delta",
                    "alias": "${param.lakehouse}.customers",
                }
            },
            "target": {"alias": "${param.lakehouse}.output"},
        }
        context = {"param": {"lakehouse": "silver"}}
        resolved = resolve_variables(config, context)

        assert resolved["sources"]["data"]["alias"] == "silver.customers"
        assert resolved["target"]["alias"] == "silver.output"

    def test_whole_value_whitespace_stripped(self):
        """Whole-value match works even when value has surrounding whitespace."""
        config = {"keys": "  ${param.pk_columns}  "}
        context = {"param": {"pk_columns": ["id", "code"]}}
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
        config = {"path": "/lakehouse/${param.env}/${run.timestamp}"}
        result = resolve_variables(config, {"param": {"env": "prod"}})
        assert result["path"] == "/lakehouse/prod/${run.timestamp}"


class TestHierarchyVariableDeferral:
    """Execution-time hierarchy variables survive config-load resolution."""

    @pytest.mark.parametrize(
        "name",
        [
            "thread.name",
            "thread.qualified_key",
            "thread.source",
            "thread.sources",
            "weave.name",
            "loom.name",
        ],
    )
    def test_embedded_reference_preserved(self, name: str) -> None:
        config = {"path": f"/archive/${{{name}}}/data"}
        assert resolve_variables(config, {}) == config

    @pytest.mark.parametrize(
        "name",
        [
            "thread.name",
            "thread.qualified_key",
            "thread.source",
            "thread.sources",
            "weave.name",
            "loom.name",
        ],
    )
    def test_whole_value_reference_preserved(self, name: str) -> None:
        config = {"label": f"${{{name}}}"}
        assert resolve_variables(config, {}) == config

    def test_mixed_with_params_and_run_vars(self) -> None:
        config = {"path": "/${param.env}/${loom.name}/${thread.name}/${run.timestamp}"}
        result = resolve_variables(config, {"param": {"env": "prod"}})
        assert result["path"] == "/prod/${loom.name}/${thread.name}/${run.timestamp}"

    def test_unknown_property_still_raises(self) -> None:
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables({"path": "/archive/${thread.nme}"}, {})
        assert [d.code for d in exc_info.value.diagnostics] == ["P112"]

    def test_default_still_resolves_at_load(self) -> None:
        config = {"path": "/archive/${thread.name:-fallback}", "label": "${weave.name:-w}"}
        result = resolve_variables(config, {})
        assert result == {"path": "/archive/fallback", "label": "w"}

    def test_load_context_never_supplies_it(self) -> None:
        """A load context never supplies a hierarchy name: it stays for execution."""
        config = {"path": "/archive/${thread.name}", "label": "${thread.name}"}
        result = resolve_variables(config, {"thread": {"name": "from_params"}})
        assert result == config

    def test_every_deferred_name_resolves_at_execution(self) -> None:
        """Nothing deferred at load is left unresolved by the execution-time resolver."""
        ctx = AuditContext(
            thread_name="t",
            thread_qualified_key="w.t",
            thread_source="src",
            thread_sources_json="[]",
            weave_name="w",
            loom_name="l",
        )
        assert HIERARCHY_CONTEXT_KEYS
        assert set(CONTEXT_VARIABLE_FIELDS.values()) <= {
            f.name for f in dataclasses.fields(AuditContext)
        }
        for name in HIERARCHY_CONTEXT_KEYS:
            assert "${" not in resolve_context_variables(f"${{{name}}}", ctx), name


class TestThreadParameterizationE2E:
    """End-to-end integration tests for thread parameterization (M121 Task 17)."""

    def _write_template(self, tmp_path, name="template.thread"):
        """Write a parameterized template thread file."""
        f = tmp_path / name
        write_document(
            f,
            'config_version: "2.0"\n'
            "sources:\n"
            "  data:\n"
            "    type: delta\n"
            "    alias: ${param.schema}.${param.table_name}\n"
            "target:\n"
            "  alias: silver.${param.table_name}\n",
        )
        return f

    def test_two_instances_resolve_independently(self, tmp_path):
        """Two instances of same ref with different params resolve independently."""
        from resolving import resolve_tree

        self._write_template(tmp_path)

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "template.thread",
                    "as": "sap_adr6",
                    "params": {"schema": "raw", "table_name": "ADR6"},
                },
                {
                    "ref": "template.thread",
                    "as": "sap_adr2",
                    "params": {"schema": "raw", "table_name": "ADR2"},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        resolved = [child.data for child in result]

        assert len(resolved) == 2
        assert resolved[0]["sources"]["data"]["alias"] == "raw.ADR6"
        assert resolved[0]["target"]["alias"] == "silver.ADR6"
        assert result[0].entry["as"] == "sap_adr6"
        assert resolved[1]["sources"]["data"]["alias"] == "raw.ADR2"
        assert resolved[1]["target"]["alias"] == "silver.ADR2"
        assert result[1].entry["as"] == "sap_adr2"

    def test_three_instances_same_ref(self, tmp_path):
        """Three instances of same ref all resolve independently."""
        from resolving import resolve_tree

        self._write_template(tmp_path)

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "template.thread",
                    "as": "inst_a",
                    "params": {"schema": "raw", "table_name": "A"},
                },
                {
                    "ref": "template.thread",
                    "as": "inst_b",
                    "params": {"schema": "raw", "table_name": "B"},
                },
                {
                    "ref": "template.thread",
                    "as": "inst_c",
                    "params": {"schema": "stg", "table_name": "C"},
                },
            ],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        resolved = [child.data for child in result]
        assert len(resolved) == 3
        assert resolved[2]["sources"]["data"]["alias"] == "stg.C"

    def test_nested_param_expression(self, tmp_path):
        """ThreadEntry param values with ${param.env} resolve against parent."""
        from resolving import resolve_tree

        f = tmp_path / "schema_thread.thread"
        write_document(
            f,
            'config_version: "2.0"\n'
            "sources:\n"
            "  data:\n"
            "    type: delta\n"
            "    alias: ${param.target_schema}.customers\n"
            "target:\n"
            "  alias: ${param.target_schema}.dim_customers\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [
                {
                    "ref": "schema_thread.thread",
                    "as": "dim_cust",
                    "params": {"target_schema": "${param.env}_silver"},
                },
            ],
        }
        result = resolve_tree(
            weave_config,
            "weave",
            tmp_path,
            runtime_params={"env": "prod"},
        )
        resolved = result[0].data
        assert resolved["sources"]["data"]["alias"] == "prod_silver.customers"
        assert resolved["target"]["alias"] == "prod_silver.dim_customers"

    def test_loom_with_aliased_threads_in_weave(self, tmp_path):
        """Loom resolves weave with aliased thread instances."""
        from resolving import resolve_tree

        self._write_template(tmp_path)

        weave_file = tmp_path / "silver.weave"
        write_document(
            weave_file,
            'config_version: "2.0"\n'
            "threads:\n"
            "  - ref: template.thread\n"
            "    as: sap_adr6\n"
            "    params:\n"
            "      schema: raw\n"
            "      table_name: ADR6\n"
            "  - ref: template.thread\n"
            "    as: sap_adr2\n"
            "    params:\n"
            "      schema: raw\n"
            "      table_name: ADR2\n",
        )

        loom_config = {
            "config_version": "2.0",
            "weaves": [{"ref": "silver.weave"}],
        }
        result = resolve_tree(loom_config, "loom", tmp_path)
        threads = result[0].children
        assert len(threads) == 2
        assert threads[0].entry["as"] == "sap_adr6"
        assert threads[1].entry["as"] == "sap_adr2"

    def test_standalone_template_raises(self):
        """Standalone thread with unresolved ${param.x} raises."""
        config = {
            "config_version": "2.0",
            "sources": {"d": {"type": "delta", "alias": "${param.table}"}},
            "target": {},
        }
        context = {}
        with pytest.raises(VariableResolutionError, match="param.table"):
            resolve_variables(config, context)

    def test_backwards_compat_no_as_no_params(self, tmp_path):
        """Existing weave configs without as/params still work."""
        from resolving import resolve_tree

        thread_file = tmp_path / "simple.thread"
        write_document(
            thread_file,
            'config_version: "2.0"\n'
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            "target: {}\n",
        )

        weave_config = {
            "config_version": "2.0",
            "threads": [{"ref": "simple.thread"}],
        }
        result = resolve_tree(weave_config, "weave", tmp_path)
        assert len(result) == 1
        assert "as" not in result[0].entry
        assert "params" not in result[0].entry
